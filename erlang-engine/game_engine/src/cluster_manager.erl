%%%-------------------------------------------------------------------
%% @doc Cluster Manager — scopre e monitora i nodi del cluster Erlang.
%%
%%      Responsabilita':
%%        - al boot, contattare i nodi peer dichiarati in configurazione
%%          con net_adm:ping/1 e formare il cluster;
%%        - sottoscriversi con net_kernel:monitor_nodes/2 per ricevere
%%          notifiche {nodeup, Node} e {nodedown, Node};
%%        - tenere aggiornata la lista dei nodi connessi;
%%        - esporre le due liste su cui si appoggiano elezione e snapshot:
%%            * configured_nodes/0 : lista STATICA dei nodi configurati,
%%              denominatore del quorum;
%%            * get_participants/0 : lista ORDINATA dei nodi vivi, che lo
%%              snapshot congela all'avvio del taglio;
%%        - notificare leader_election quando la topologia cambia.
%%
%%      Entrambe le liste sono intersecate con i nodi configurati: il
%%      monitoraggio e' attivo con {node_type, all}, quindi una shell
%%      diagnostica attaccata al cluster comparirebbe altrimenti nel
%%      conteggio del quorum e fra i partecipanti allo snapshot.
%% @end
%%%-------------------------------------------------------------------
-module(cluster_manager).
-behaviour(gen_server).

-export([start_link/0, get_nodes/0, get_connected_nodes/0,
         configured_nodes/0, get_participants/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

%% Tempo di attesa prima della prima elezione: lascia il tempo ai nodi
%% peer di rispondere ai ping e di partire a loro volta.
-define(INITIAL_ELECTION_DELAY, 2000).

%% Intervallo tra i tentativi periodici di connessione ai peer non ancora
%% raggiungibili (ms).  Piu' lungo del retry di rabbitmq_manager perche'
%% il cluster puo' metterci tempo a formarsi.
-define(RECONNECT_INTERVAL, 10000).

-record(state, {
    known_nodes     = [] :: [node()],   %% Nodi peer configurati (senza il nostro)
    connected_nodes = [] :: [node()],   %% Nodi attualmente connessi
    self_node       :: node()
}).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% Restituisce tutti i nodi peer configurati (connessi o meno), escluso il nostro.
-spec get_nodes() -> [node()].
get_nodes() ->
    gen_server:call(?SERVER, get_nodes).

%% Restituisce solo i nodi peer attualmente connessi.
-spec get_connected_nodes() -> [node()].
get_connected_nodes() ->
    gen_server:call(?SERVER, get_connected_nodes).

%% Lista STATICA di tutti i nodi del cluster, incluso il nostro.
%% E' il denominatore del quorum in leader_election: deve essere statica,
%% perche' una lista che si restringe da sola durante una partizione
%% renderebbe la guardia di maggioranza inutile.
-spec configured_nodes() -> [node()].
configured_nodes() ->
    gen_server:call(?SERVER, configured_nodes).

%% Lista ORDINATA e stabile dei nodi vivi del cluster, incluso il nostro.
%% E' la lista che lo snapshot congela all'avvio del taglio.
-spec get_participants() -> [node()].
get_participants() ->
    gen_server:call(?SERVER, get_participants).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Leggi la lista di peer dalla configurazione dell'applicazione.
    %% app.src contiene tutti e 3 i nodi: filtriamo il nostro nome,
    %% cosi' ogni nodo usa la stessa config senza override da CLI.
    AllPeers = application:get_env(game_engine, peer_nodes, []),
    Self = node(),
    PeerNodes = AllPeers -- [Self],

    io:format("~n=================================~n"),
    io:format("  Cluster Manager avviato~n"),
    io:format("  Nodo locale : ~p~n", [Self]),
    io:format("  Peer config.: ~p~n", [PeerNodes]),
    io:format("=================================~n~n"),

    %% Sottoscrizione alle notifiche del kernel di distribuzione.
    %% Il flag {node_type, all} include anche i nodi nascosti (-hidden).
    ok = net_kernel:monitor_nodes(true, [{node_type, all}]),

    %% Tenta il ping dei peer in modo asincrono (non blocca init/1).
    self() ! ping_peers,

    %% Schedula la prima elezione dopo un breve ritardo: i peer hanno
    %% bisogno di qualche istante per rispondere ai ping.
    erlang:send_after(?INITIAL_ELECTION_DELAY, self(), initial_election),

    %% Reconnect periodico: ritenta i peer non ancora connessi.
    erlang:send_after(?RECONNECT_INTERVAL, self(), reconnect_tick),

    {ok, #state{known_nodes = PeerNodes, self_node = Self}}.

handle_call(get_nodes, _From, State) ->
    {reply, State#state.known_nodes, State};

handle_call(get_connected_nodes, _From, State) ->
    {reply, State#state.connected_nodes, State};

handle_call(configured_nodes, _From, State) ->
    {reply, all_configured(State), State};

handle_call(get_participants, _From, State) ->
    %% Nodi vivi secondo il kernel, intersecati con quelli configurati:
    %% una shell distribuita non deve diventare un partecipante fantasma.
    Cfg = all_configured(State),
    Live = lists:usort([node() | nodes()]),
    {reply, [N || N <- Live, lists:member(N, Cfg)], State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Ping dei peer al boot
%%--------------------------------------------------------------------
handle_info(ping_peers, State) ->
    Connected = ping_all(State#state.known_nodes),
    io:format("[CLUSTER] Ping iniziale completato, connessi: ~p~n", [Connected]),
    {noreply, State#state{connected_nodes = Connected}};

%%--------------------------------------------------------------------
%% Reconnect periodico dei peer non ancora connessi
%%--------------------------------------------------------------------
handle_info(reconnect_tick, State) ->
    #state{known_nodes = Known, connected_nodes = Current} = State,
    Missing = Known -- Current,
    case Missing of
        [] -> ok;
        _ ->
            NewlyConnected = ping_all(Missing),
            case NewlyConnected of
                [] -> ok;
                _  -> io:format("[CLUSTER] Reconnect: raggiunti ~p~n", [NewlyConnected])
            end
    end,
    erlang:send_after(?RECONNECT_INTERVAL, self(), reconnect_tick),
    %% La lista connessi vera e propria viene aggiornata dal nodeup/nodedown,
    %% qui ci limitiamo a ritentare il ping.
    {noreply, State};

%%--------------------------------------------------------------------
%% Prima elezione (ritardata)
%%--------------------------------------------------------------------
handle_info(initial_election, State) ->
    io:format("[CLUSTER] Trigger elezione iniziale~n"),
    leader_election:start_election(),
    {noreply, State};

%%--------------------------------------------------------------------
%% Un nodo si e' connesso al cluster
%%--------------------------------------------------------------------
handle_info({nodeup, Node, _InfoList}, State) ->
    io:format("[CLUSTER] Nodo connesso: ~p~n", [Node]),
    NewConnected = lists:usort([Node | State#state.connected_nodes]),
    %% Un nuovo nodo nel cluster: serve un'elezione cosi' tutti sanno chi
    %% e' il leader (incluso il nuovo arrivato).
    leader_election:start_election(),
    {noreply, State#state{connected_nodes = NewConnected}};

%%--------------------------------------------------------------------
%% Un nodo ha lasciato il cluster
%%--------------------------------------------------------------------
handle_info({nodedown, Node, _InfoList}, State) ->
    io:format("[CLUSTER] Nodo disconnesso: ~p~n", [Node]),
    NewConnected = lists:delete(Node, State#state.connected_nodes),
    %% La decisione "rieleggere o retrocedere" dipende dal ruolo corrente e
    %% dal quorum, quindi vive dentro leader_election: qui ci limitiamo a
    %% notificare l'evento.
    leader_election:node_down(Node),
    {noreply, State#state{connected_nodes = NewConnected}};

%% Varianti senza InfoList (net_kernel:monitor_nodes(true) senza opzioni)
handle_info({nodeup, Node}, State) ->
    handle_info({nodeup, Node, []}, State);
handle_info({nodedown, Node}, State) ->
    handle_info({nodedown, Node, []}, State);

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    net_kernel:monitor_nodes(false),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal
%%====================================================================

%% Lista statica completa: peer configurati + il nodo locale.
%% init/1 filtra se stesso da peer_nodes, quindi va ri-aggiunto qui.
all_configured(#state{known_nodes = Known, self_node = Self}) ->
    lists:usort([Self | Known]).

%% Pinga una lista di nodi, ritorna quelli che hanno risposto pong.
-spec ping_all([node()]) -> [node()].
ping_all(Nodes) ->
    lists:filter(fun(N) ->
        case net_adm:ping(N) of
            pong ->
                io:format("[CLUSTER]   pong da ~p~n", [N]),
                true;
            pang ->
                false
        end
    end, Nodes).
