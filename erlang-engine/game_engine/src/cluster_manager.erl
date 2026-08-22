%%%-------------------------------------------------------------------
%% @doc Cluster Manager — scopre e monitora i nodi del cluster Erlang.
%%
%%      Responsabilita':
%%        - al boot, contattare i nodi peer dichiarati in configurazione
%%          con net_adm:ping/1 e formare il cluster;
%%        - sottoscriversi con net_kernel:monitor_nodes/2 per ricevere
%%          notifiche {nodeup, Node} e {nodedown, Node};
%%        - tenere aggiornata la lista dei nodi connessi;
%%        - notificare il leader_election (Fase 3) quando la topologia
%%          cambia, in modo che venga lanciata una elezione.
%%
%%      Il modulo leader_election potrebbe non esistere ancora (viene
%%      creato nella Fase 3): ogni chiamata verso di lui e' protetta
%%      da un try/catch o da un controllo di esistenza, cosicche' la
%%      Fase 2 possa funzionare e essere testata autonomamente.
%% @end
%%%-------------------------------------------------------------------
-module(cluster_manager).
-behaviour(gen_server).

-export([start_link/0, get_nodes/0, get_connected_nodes/0]).
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
    known_nodes     = [] :: [node()],   %% Nodi peer configurati
    connected_nodes = [] :: [node()],   %% Nodi attualmente connessi
    self_node       :: node()
}).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% Restituisce tutti i nodi peer configurati (connessi o meno).
-spec get_nodes() -> [node()].
get_nodes() ->
    gen_server:call(?SERVER, get_nodes).

%% Restituisce solo i nodi attualmente connessi.
-spec get_connected_nodes() -> [node()].
get_connected_nodes() ->
    gen_server:call(?SERVER, get_connected_nodes).

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
    maybe_start_election(),
    {noreply, State};

%%--------------------------------------------------------------------
%% Un nodo si e' connesso al cluster
%%--------------------------------------------------------------------
handle_info({nodeup, Node, _InfoList}, State) ->
    io:format("[CLUSTER] Nodo connesso: ~p~n", [Node]),
    NewConnected = lists:usort([Node | State#state.connected_nodes]),
    %% Un nuovo nodo nel cluster: serve un'elezione cosi' tutti sanno chi
    %% e' il leader (incluso il nuovo arrivato).
    maybe_start_election(),
    {noreply, State#state{connected_nodes = NewConnected}};

%%--------------------------------------------------------------------
%% Un nodo ha lasciato il cluster
%%--------------------------------------------------------------------
handle_info({nodedown, Node, _InfoList}, State) ->
    io:format("[CLUSTER] Nodo disconnesso: ~p~n", [Node]),
    NewConnected = lists:delete(Node, State#state.connected_nodes),
    %% Se il nodo caduto era il leader dobbiamo eleggerne uno nuovo.
    %% La logica "era il leader?" e' dentro leader_election, se esiste.
    maybe_start_election_on_nodedown(Node),
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

%% Chiama leader_election:start_election() se il modulo esiste.
%% Se siamo ancora in Fase 2 (leader_election non compilato),
%% il tentativo fallisce silenziosamente.
maybe_start_election() ->
    try
        leader_election:start_election()
    catch
        error:undef ->
            io:format("[CLUSTER] leader_election non ancora disponibile, elezione rimandata~n");
        _Class:_Reason ->
            ok
    end.

%% In caso di nodedown, controlla se il nodo caduto era il leader.
%% Se si', lancia un'elezione d'emergenza.
maybe_start_election_on_nodedown(DownNode) ->
    try
        case leader_election:get_leader() of
            {ok, DownNode} ->
                io:format("[CLUSTER] *** LEADER ~p CADUTO! Elezione d'emergenza ***~n", [DownNode]),
                leader_election:start_election();
            _ ->
                %% Il nodo caduto non era il leader: lanciamo comunque
                %% un'elezione per riallineare tutti.
                leader_election:start_election()
        end
    catch
        error:undef ->
            io:format("[CLUSTER] leader_election non ancora disponibile~n");
        _Class:_Reason ->
            ok
    end.
