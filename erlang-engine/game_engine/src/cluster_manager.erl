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
%%        - notificare leader_election quando la topologia cambia;
%%        - fare il bootstrap di Mnesia DOPO che il cluster si e' formato,
%%          creando o unendosi alla tabella replicata snapshot_record.
%%
%%      Entrambe le liste sono intersecate con i nodi configurati: il
%%      monitoraggio e' attivo con {node_type, all}, quindi una shell
%%      diagnostica attaccata al cluster comparirebbe altrimenti nel
%%      conteggio del quorum e fra i partecipanti allo snapshot.
%% @end
%%%-------------------------------------------------------------------
-module(cluster_manager).
-behaviour(gen_server).

-include("game_engine.hrl").

-export([start_link/0, get_nodes/0, get_connected_nodes/0,
         configured_nodes/0, get_participants/0, force_load_snapshots/0]).
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

%% Bootstrap di Mnesia: parte dopo il ping iniziale, mai da init/1.
%% Creare lo schema prima che il cluster si sia formato porterebbe ogni nodo
%% a crearsi un database indipendente.
-define(MNESIA_BOOTSTRAP_DELAY, 3000).
-define(MNESIA_RETRY_INTERVAL, 2000).
%% Quante volte un nodo aspetta che qualcun altro crei la tabella prima di
%% crearla lui. Serve a evitare che N nodi avviati insieme creino N database
%% distinti: crea per primo solo il nodo con il nome piu' basso fra quelli
%% connessi, gli altri attendono. Dopo l'ultimo tentativo si procede comunque,
%% cosi' un cluster in cui quel nodo non parte mai non resta bloccato.
-define(MNESIA_JOIN_ATTEMPTS, 5).
-define(MNESIA_WAIT, 5000).

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

%% Forza il caricamento della copia locale di snapshot_record quando Mnesia
%% sta aspettando nodi che non torneranno.
%%
%% Da usare CONSAPEVOLMENTE: la copia locale potrebbe non essere la piu'
%% recente, e quando gli altri nodi rientreranno adotteranno questa. Per un
%% audit trail significa poter perdere i checkpoint scritti mentre questo
%% nodo era fermo. Serve solo a ripartire da soli dopo un guasto definitivo.
-spec force_load_snapshots() -> yes | term().
force_load_snapshots() ->
    Res = mnesia:force_load_table(snapshot_record),
    io:format("[MNESIA] force_load_table(snapshot_record) -> ~p~n", [Res]),
    Res.

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

    %% Bootstrap di Mnesia, dopo che i ping hanno avuto il tempo di connettere.
    erlang:send_after(?MNESIA_BOOTSTRAP_DELAY, self(), {mnesia_bootstrap, 1}),

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
%% Bootstrap di Mnesia
%%--------------------------------------------------------------------
handle_info({mnesia_bootstrap, Attempt}, State) ->
    case bootstrap_mnesia(State, Attempt) of
        done ->
            subscribe_mnesia_events();
        retry ->
            erlang:send_after(?MNESIA_RETRY_INTERVAL, self(), {mnesia_bootstrap, Attempt + 1})
    end,
    {noreply, State};

%%--------------------------------------------------------------------
%% Eventi di sistema di Mnesia
%%--------------------------------------------------------------------
handle_info({mnesia_system_event, {inconsistent_database, Context, Node}}, State) ->
    %% Mnesia lo emette quando rileva una PARTIZIONE, non necessariamente una
    %% divergenza dei dati. Va comunque loggato in modo rumoroso: e' il segnale
    %% che va verificato a mano che esista un solo snapshot_record per round.
    io:format("~n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!~n"),
    io:format("!!! [MNESIA] DATABASE INCONSISTENTE: ~p con ~p~n", [Context, Node]),
    io:format("!!! Probabile partizione di rete. Verificare che ci sia UN SOLO~n"),
    io:format("!!! snapshot_record per round prima di riparare a mano.~n"),
    io:format("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!~n~n"),
    {noreply, State};

handle_info({mnesia_system_event, Event}, State) ->
    io:format("[MNESIA] Evento di sistema: ~p~n", [Event]),
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

%%--------------------------------------------------------------------
%% Mnesia
%%--------------------------------------------------------------------

%% Ritorna `done` quando il bootstrap e' concluso, `retry` quando conviene
%% aspettare che sia un altro nodo a creare la tabella.
bootstrap_mnesia(State, Attempt) ->
    Peers = [N || N <- State#state.known_nodes, lists:member(N, nodes())],
    case find_table_owner(Peers) of
        {ok, Master} ->
            join_mnesia_cluster(Master),
            done;
        none ->
            case should_create(State, Peers) orelse Attempt >= ?MNESIA_JOIN_ATTEMPTS of
                true ->
                    create_mnesia_local(),
                    done;
                false ->
                    io:format("[MNESIA] Nessuna tabella nel cluster, attendo che la crei "
                              "un nodo con nome piu' basso (tentativo ~p/~p)~n",
                              [Attempt, ?MNESIA_JOIN_ATTEMPTS]),
                    retry
            end
    end.

%% Crea per primo il nodo con il nome piu' basso fra quelli connessi: senza
%% questa regola N nodi avviati insieme creerebbero N schemi indipendenti,
%% che Mnesia non unisce da sola.
should_create(#state{self_node = Self}, Peers) ->
    Self =:= hd(lists:sort([Self | Peers])).

%% Un peer possiede gia' la tabella su disco?
find_table_owner([]) -> none;
find_table_owner([N | T]) ->
    case rpc:call(N, mnesia, table_info, [snapshot_record, disc_copies], 3000) of
        Copies when is_list(Copies), Copies =/= [] -> {ok, N};
        _ -> find_table_owner(T)
    end.

create_mnesia_local() ->
    ensure_mnesia_started(),
    case lists:member(node(), disc_copies_of_table()) of
        true ->
            %% Non e' una creazione: e' un riavvio con la copia gia' su disco.
            io:format("[MNESIA] Copia locale gia' presente su disco, nessuno schema da creare~n"),
            wait_for_snapshot_table();
        false ->
            create_schema_and_table()
    end.

create_schema_and_table() ->
    io:format("[MNESIA] Nessun peer possiede snapshot_record: creo lo schema locale~n"),
    %% create_schema/1 esige Mnesia FERMA sui nodi elencati.
    mnesia:stop(),
    case mnesia:create_schema([node()]) of
        ok ->
            io:format("[MNESIA] Schema su disco creato~n");
        {error, {_, {already_exists, _}}} ->
            io:format("[MNESIA] Schema su disco gia' presente~n");
        {error, Err} ->
            io:format("[MNESIA] create_schema fallita: ~p~n", [Err])
    end,
    mnesia:start(),
    case mnesia:create_table(snapshot_record,
                             [{attributes, record_info(fields, snapshot_record)},
                              {type, ordered_set},
                              {disc_copies, [node()]}]) of
        {atomic, ok} ->
            io:format("[MNESIA] Tabella snapshot_record creata~n");
        {aborted, {already_exists, _}} ->
            io:format("[MNESIA] Tabella snapshot_record gia' presente~n");
        {aborted, Reason} ->
            io:format("[MNESIA] create_table fallita: ~p~n", [Reason])
    end,
    wait_for_snapshot_table().

join_mnesia_cluster(Master) ->
    io:format("[MNESIA] Mi aggiungo al cluster Mnesia tramite ~p~n", [Master]),
    ensure_mnesia_started(),
    case mnesia:change_config(extra_db_nodes, [Master]) of
        {ok, []} ->
            io:format("[MNESIA] ATTENZIONE: change_config non ha connesso ~p~n", [Master]);
        {ok, _Connected} ->
            ok;
        {error, ConfErr} ->
            io:format("[MNESIA] change_config fallita: ~p~n", [ConfErr])
    end,
    %% Il passo che si dimentica piu' spesso: senza, lo schema resta in RAM e
    %% il nodo PERDE la propria copia a ogni riavvio, vanificando disc_copies.
    log_mnesia_result("conversione dello schema in disc_copies",
                      mnesia:change_table_copy_type(schema, node(), disc_copies)),
    log_mnesia_result("copia locale di snapshot_record",
                      mnesia:add_table_copy(snapshot_record, node(), disc_copies)),
    wait_for_snapshot_table().

ensure_mnesia_started() ->
    case mnesia:system_info(is_running) of
        yes -> ok;
        _   -> mnesia:start()
    end.

log_mnesia_result(What, {atomic, ok}) ->
    io:format("[MNESIA] ~s: ok~n", [What]);
log_mnesia_result(What, {aborted, Reason}) when element(1, Reason) =:= already_exists ->
    io:format("[MNESIA] ~s: gia' presente~n", [What]);
log_mnesia_result(What, Other) ->
    io:format("[MNESIA] ~s: ~p~n", [What, Other]).

wait_for_snapshot_table() ->
    case mnesia:wait_for_tables([snapshot_record], ?MNESIA_WAIT) of
        ok ->
            io:format("[MNESIA] Pronto. Copie su disco: ~p~n", [disc_copies_of_table()]);
        {timeout, _} ->
            %% Comportamento normale di Mnesia, non un errore: la copia locale
            %% potrebbe non essere l'ultima scritta, quindi il nodo attende i
            %% peer che possiedono le altre repliche invece di caricare dati
            %% potenzialmente vecchi.
            Missing = disc_copies_of_table() -- [node()],
            io:format("[MNESIA] La copia locale non risulta autoritativa: attendo i nodi ~p.~n"
                      "[MNESIA] Il gioco funziona lo stesso, ma i checkpoint non sono~n"
                      "[MNESIA] leggibili finche' quei nodi non tornano. Se non torneranno,~n"
                      "[MNESIA] forzare con cluster_manager:force_load_snapshots().~n",
                      [Missing]);
        Other ->
            io:format("[MNESIA] Tabella non disponibile: ~p~n", [Other])
    end.

%% Ritorna SEMPRE una lista: quando la tabella non esiste ancora,
%% mnesia:table_info/2 esce con {aborted,{no_exists,_}} e i chiamanti la
%% usano con lists:member/2.
disc_copies_of_table() ->
    try mnesia:table_info(snapshot_record, disc_copies) of
        Copies when is_list(Copies) -> Copies;
        _ -> []
    catch
        _:_ -> []
    end.

subscribe_mnesia_events() ->
    case mnesia:subscribe(system) of
        {ok, _} -> ok;
        Other   -> io:format("[MNESIA] Sottoscrizione agli eventi fallita: ~p~n", [Other])
    end.

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
