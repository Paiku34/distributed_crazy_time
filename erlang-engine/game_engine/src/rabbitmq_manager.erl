%%%-------------------------------------------------------------------
%% @doc RabbitMQ Manager — possiede la connessione AMQP 0-9-1 e i suoi
%%      canali. Sostituisce le chiamate HTTP alla Management API che
%%      prima erano sparse in worker.erl e wheel_process.erl.
%%
%%      Responsabilita':
%%        - aprire e mantenere una connessione persistente a RabbitMQ,
%%          riconnettendosi da sola se il broker cade o non e' ancora su;
%%        - dichiarare le code (durable, come le dichiara il gateway Java);
%%        - esporre publish/2 senza passare dal gen_server, cosi' il
%%          wheel_process (che pubblica ogni secondo) non si blocca mai;
%%        - registrare i consumer: la sottoscrizione viene fatta con il
%%          PID del chiamante, quindi le delivery arrivano direttamente
%%          nella mailbox del worker, che fara' l'ack manuale.
%% @end
%%%-------------------------------------------------------------------
-module(rabbitmq_manager).
-behaviour(gen_server).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([start_link/0, publish/2, subscribe/2, ack/1, reject/2, is_connected/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% Tabella ETS pubblica: contiene i PID dei canali, letti direttamente dai
%% chiamanti di publish/2 e ack/1 (nessun hop attraverso il gen_server).
-define(TAB, rabbitmq_manager_tab).

%% Le stesse code dichiarate da GatewayApplication.java, tutte durable.
%% refunds_queue e' stata rimossa: i rimborsi passano ora da un evento
%% bet_rejected su results_queue, indirizzato per bet_id.
-define(QUEUES, [<<"bets_queue">>, <<"state_queue">>, <<"results_queue">>]).

-define(DEFAULT_CFG, #{
    host => "localhost",
    port => 5672,
    username => <<"guest">>,
    password => <<"guest">>,
    vhost => <<"/">>,
    prefetch => 10,
    retry_interval => 5000
}).

%% Una sottoscrizione attiva: {Coda, PidConsumer, MonitorRef, ConsumerTag}
-record(sub, {queue, pid, mref, tag = undefined}).

-record(state, {
    conn = undefined,
    conn_ref = undefined,
    pub_ch = undefined,
    pub_ref = undefined,
    cons_ch = undefined,
    cons_ref = undefined,
    subs = [] :: [#sub{}],
    cfg = ?DEFAULT_CFG
}).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% publish(Queue, Payload) -> ok | {error, Reason}
%% Pubblica sull'exchange di default con routing key = nome della coda,
%% esattamente come faceva la vecchia POST su amq.default.
publish(Queue, Payload) when is_binary(Queue), is_binary(Payload) ->
    case lookup_channel(pub_ch) of
        {ok, Ch} ->
            amqp_channel:cast(Ch,
                              #'basic.publish'{exchange = <<>>, routing_key = Queue},
                              #amqp_msg{payload = Payload});
        Error ->
            Error
    end.

%% subscribe(Queue, ConsumerPid) -> ok | {error, Reason}
%% Registra ConsumerPid come consumer della coda. Se la connessione non e'
%% ancora pronta la sottoscrizione viene ricordata e attivata appena il
%% broker torna raggiungibile.
subscribe(Queue, Pid) when is_binary(Queue), is_pid(Pid) ->
    gen_server:call(?MODULE, {subscribe, Queue, Pid}).

%% ack(DeliveryTag) -> ok | {error, Reason}
ack(Tag) ->
    case lookup_channel(cons_ch) of
        {ok, Ch} -> amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = Tag});
        Error -> Error
    end.

%% reject(DeliveryTag, Requeue) -> ok | {error, Reason}
reject(Tag, Requeue) when is_boolean(Requeue) ->
    case lookup_channel(cons_ch) of
        {ok, Ch} -> amqp_channel:cast(Ch, #'basic.reject'{delivery_tag = Tag, requeue = Requeue});
        Error -> Error
    end.

is_connected() ->
    case lookup_channel(pub_ch) of
        {ok, _} -> true;
        _ -> false
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Trappiamo le uscite: i processi di amqp_client possono essere linkati
    %% e non vogliamo che una caduta del broker abbatta questo gen_server.
    process_flag(trap_exit, true),
    ?TAB = ets:new(?TAB, [named_table, public, set, {read_concurrency, true}]),
    Cfg = maps:merge(?DEFAULT_CFG, application:get_env(game_engine, rabbitmq, #{})),
    io:format("~n=================================~n"),
    io:format("  RabbitMQ Manager (AMQP) avviato~n"),
    io:format("  Broker: ~s:~p~n", [maps:get(host, Cfg), maps:get(port, Cfg)]),
    io:format("=================================~n~n"),
    %% Non ci connettiamo dentro init/1: se il broker non e' ancora su,
    %% fallire qui farebbe scattare il limite di restart del supervisor e
    %% spegnerebbe l'intera applicazione.
    self() ! connect,
    {ok, #state{cfg = Cfg}}.

handle_call({subscribe, Queue, Pid}, _From, State) ->
    %% Se esisteva gia' una sottoscrizione per questa coda la sostituiamo
    %% (caso tipico: il worker e' crashato ed e' stato riavviato).
    State1 = drop_sub(fun(#sub{queue = Q}) -> Q =:= Queue end, State),
    MRef = erlang:monitor(process, Pid),
    Sub = #sub{queue = Queue, pid = Pid, mref = MRef},
    Sub1 = activate_sub(Sub, State1),
    {reply, ok, State1#state{subs = [Sub1 | State1#state.subs]}};

handle_call(is_connected, _From, State) ->
    {reply, State#state.pub_ch =/= undefined, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(connect, State = #state{conn = undefined}) ->
    {noreply, try_connect(State)};
handle_info(connect, State) ->
    {noreply, State};

%% Caduta della connessione o di uno dei due canali -> teardown e retry.
handle_info({'DOWN', MRef, process, _Pid, Reason}, State)
  when MRef =:= State#state.conn_ref;
       MRef =:= State#state.pub_ref;
       MRef =:= State#state.cons_ref ->
    io:format("[RABBIT] Connessione AMQP persa: ~p~n", [Reason]),
    {noreply, schedule_reconnect(teardown(State))};

%% Caduta di un consumer registrato (es. il worker).
handle_info({'DOWN', MRef, process, Pid, _Reason}, State) ->
    case lists:keyfind(MRef, #sub.mref, State#state.subs) of
        false ->
            {noreply, State};
        #sub{queue = Q} ->
            io:format("[RABBIT] Consumer ~p di ~s terminato, sottoscrizione rimossa~n", [Pid, Q]),
            {noreply, drop_sub(fun(#sub{mref = M}) -> M =:= MRef end, State)}
    end;

%% amqp_client puo' linkare i propri processi al chiamante.
handle_info({'EXIT', Pid, Reason}, State)
  when Pid =:= State#state.conn;
       Pid =:= State#state.pub_ch;
       Pid =:= State#state.cons_ch ->
    io:format("[RABBIT] Processo AMQP ~p terminato: ~p~n", [Pid, Reason]),
    {noreply, schedule_reconnect(teardown(State))};

handle_info({'EXIT', _Pid, _Reason}, State) ->
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    catch teardown(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal — connessione
%%====================================================================

try_connect(State = #state{cfg = Cfg}) ->
    Params = #amqp_params_network{
        host = maps:get(host, Cfg),
        port = maps:get(port, Cfg),
        username = maps:get(username, Cfg),
        password = maps:get(password, Cfg),
        virtual_host = maps:get(vhost, Cfg)
    },
    case amqp_connection:start(Params) of
        {ok, Conn} ->
            case setup_channels(Conn, State) of
                {ok, NewState} ->
                    io:format("[RABBIT] Connesso a ~s:~p - code dichiarate~n",
                              [maps:get(host, Cfg), maps:get(port, Cfg)]),
                    resubscribe_all(NewState);
                {error, Reason} ->
                    io:format("[RABBIT] Setup canali fallito: ~p~n", [Reason]),
                    catch amqp_connection:close(Conn),
                    schedule_reconnect(State)
            end;
        {error, Reason} ->
            io:format("[RABBIT] Broker non raggiungibile (~p), nuovo tentativo tra ~ps~n",
                      [Reason, maps:get(retry_interval, Cfg) div 1000]),
            schedule_reconnect(State)
    end.

setup_channels(Conn, State = #state{cfg = Cfg}) ->
    try
        {ok, PubCh} = amqp_connection:open_channel(Conn),
        {ok, ConsCh} = amqp_connection:open_channel(Conn),
        %% Le code sono gia' dichiarate durable dal gateway Java: dichiararle
        %% con parametri diversi darebbe PRECONDITION_FAILED.
        lists:foreach(fun(Q) ->
            #'queue.declare_ok'{} =
                amqp_channel:call(PubCh, #'queue.declare'{queue = Q, durable = true})
        end, ?QUEUES),
        #'basic.qos_ok'{} =
            amqp_channel:call(ConsCh, #'basic.qos'{prefetch_count = maps:get(prefetch, Cfg)}),
        ets:insert(?TAB, [{pub_ch, PubCh}, {cons_ch, ConsCh}]),
        {ok, State#state{conn = Conn,
                         conn_ref = erlang:monitor(process, Conn),
                         pub_ch = PubCh,
                         pub_ref = erlang:monitor(process, PubCh),
                         cons_ch = ConsCh,
                         cons_ref = erlang:monitor(process, ConsCh)}}
    catch
        Class:Err -> {error, {Class, Err}}
    end.

teardown(State) ->
    ets:delete(?TAB, pub_ch),
    ets:delete(?TAB, cons_ch),
    demonitor_ref(State#state.conn_ref),
    demonitor_ref(State#state.pub_ref),
    demonitor_ref(State#state.cons_ref),
    catch amqp_channel:close(State#state.pub_ch),
    catch amqp_channel:close(State#state.cons_ch),
    catch amqp_connection:close(State#state.conn),
    %% Le sottoscrizioni restano in stato, ma senza consumer tag: verranno
    %% riattivate alla prossima connessione riuscita.
    Subs = [S#sub{tag = undefined} || S <- State#state.subs],
    State#state{conn = undefined, conn_ref = undefined,
                pub_ch = undefined, pub_ref = undefined,
                cons_ch = undefined, cons_ref = undefined,
                subs = Subs}.

schedule_reconnect(State = #state{cfg = Cfg}) ->
    erlang:send_after(maps:get(retry_interval, Cfg), self(), connect),
    State.

demonitor_ref(undefined) -> ok;
demonitor_ref(Ref) -> erlang:demonitor(Ref, [flush]), ok.

%%====================================================================
%% Internal — sottoscrizioni
%%====================================================================

resubscribe_all(State) ->
    Subs = [activate_sub(S, State) || S <- State#state.subs],
    State#state{subs = Subs}.

%% Registra il consumer sul canale. Il terzo argomento di amqp_channel:subscribe
%% e' il PID che ricevera' le delivery: passiamo quello del worker, cosi' i
%% messaggi non fanno un salto in piu' attraverso questo processo.
activate_sub(Sub = #sub{tag = Tag}, _State) when Tag =/= undefined ->
    Sub;
activate_sub(Sub, #state{cons_ch = undefined}) ->
    Sub;
activate_sub(Sub = #sub{queue = Queue, pid = Pid}, #state{cons_ch = Ch}) ->
    try amqp_channel:subscribe(Ch, #'basic.consume'{queue = Queue, no_ack = false}, Pid) of
        #'basic.consume_ok'{consumer_tag = Tag} ->
            io:format("[RABBIT] Consumer registrato su ~s (~p)~n", [Queue, Pid]),
            Sub#sub{tag = Tag}
    catch
        Class:Err ->
            io:format("[RABBIT] Sottoscrizione a ~s fallita: ~p:~p~n", [Queue, Class, Err]),
            Sub
    end.

drop_sub(Pred, State) ->
    {ToDrop, Keep} = lists:partition(Pred, State#state.subs),
    lists:foreach(fun(S) -> cancel_sub(S, State) end, ToDrop),
    State#state{subs = Keep}.

cancel_sub(#sub{mref = MRef, tag = Tag}, #state{cons_ch = Ch}) ->
    demonitor_ref(MRef),
    case {Ch, Tag} of
        {undefined, _} -> ok;
        {_, undefined} -> ok;
        _ -> catch amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = Tag}), ok
    end.

%%====================================================================
%% Internal — accesso ai canali senza passare dal gen_server
%%====================================================================

lookup_channel(Key) ->
    try ets:lookup(?TAB, Key) of
        [{Key, Ch}] -> {ok, Ch};
        [] -> {error, not_connected}
    catch
        error:badarg -> {error, not_started}
    end.
