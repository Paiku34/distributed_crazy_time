%%%-------------------------------------------------------------------
%% @doc Worker process — consuma le scommesse da bets_queue via AMQP e
%%      le inoltra al wheel_process del LEADER.
%%
%%      Il worker gira su TUTTI i nodi del cluster: i worker sono
%%      competing consumers della stessa coda, quindi l'ingestione delle
%%      scommesse e' distribuita. Solo wheel_process e' leader-only.
%%
%%      Ne segue che ogni messaggio consumato va instradato al nodo
%%      leader, non al wheel_process locale: su uno standby quel processo
%%      e' dormiente e il messaggio sparirebbe in silenzio. L'identita'
%%      del leader arriva da leader_election con {set_leader, Node}.
%%
%%      Se non c'e' un leader noto (nessuna elezione conclusa, oppure
%%      nodo finito nella minoranza di una partizione) il messaggio viene
%%      rimesso in coda: restera' nel broker finche' un nodo che puo'
%%      servirlo lo consumera'.
%% @end
%%%-------------------------------------------------------------------
-module(worker).
-behaviour(gen_server).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(BETS_QUEUE, <<"bets_queue">>).

%% Timeout della call verso il wheel del leader. Deve essere piu' alto dei
%% 10 s in cui il wheel resta bloccato nella call al minigioco: con il
%% default di 5 s il worker crollerebbe ogni volta che una scommessa in
%% ritardo arriva durante un bonus.
-define(WHEEL_CALL_TIMEOUT, 15000).

-record(state, {
    leader = undefined :: node() | undefined
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    io:format("~n=================================~n"),
    io:format("  Worker RabbitMQ (AMQP) avviato~n"),
    io:format("  Consumer su bets_queue (tutti i nodi)~n"),
    io:format("=================================~n~n"),
    ok = rabbitmq_manager:subscribe(?BETS_QUEUE, self()),
    %% Dopo un restart del worker nessuno ri-annuncia il leader: ce lo
    %% facciamo dire subito da leader_election, che parte prima di noi.
    Leader = current_leader(),
    {ok, #state{leader = Leader}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({set_leader, Node}, State) ->
    case Node =:= State#state.leader of
        true  -> ok;
        false -> io:format("[WORKER] Leader corrente: ~p~n", [Node])
    end,
    {noreply, State#state{leader = Node}};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% Conferme del broker alla sottoscrizione / cancellazione: nulla da fare.
handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};
handle_info(#'basic.cancel_ok'{}, State) ->
    {noreply, State};
handle_info(#'basic.cancel'{}, State) ->
    io:format("[WORKER] Consumer cancellato dal broker.~n"),
    {noreply, State};

%% Nessun leader noto: il messaggio torna al broker, che lo fara' servire
%% da un nodo in grado di elaborarlo. E' l'unico caso in cui si rifiuta.
handle_info({#'basic.deliver'{delivery_tag = Tag}, _Msg}, State = #state{leader = undefined}) ->
    io:format("[WORKER] Nessun leader eletto: messaggio rimesso in coda~n"),
    rabbitmq_manager:reject(Tag, true),
    {noreply, State};

handle_info({#'basic.deliver'{delivery_tag = Tag}, #amqp_msg{payload = Payload}},
            State = #state{leader = Leader}) ->
    %% process_message/3 decide da se' se ackare o rimettere in coda.
    %% Su errore di elaborazione si acka comunque: un messaggio malformato
    %% rimesso in coda verrebbe riconsegnato all'infinito.
    try
        process_message(binary_to_list(Payload), Tag, Leader)
    catch
        Class:Err:Stack ->
            io:format("[WORKER] Errore elaborazione messaggio: ~p:~p~nStacktrace: ~p~n",
                      [Class, Err, Stack]),
            rabbitmq_manager:ack(Tag)
    end,
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal — parsing e forwarding
%%====================================================================

%% Chiede a leader_election chi e' il leader corrente. Usata solo all'avvio.
current_leader() ->
    try leader_election:get_leader() of
        {ok, Node} -> Node;
        _          -> undefined
    catch
        _:_ -> undefined
    end.

%% PayloadStr e' il JSON della scommessa cosi' come arriva dal broker.
%% Tutti i percorsi vengono instradati al wheel_process del LEADER: se
%% finissero al wheel_process locale, su uno standby sparirebbero in
%% silenzio (con 3 nodi succede circa 2 volte su 3).
process_message(PayloadStr, Tag, Leader) ->
    case parse_bet_json(PayloadStr) of
        {ok, BetMap} ->
            io:format("[WORKER] Messaggio ricevuto: ~p~n", [BetMap]),
            case maps:get(<<"type">>, BetMap, <<"bet">>) of
                <<"force_segment">> ->
                    cast_to_wheel(Leader, {force_segment, maps:get(<<"segment">>, BetMap)}),
                    rabbitmq_manager:ack(Tag);
                <<"minigame_choice">> ->
                    cast_to_wheel(Leader, {minigame_choice,
                                           maps:get(<<"username">>, BetMap),
                                           maps:get(<<"choice">>, BetMap)}),
                    rabbitmq_manager:ack(Tag);
                _ ->
                    case maps:get(<<"segment">>, BetMap, <<>>) of
                        <<"UNDO_BETS">> ->
                            Username = maps:get(<<"username">>, BetMap),
                            io:format("[WORKER] Comando UNDO per utente: ~s~n", [Username]),
                            %% cast, non call: come call cross-nodo andrebbe in
                            %% timeout quando il wheel e' bloccato nel minigioco.
                            %% Il rimborso lo pubblica il wheel, che conosce gli
                            %% importi annullati.
                            cast_to_wheel(Leader, {undo_bets, Username}),
                            rabbitmq_manager:ack(Tag);
                        <<"FORCE_", Seg/binary>> ->
                            io:format("[WORKER] Comando FORZATURA segmento: ~s~n", [Seg]),
                            cast_to_wheel(Leader, {force_segment, Seg}),
                            rabbitmq_manager:ack(Tag);
                        _ ->
                            forward_bet(BetMap, Tag, Leader)
                    end
            end;
        {error, Reason} ->
            io:format("[WORKER] Errore parsing bet: ~p~n", [Reason]),
            rabbitmq_manager:ack(Tag)
    end.

cast_to_wheel(Leader, Msg) ->
    gen_server:cast({wheel_process, Leader}, Msg).

%% Inoltro della scommessa al wheel del leader.
%%
%% NOTA: impalcatura temporanea. Il canale worker -> wheel diventera'
%% asincrono ({bet, _} in cast con {bet_result, _, _} di ritorno) insieme
%% all'ack differito; fino ad allora la call sincrona con timeout esplicito
%% e' cio' che garantisce che nessuna scommessa venga ackata senza esito.
forward_bet(BetMap, Tag, Leader) ->
    try gen_server:call({wheel_process, Leader}, {place_bet, BetMap}, ?WHEEL_CALL_TIMEOUT) of
        {ok, accepted} ->
            io:format("[WORKER] Bet accettata dal wheel_process su ~p.~n", [Leader]),
            rabbitmq_manager:ack(Tag);
        {error, betting_closed} ->
            io:format("[WORKER] Bet RIFIUTATA — scommesse chiuse. Invio rimborso.~n"),
            publish_refund(BetMap),
            rabbitmq_manager:ack(Tag);
        {error, not_leader} ->
            %% Il leader e' cambiato mentre il messaggio era in volo.
            io:format("[WORKER] ~p non e' piu' leader: bet rimessa in coda~n", [Leader]),
            rabbitmq_manager:reject(Tag, true);
        Other ->
            io:format("[WORKER] Risposta inattesa dal wheel_process: ~p~n", [Other]),
            rabbitmq_manager:reject(Tag, true)
    catch
        exit:Reason ->
            %% Leader morto, irraggiungibile o bloccato oltre il timeout.
            io:format("[WORKER] Wheel su ~p non raggiungibile (~p): bet rimessa in coda~n",
                      [Leader, Reason]),
            rabbitmq_manager:reject(Tag, true)
    end.

%% Parser semplice per JSON
parse_bet_json(JsonStr) ->
    try
        TypeMatch = extract_string_field(JsonStr, "type"),
        Type = case TypeMatch of {ok, T} -> list_to_binary(T); _ -> <<>> end,

        UsernameMatch = extract_string_field(JsonStr, "username"),
        Username = case UsernameMatch of {ok, U} -> list_to_binary(U); _ -> <<"unknown">> end,

        case Type of
            <<"minigame_choice">> ->
                ChoiceMatch = extract_string_field(JsonStr, "choice"),
                Choice = case ChoiceMatch of {ok, C} -> list_to_binary(C); _ -> <<"blue">> end,
                {ok, #{<<"type">> => Type,
                       <<"username">> => Username,
                       <<"choice">> => Choice}};
            <<"force_segment">> ->
                SegmentMatch = extract_string_field(JsonStr, "segment"),
                Segment = case SegmentMatch of {ok, S} -> list_to_binary(S); _ -> <<>> end,
                {ok, #{<<"type">> => Type,
                       <<"segment">> => Segment}};
            _ ->
                AmountMatch = extract_number_field(JsonStr, "amount"),
                SegmentMatch = extract_string_field(JsonStr, "segment"),
                case {AmountMatch, SegmentMatch} of
                    {{ok, A}, {ok, S}} ->
                        {ok, #{<<"type">> => Type,
                               <<"username">> => Username,
                               <<"amount">> => A,
                               <<"segment">> => list_to_binary(S)}};
                    _ ->
                        io:format("[WORKER] DEBUG: missing_fields in JSON: ~p~n", [JsonStr]),
                        {error, {missing_fields, JsonStr}}
                end
        end
    catch
        Class:Err:Stack ->
            io:format("[WORKER] EXCEPTION in parse_bet_json: ~p:~p~nStacktrace: ~p~n", [Class, Err, Stack]),
            {error, Err}
    end.

extract_string_field(Json, FieldName) ->
    Pattern = "\"" ++ FieldName ++ "\"\\s*:\\s*\"([^\"]+)\"",
    case re:run(Json, Pattern, [{capture, all_but_first, list}]) of
        {match, [Value]} -> {ok, Value};
        _ -> {error, not_found}
    end.

extract_number_field(Json, FieldName) ->
    Pattern = "\"" ++ FieldName ++ "\"\\s*:\\s*([0-9]+\\.?[0-9]*)",
    case re:run(Json, Pattern, [{capture, all_but_first, list}]) of
        {match, [Value]} ->
            case string:find(Value, ".") of
                nomatch -> {ok, list_to_integer(Value)};
                _ -> {ok, list_to_float(Value)}
            end;
        _ -> {error, not_found}
    end.

escape_json_string(Str) when is_binary(Str) ->
    escape_json_string(binary_to_list(Str));
escape_json_string(Str) ->
    lists:flatmap(fun($") -> "\\\""; ($\\) -> "\\\\"; (C) -> [C] end, Str).

%% Pubblica un messaggio di rimborso su refunds_queue.
%% Resta qui il solo caso "scommessa rifiutata perche' le puntate sono
%% chiuse": il rimborso dell'UNDO e' passato al wheel, che conosce gli
%% importi. Entrambi diventeranno eventi bet_rejected per bet_id.
publish_refund(BetMap) ->
    Username = maps:get(<<"username">>, BetMap, <<"unknown">>),
    Amount = maps:get(<<"amount">>, BetMap, 0),
    Payload = lists:flatten(io_lib:format(
        "{\"username\":\"~s\",\"amount\":~p,\"reason\":\"betting_closed\"}",
        [escape_json_string(Username), Amount])),
    case rabbitmq_manager:publish(<<"refunds_queue">>, unicode:characters_to_binary(Payload)) of
        ok ->
            io:format("[WORKER] Rimborso pubblicato per ~s ($~p)~n", [Username, Amount]);
        {error, Reason} ->
            io:format("[WORKER] Errore pubblicazione rimborso: ~p~n", [Reason])
    end.
