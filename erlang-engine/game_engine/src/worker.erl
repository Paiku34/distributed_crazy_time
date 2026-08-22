%%%-------------------------------------------------------------------
%% @doc Worker process — consuma le scommesse da bets_queue via AMQP e
%%      le inoltra al wheel_process come mappe gia' parsate.
%%      Runs as a gen_server under the main supervisor.
%%
%%      Il worker si registra come consumer presso il rabbitmq_manager,
%%      che sottoscrive la coda passando il PID di questo processo: le
%%      delivery arrivano quindi direttamente qui, e vengono confermate
%%      con un ack manuale solo dopo l'elaborazione.
%% @end
%%%-------------------------------------------------------------------
-module(worker).
-behaviour(gen_server).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(BETS_QUEUE, <<"bets_queue">>).

-record(state, {
    active = false :: boolean()
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    io:format("~n=================================~n"),
    io:format("  Worker RabbitMQ (AMQP) avviato~n"),
    io:format("  Consumer su bets_queue... (In attesa elezione)~n"),
    io:format("=================================~n~n"),
    ok = rabbitmq_manager:subscribe(?BETS_QUEUE, self()),
    {ok, #state{active = false}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(activate, State) ->
    io:format("[WORKER] ACTIVATO come leader — inizio elaborazione scommesse~n"),
    {noreply, State#state{active = true}};

handle_cast(deactivate, State) ->
    io:format("[WORKER] DISATTIVATO — in standby~n"),
    {noreply, State#state{active = false}};

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

handle_info({#'basic.deliver'{delivery_tag = Tag}, #amqp_msg{payload = Payload}}, State = #state{active = true}) ->
    %% L'ack viene sempre inviato, anche in caso di errore di parsing: un
    %% messaggio malformato rimesso in coda verrebbe riconsegnato all'infinito.
    try
        process_message(binary_to_list(Payload))
    catch
        Class:Err:Stack ->
            io:format("[WORKER] Errore elaborazione messaggio: ~p:~p~nStacktrace: ~p~n",
                      [Class, Err, Stack])
    end,
    rabbitmq_manager:ack(Tag),
    {noreply, State};

handle_info({#'basic.deliver'{delivery_tag = Tag}, _Msg}, State = #state{active = false}) ->
    %% Non sono il leader, rifiuto il messaggio e lo rimetto in coda per il leader
    rabbitmq_manager:reject(Tag, true),
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

%% PayloadStr e' il JSON della scommessa cosi' come arriva dal broker:
%% con AMQP non c'e' piu' il wrapper della Management API da sbucciare.
process_message(PayloadStr) ->
    case parse_bet_json(PayloadStr) of
        {ok, BetMap} ->
            io:format("[WORKER] Bet ricevuta: ~p~n", [BetMap]),
            case maps:get(<<"type">>, BetMap, <<"bet">>) of
                <<"force_segment">> ->
                    wheel_process:force_segment(maps:get(<<"segment">>, BetMap));
                <<"minigame_choice">> ->
                    wheel_process:submit_choice(
                        maps:get(<<"username">>, BetMap),
                        maps:get(<<"choice">>, BetMap));
                _ ->
                    case maps:get(<<"segment">>, BetMap, <<>>) of
                        <<"UNDO_BETS">> ->
                            Username = maps:get(<<"username">>, BetMap),
                            io:format("[WORKER] Comando UNDO per utente: ~s~n", [Username]),
                            TotalRefund = wheel_process:undo_bets(Username),
                            if TotalRefund > 0 ->
                                   io:format("[WORKER] Rimborso totale per ~s: ~p~n", [Username, TotalRefund]),
                                   RefundMap = #{<<"username">> => Username, <<"amount">> => TotalRefund, <<"segment">> => <<"REFUND">>},
                                   publish_refund(RefundMap);
                               true ->
                                   io:format("[WORKER] Nessuna scommessa da annullare per ~s~n", [Username])
                            end;
                        <<"FORCE_", Seg/binary>> ->
                            io:format("[WORKER] Comando FORZATURA segmento: ~s~n", [Seg]),
                            wheel_process:force_segment(Seg);
                        _ ->
                            case wheel_process:place_bet(BetMap) of
                                {ok, accepted} ->
                                    io:format("[WORKER] Bet accettata dal wheel_process.~n");
                                {error, betting_closed} ->
                                    io:format("[WORKER] Bet RIFIUTATA — scommesse chiuse. Invio rimborso.~n"),
                                    publish_refund(BetMap);
                                Other ->
                                    io:format("[WORKER] Risposta wheel_process: ~p~n", [Other])
                            end
                    end
            end;
        {error, Reason} ->
            io:format("[WORKER] Errore parsing bet: ~p~n", [Reason])
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

%% Pubblica un messaggio di rimborso su refunds_queue
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
