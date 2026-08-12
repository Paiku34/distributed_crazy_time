%%%-------------------------------------------------------------------
%% @doc Worker process — polls RabbitMQ for incoming bets and
%%      forwards them to the wheel_process as parsed maps.
%%      Runs as a gen_server under the main supervisor.
%% @end
%%%-------------------------------------------------------------------
-module(worker).
-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(POLL_IDLE, 2000).   %% intervallo polling quando la coda e' vuota (ms)
-define(POLL_ACTIVE, 100).  %% intervallo polling quando ci sono messaggi (ms)

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    inets:start(),
    io:format("~n=================================~n"),
    io:format("  Worker RabbitMQ Poller avviato~n"),
    io:format("  Polling bets_queue...~n"),
    io:format("=================================~n~n"),
    self() ! poll,
    {ok, #{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(poll, State) ->
    Url = "http://localhost:15672/api/queues/%2F/bets_queue/get",
    Headers = [{"Authorization", "Basic Z3Vlc3Q6Z3Vlc3Q="}],
    Body = "{\"count\":1,\"ackmode\":\"ack_requeue_false\",\"encoding\":\"auto\"}",
    Request = {Url, Headers, "application/json", Body},
    
    case httpc:request(post, Request, [], []) of
        {ok, {{_, 200, _}, _, ResponseBody}} ->
            case ResponseBody of
                "[]" ->
                    erlang:send_after(?POLL_IDLE, self(), poll);
                _ ->
                    process_message(ResponseBody),
                    erlang:send_after(?POLL_ACTIVE, self(), poll)
            end;
        {ok, {{_, 404, _}, _, _}} ->
            io:format("[WORKER] Coda 'bets_queue' non trovata. Attendo...~n"),
            erlang:send_after(?POLL_IDLE, self(), poll);
        {ok, {{_, Code, _}, _, _}} ->
            io:format("[WORKER] Risposta HTTP ~p da RabbitMQ.~n", [Code]),
            erlang:send_after(?POLL_IDLE, self(), poll);
        {error, Reason} ->
            io:format("[WORKER] Errore connessione RabbitMQ: ~p~n", [Reason]),
            erlang:send_after(3000, self(), poll)
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

process_message(ResponseBody) ->
    %% Estraiamo il payload dal JSON della risposta RabbitMQ Management API
    case extract_payload(ResponseBody) of
        {ok, PayloadStr} ->
            %% Parsing del payload JSON della scommessa
            case parse_bet_json(PayloadStr) of
                {ok, BetMap} ->
                    io:format("[WORKER] Bet ricevuta: ~p~n", [BetMap]),
                    case maps:get(<<"segment">>, BetMap, <<>>) of
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
                    end;
                {error, Reason} ->
                    io:format("[WORKER] Errore parsing bet: ~p~n", [Reason])
            end;
        {error, Reason} ->
            io:format("[WORKER] Errore estrazione payload: ~p~n", [Reason])
    end.

extract_payload(ResponseBody) ->
    case re:run(ResponseBody, "\"payload\":\"(.*?)\",\"payload_encoding\"", [{capture, all_but_first, list}]) of
        {match, [Escaped]} ->
            %% Rimuoviamo i backslash di escape
            Clean = re:replace(Escaped, "\\\\\"", "\"", [global, {return, list}]),
            {ok, Clean};
        _ ->
            {error, no_payload_found}
    end.

%% Parser semplice per JSON
parse_bet_json(JsonStr) ->
    try
        Username = extract_string_field(JsonStr, "username"),
        Amount = extract_number_field(JsonStr, "amount"),
        Segment = extract_string_field(JsonStr, "segment"),
        case {Username, Amount, Segment} of
            {{ok, U}, {ok, A}, {ok, S}} ->
                {ok, #{<<"username">> => list_to_binary(U),
                       <<"amount">> => A,
                       <<"segment">> => list_to_binary(S)}};
            _ ->
                {error, missing_fields}
        end
    catch
        _:Err -> {error, Err}
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

%% Pubblica un messaggio di rimborso su refunds_queue
publish_refund(BetMap) ->
    Username = maps:get(<<"username">>, BetMap, <<"unknown">>),
    Amount = maps:get(<<"amount">>, BetMap, 0),
    Url = "http://localhost:15672/api/exchanges/%2f/amq.default/publish",
    Headers = [{"Authorization", "Basic Z3Vlc3Q6Z3Vlc3Q="}],
    Payload = lists:flatten(io_lib:format(
        "{\"username\":\"~s\",\"amount\":~p,\"reason\":\"betting_closed\"}",
        [Username, Amount])),
    EscapedPayload = lists:flatten(string:replace(Payload, "\"", "\\\"", all)),
    Body = "{\"properties\":{},\"routing_key\":\"refunds_queue\",\"payload\":\"" ++ EscapedPayload ++ "\",\"payload_encoding\":\"string\"}",
    case httpc:request(post, {Url, Headers, "application/json", Body}, [], []) of
        {ok, {{_, 200, _}, _, _}} ->
            io:format("[WORKER] Rimborso pubblicato per ~s ($~p)~n", [Username, Amount]);
        {error, Reason} ->
            io:format("[WORKER] Errore pubblicazione rimborso: ~p~n", [Reason]);
        _ -> ok
    end.
