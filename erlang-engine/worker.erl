-module(worker).
-export([start/0, poll/0]).

start() ->
    inets:start(),
    io:format("~n=================================~n"),
    io:format("Erlang Worker (OTP 28 Native) avviato!~n"),
    io:format("In ascolto su RabbitMQ via HTTP...~n"),
    io:format("=================================~n~n"),
    poll().

poll() ->
    Url = "http://localhost:15672/api/queues/%2F/bets_queue/get",
    Headers = [{"Authorization", "Basic Z3Vlc3Q6Z3Vlc3Q="}],
    Body = "{\"count\":1,\"ackmode\":\"ack_requeue_false\",\"encoding\":\"auto\"}",
    Request = {Url, Headers, "application/json", Body},
    
    case httpc:request(post, Request, [], []) of
        {ok, {{_, 200, _}, _, ResponseBody}} ->
            case ResponseBody of
                "[]" ->
                    timer:sleep(2000),
                    poll();
                _ ->
                    io:format("~n[ERLANG GAME ENGINE] MESSAGGIO RICEVUTO DA JAVA:~n~s~n~n", [ResponseBody]),
                    timer:sleep(2000),
                    poll()
            end;
        {ok, {{_, 404, _}, _, _}} ->
            io:format("Coda 'bets_queue' non ancora presente su RabbitMQ. In attesa...~n"),
            timer:sleep(2000),
            poll();
        {ok, {{_, Code, _}, _, _}} ->
            io:format("Risposta HTTP ~p da RabbitMQ. In attesa...~n", [Code]),
            timer:sleep(2000),
            poll();
        {error, Reason} ->
            io:format("Errore di connessione a RabbitMQ: ~p~n", [Reason]),
            timer:sleep(3000),
            poll()
    end.