%%%-------------------------------------------------------------------
%% @doc Main Wheel Process — the core of the Crazy Time game.
%%      Matches the "Main Wheel Process" box in the PDF architecture.
%%
%%      Lifecycle of a round:
%%        1. BETTING phase (10 seconds countdown)
%%        2. SPINNING phase — wheel spins, winner segment selected
%%        3. If bonus → delegates to mini-game process
%%        4. RESULT — payouts calculated, published to RabbitMQ
%%        5. COOLDOWN (5 seconds) → back to BETTING
%% @end
%%%-------------------------------------------------------------------
-module(wheel_process).
-behaviour(gen_server).

-export([start_link/0, place_bet/1, get_state/0, force_segment/1, undo_bets/1, submit_choice/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(BET_DURATION, 10).   %% secondi per piazzare le scommesse
-define(COOLDOWN, 5000).     %% millisecondi di pausa tra round

-record(state, {
    phase = betting,          %% betting | spinning | minigame | cooldown
    time_left = ?BET_DURATION,
    round = 1,
    bets = [],                %% [{Username, Amount, Segment}, ...]
    forced_segment = undefined,
    history = [],             %% [{Segment, Multiplier}, ...]
    minigame_choices = #{}    %% #{Username => Choice}
}).

%%====================================================================
%% Canonical 54-segment wheel (same order as frontend)
%% This is the REAL Crazy Time distribution:
%%   21×1, 13×2, 7×5, 4×10, 4×Pachinko, 2×CoinFlip, 2×CashHunt, 1×CrazyTime
%%====================================================================
wheel_segments() ->
    [
        <<"CrazyTime">>, <<"1">>,       <<"2">>,       <<"5">>,       <<"1">>,       <<"2">>,
        <<"Pachinko">>,  <<"1">>,       <<"5">>,       <<"1">>,       <<"2">>,       <<"1">>,
        <<"CoinFlip">>,  <<"1">>,       <<"2">>,       <<"1">>,       <<"10">>,      <<"2">>,
        <<"CashHunt">>,  <<"1">>,       <<"2">>,       <<"1">>,       <<"5">>,       <<"1">>,
        <<"CoinFlip">>,  <<"1">>,       <<"5">>,       <<"2">>,       <<"10">>,      <<"1">>,
        <<"Pachinko">>,  <<"1">>,       <<"2">>,       <<"5">>,       <<"1">>,       <<"2">>,
        <<"CoinFlip">>,  <<"1">>,       <<"10">>,      <<"1">>,       <<"5">>,       <<"1">>,
        <<"CashHunt">>,  <<"1">>,       <<"2">>,       <<"5">>,       <<"1">>,       <<"2">>,
        <<"CoinFlip">>,  <<"2">>,       <<"1">>,       <<"10">>,      <<"2">>,       <<"1">>
    ].

%%====================================================================
%% API
%%====================================================================
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% place_bet/1 — chiamata dal worker quando arriva una scommessa da RabbitMQ.
%% Bet = #{username => ..., amount => ..., segment => ...}
place_bet(Bet) ->
    gen_server:call(?MODULE, {place_bet, Bet}).

get_state() ->
    gen_server:call(?MODULE, get_state).

force_segment(Seg) ->
    gen_server:cast(?MODULE, {force_segment, Seg}).

undo_bets(Username) ->
    gen_server:call(?MODULE, {undo_bets, Username}).

submit_choice(Username, Choice) ->
    gen_server:cast(?MODULE, {minigame_choice, Username, Choice}).

%%====================================================================
%% Callbacks
%%====================================================================
init([]) ->
    io:format("~n========================================~n"),
    io:format("  WHEEL PROCESS avviato (Round #1)~n"),
    io:format("  Fase: BETTING (~p secondi)~n", [?BET_DURATION]),
    io:format("========================================~n~n"),
    erlang:send_after(1000, self(), tick),
    {ok, #state{}}.

%% --- PLACE BET (solo durante betting) ---
handle_call({place_bet, Bet}, _From, State = #state{phase = betting, bets = Bets}) ->
    io:format("[WHEEL] Scommessa accettata: ~p~n", [Bet]),
    {reply, {ok, accepted}, State#state{bets = [Bet | Bets]}};
handle_call({place_bet, _Bet}, _From, State) ->
    io:format("[WHEEL] Scommessa RIFIUTATA — fase: ~p~n", [State#state.phase]),
    {reply, {error, betting_closed}, State};

%% --- GET STATE ---
handle_call(get_state, _From, State) ->
    Reply = #{
        phase => State#state.phase,
        time_left => State#state.time_left,
        round => State#state.round,
        num_bets => length(State#state.bets)
    },
    {reply, Reply, State};

handle_call({undo_bets, Username}, _From, State = #state{phase = betting, bets = Bets}) ->
    % Trova tutte le scommesse dell'utente
    UserBets = lists:filter(fun(B) -> maps:get(<<"username">>, B) == Username end, Bets),
    OtherBets = lists:filter(fun(B) -> maps:get(<<"username">>, B) =/= Username end, Bets),
    
    % Calcola il totale da rimborsare
    TotalRefund = lists:foldl(fun(B, Acc) -> Acc + maps:get(<<"amount">>, B) end, 0.0, UserBets),
    io:format("[WHEEL] Scommesse annullate per ~s: totale ~p~n", [Username, TotalRefund]),
    
    {reply, TotalRefund, State#state{bets = OtherBets}};
handle_call({undo_bets, _Username}, _From, State) ->
    % Se non è in fase betting, non rimborsiamo (oppure gestiamo altrimenti)
    {reply, 0.0, State};

handle_call(_Req, _From, State) ->
    {reply, {error, unknown}, State}.

handle_cast({force_segment, <<"NONE">>}, State) ->
    io:format("[WHEEL] Annullamento forzatura segmento (esito casuale)~n"),
    {noreply, State#state{forced_segment = undefined}};
handle_cast({force_segment, Seg}, State) ->
    io:format("[WHEEL] Forzando segmento per il prossimo giro: ~s~n", [Seg]),
    {noreply, State#state{forced_segment = Seg}};
    
handle_cast({minigame_choice, Username, Choice}, State) ->
    NewChoices = maps:put(Username, Choice, State#state.minigame_choices),
    {noreply, State#state{minigame_choices = NewChoices}};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% --- TICK durante BETTING (countdown > 0) ---
handle_info(tick, State = #state{phase = betting, time_left = T}) when T > 1 ->
    NewTime = T - 1,
    publish_timer(NewTime, State#state.round, State#state.history),
    erlang:send_after(1000, self(), tick),
    {noreply, State#state{time_left = NewTime}};

%% --- TICK durante BETTING (countdown = 1 → spin!) ---
handle_info(tick, State = #state{phase = betting, time_left = 1}) ->
    io:format("~n--- ROUND #~p: NO MORE BETS! SPINNING... ---~n", [State#state.round]),
    publish_timer(0, State#state.round, State#state.history),

    Segments = wheel_segments(),
    
    {WinnerIndex, WinnerSeg} = case State#state.forced_segment of
        undefined ->
            Idx = rand:uniform(54) - 1,
            {Idx, lists:nth(Idx + 1, Segments)};
        ForcedSeg ->
            Idx = find_segment_index(ForcedSeg, Segments, 0),
            {Idx, ForcedSeg}
    end,

    io:format("[WHEEL] La ruota si ferma su: ~s (indice ~p)~n", [WinnerSeg, WinnerIndex]),
    
    %% Reset forced_segment
    State1 = State#state{forced_segment = undefined},

    %% Determina se è un moltiplicatore diretto o un minigioco
    case segment_type(WinnerSeg) of
        {multiplier, Value} ->
            %% Pubblica spinning state con winner_index per l'animazione
            publish_spinning(State1#state.round, WinnerIndex, WinnerSeg, State1#state.history),
            %% Dopo 10.5s (tempo per l'animazione), risolvi il round
            erlang:send_after(10500, self(), {resolve_multiplier, WinnerSeg, Value, WinnerIndex}),
            {noreply, State1#state{phase = spinning, time_left = 0}};
        {minigame, Module} ->
            io:format("[WHEEL] BONUS! Entriamo in fase minigame: ~p~n", [Module]),
            %% Pubblica spinning state con winner_index
            publish_spinning(State1#state.round, WinnerIndex, WinnerSeg, State1#state.history),
            %% Dopo 10.5 secondi (tempo per l'animazione della ruota nel frontend), avvia il minigioco
            erlang:send_after(10500, self(), {start_minigame, WinnerSeg, Module, WinnerIndex}),
            {noreply, State1#state{phase = spinning, time_left = 0}}
    end;

%% --- RESOLVE MULTIPLIER (dopo animazione ruota) ---
handle_info({resolve_multiplier, WinnerSeg, Value, WinnerIndex}, State) ->
    resolve_round(WinnerSeg, Value, WinnerIndex, State#state.bets, State#state.round),
    NewHistory = lists:sublist([{WinnerSeg, Value} | State#state.history], 21),
    erlang:send_after(?COOLDOWN, self(), new_round),
    {noreply, State#state{phase = cooldown, time_left = 0, history = NewHistory}};

%% --- START MINIGAME (dopo che la ruota si è fermata nel frontend) ---
handle_info({start_minigame, SegName, Module, WinnerIndex}, State) ->
    BonusBets = [B || B <- State#state.bets, bet_segment(B) =:= SegName],
    AllBets = State#state.bets,

    %% Gioca il minigioco (calcola l'esito)
    case Module:play(BonusBets) of
        {async_minigame, Details} ->
            io:format("[WHEEL] Mini-game ~p in corso (attesa scelte utente per 16s)...~n", [Module]),
            %% Invia stato minigame con details
            publish_minigame_start(State#state.round, SegName, WinnerIndex, Details, State#state.history),
            %% Schedula la risoluzione vera e propria tra 16 secondi (5s scelta + 11s animazione ruota)
            WaitTimeAsync = 16000,
            erlang:send_after(WaitTimeAsync, self(), {resolve_async_minigame, SegName, Details, BonusBets, WinnerIndex}),
            {noreply, State#state{phase = minigame, time_left = 16}};
        {ok, Multiplier, Details} ->
            publish_minigame_start(State#state.round, SegName, State#state.history),
            io:format("[WHEEL] Mini-game ~p completato. Moltiplicatore: x~p~n", [Module, Multiplier]),
            %% Risolve subito il risultato e lo pubblica (così il frontend avvia l'animazione)
            resolve_round_with_bonus(SegName, Multiplier, Details, AllBets, WinnerIndex, State#state.round),
            NewHistory = lists:sublist([{SegName, Multiplier} | State#state.history], 21),
            WaitTime = case Module of
                pachinko ->
                    DropsList = maps:get(drops, Details, []),
                    2500 + (length(DropsList) * 12000); %% 12s per drop in animation
                coinflip ->
                    14000; %% Slower coinflip randomizer
                cashhunt ->
                    35000; %% 8s scroll + 2s cover + 6s shuffle + 10s pick + 3s reveal + 6s result
                _ -> 
                    14000
            end,
            %% Attendi il tempo calcolato + 5s cooldown
            erlang:send_after(WaitTime + ?COOLDOWN, self(), new_round),
            {noreply, State#state{phase = minigame, time_left = WaitTime div 1000, history = NewHistory}};
        {ok, Multiplier} ->
            publish_minigame_start(State#state.round, SegName, State#state.history),
            io:format("[WHEEL] Mini-game ~p completato. Moltiplicatore: x~p~n", [Module, Multiplier]),
            resolve_round_with_bonus(SegName, Multiplier, #{}, AllBets, WinnerIndex, State#state.round),
            NewHistory = lists:sublist([{SegName, Multiplier} | State#state.history], 21),
            erlang:send_after(14000 + ?COOLDOWN, self(), new_round),
            {noreply, State#state{phase = minigame, time_left = 14, history = NewHistory}};
        {error, Reason} ->
            publish_minigame_start(State#state.round, SegName, State#state.history),
            io:format("[WHEEL] Errore mini-game ~p: ~p. Rimborso.~n", [Module, Reason]),
            resolve_round(SegName, 1, WinnerIndex, AllBets, State#state.round),
            NewHistory = lists:sublist([{SegName, 1} | State#state.history], 21),
            erlang:send_after(?COOLDOWN, self(), new_round),
            {noreply, State#state{phase = cooldown, time_left = 0, history = NewHistory}}
    end;

handle_info({resolve_async_minigame, SegName, Details, BonusBets, WinnerIndex}, State) ->
    io:format("[WHEEL] Risoluzione async minigame ~p! Calcolo vincite...~n", [SegName]),
    Payouts = crazytime:compute_payouts(Details, BonusBets, State#state.minigame_choices),
    %% Publish the final results with payouts to results_queue
    Payload = build_result_json(SegName, <<"minigame">>, 0, WinnerIndex, Details, Payouts, State#state.round),
    publish_to_queue("results_queue", Payload),
    
    %% Add to history (just picking blue_multiplier for history display)
    BlueMult = maps:get(blue_multiplier, Details),
    NewHistory = lists:sublist([{SegName, BlueMult} | State#state.history], 21),
    
    erlang:send_after(?COOLDOWN, self(), new_round),
    {noreply, State#state{phase = cooldown, time_left = 0, history = NewHistory}};

%% (Non c'è più bisogno di finish_minigame perché lo facciamo sincrono)

%% --- NEW ROUND ---
handle_info(new_round, State) ->
    NewRound = State#state.round + 1,
    io:format("~n========================================~n"),
    io:format("  NUOVO ROUND #~p — BETTING APERTO~n", [NewRound]),
    io:format("========================================~n~n"),
    publish_timer(?BET_DURATION, NewRound, State#state.history),
    erlang:send_after(1000, self(), tick),
    {noreply, State#state{phase = betting, time_left = ?BET_DURATION, round = NewRound, bets = [], minigame_choices = #{}}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal Functions
%%====================================================================

find_segment_index(Target, [Target|_], Idx) -> Idx;
find_segment_index(Target, [_|T], Idx) -> find_segment_index(Target, T, Idx+1);
find_segment_index(_, [], _) -> 0.

%% Determina il tipo di segmento
segment_type(<<"1">>)         -> {multiplier, 1};
segment_type(<<"2">>)         -> {multiplier, 2};
segment_type(<<"5">>)         -> {multiplier, 5};
segment_type(<<"10">>)        -> {multiplier, 10};
segment_type(<<"Pachinko">>)  -> {minigame, pachinko};
segment_type(<<"CoinFlip">>)  -> {minigame, coinflip};
segment_type(<<"CashHunt">>)  -> {minigame, cashhunt};
segment_type(<<"CrazyTime">>) -> {minigame, crazytime}.

%% Estrae il segmento scommesso da una bet (che è una mappa)
bet_segment(Bet) when is_map(Bet) ->
    maps:get(<<"segment">>, Bet, <<>>);
bet_segment(_) ->
    <<>>.

%% Risolve il round per scommesse con moltiplicatore diretto
resolve_round(WinnerSeg, Multiplier, WinnerIndex, Bets, Round) ->
    Payouts = compute_payouts(WinnerSeg, Multiplier, Bets),
    Payload = build_result_json(WinnerSeg, <<"multiplier">>, Multiplier, WinnerIndex, #{}, Payouts, Round),
    publish_to_queue("results_queue", Payload),
    io:format("[WHEEL] Round #~p risolto. Vincitore: ~s (x~p). Pagamenti: ~p~n",
              [Round, WinnerSeg, Multiplier, length(Payouts)]).

%% Risolve il round per bonus mini-game
resolve_round_with_bonus(BonusSeg, Multiplier, Details, Bets, WinnerIndex, Round) ->
    Payouts = compute_payouts(BonusSeg, Multiplier, Bets),
    Payload = build_result_json(BonusSeg, <<"minigame">>, Multiplier, WinnerIndex, Details, Payouts, Round),
    publish_to_queue("results_queue", Payload),
    io:format("[WHEEL] Round #~p risolto (BONUS ~s, x~p). Pagamenti: ~p~n",
              [Round, BonusSeg, Multiplier, length(Payouts)]).

%% Calcola i pagamenti per ogni giocatore
compute_payouts(WinnerSeg, Multiplier, Bets) ->
    lists:filtermap(fun(Bet) ->
        Seg = bet_segment(Bet),
        case Seg =:= WinnerSeg of
            true ->
                Username = maps:get(<<"username">>, Bet, <<"unknown">>),
                Amount = maps:get(<<"amount">>, Bet, 0),
                Payout = Amount + (Amount * Multiplier),
                {true, #{username => Username, bet => Amount, payout => Payout}};
            false ->
                false
        end
    end, [B || B <- Bets, is_map(B)]).

%% Costruisce il JSON del risultato con winner_index e dettagli minigioco
build_result_json(Winner, Type, Multiplier, WinnerIndex, Details, Payouts, Round) ->
    PayoutsJson = lists:map(fun(P) ->
        io_lib:format("{\"username\":\"~s\",\"bet\":~p,\"payout\":~p}",
                      [maps:get(username, P), maps:get(bet, P), maps:get(payout, P)])
    end, Payouts),
    PayoutsStr = "[" ++ string:join([lists:flatten(J) || J <- PayoutsJson], ",") ++ "]",
    DetailsStr = build_details_json(Details),
    lists:flatten(io_lib:format(
        "{\"type\":\"result\",\"round\":~p,\"winner\":\"~s\",\"result_type\":\"~s\",\"multiplier\":~p,\"winner_index\":~p,\"details\":~s,\"payouts\":~s}",
        [Round, Winner, Type, Multiplier, WinnerIndex, DetailsStr, PayoutsStr])).

%% Serializza i dettagli del minigioco come JSON
build_details_json(Details) when map_size(Details) =:= 0 ->
    "{}";
build_details_json(Details) ->
    Pairs = maps:fold(fun(K, V, Acc) ->
        KStr = atom_to_list(K),
        VStr = json_value(V),
        ["\"" ++ KStr ++ "\":" ++ VStr | Acc]
    end, [], Details),
    "{" ++ string:join(Pairs, ",") ++ "}".

%% Serializza un singolo valore Erlang in JSON string
json_value(N) when is_integer(N) -> integer_to_list(N);
json_value(N) when is_float(N) -> float_to_list(N, [{decimals, 2}]);
json_value(B) when is_binary(B) -> "\"" ++ binary_to_list(B) ++ "\"";
json_value(A) when is_atom(A) -> "\"" ++ atom_to_list(A) ++ "\"";
json_value(M) when is_map(M) ->
    Pairs = maps:fold(fun(K, V, Acc) ->
        KStr = if is_atom(K) -> atom_to_list(K); is_binary(K) -> binary_to_list(K); true -> lists:flatten(io_lib:format("~p", [K])) end,
        VStr = json_value(V),
        ["\"" ++ KStr ++ "\":" ++ VStr | Acc]
    end, [], M),
    "{" ++ string:join(Pairs, ",") ++ "}";
json_value(L) when is_list(L) ->
    Items = lists:map(fun(I) -> json_value(I) end, L),
    "[" ++ string:join(Items, ",") ++ "]";
json_value(Other) -> "\"" ++ lists:flatten(io_lib:format("~p", [Other])) ++ "\"".

%% Formatta la history in JSON list
format_history(History) ->
    Entries = lists:map(fun({Seg, Mult}) ->
        io_lib:format("{\"winner\":\"~s\",\"multiplier\":~p}", [Seg, Mult])
    end, History),
    "[" ++ string:join([lists:flatten(E) || E <- Entries], ",") ++ "]".

%% Pubblica il timer sulla state_queue
publish_timer(TimeLeft, Round, History) ->
    Phase = if TimeLeft > 0 -> "betting"; true -> "spinning" end,
    HistStr = format_history(History),
    Payload = lists:flatten(io_lib:format(
        "{\"type\":\"timer\",\"round\":~p,\"time_left\":~p,\"phase\":\"~s\",\"history\":~s}",
        [Round, TimeLeft, Phase, HistStr])),
    publish_to_queue("state_queue", Payload).

%% Pubblica stato spinning con winner_index
publish_spinning(Round, WinnerIndex, WinnerSeg, History) ->
    HistStr = format_history(History),
    Payload = lists:flatten(io_lib:format(
        "{\"type\":\"timer\",\"round\":~p,\"time_left\":0,\"phase\":\"spinning\",\"winner_index\":~p,\"winner\":\"~s\",\"history\":~s}",
        [Round, WinnerIndex, WinnerSeg, HistStr])),
    publish_to_queue("state_queue", Payload).

%% Pubblica stato minigame_start senza details
publish_minigame_start(Round, MinigameName, History) ->
    HistStr = format_history(History),
    Payload = lists:flatten(io_lib:format(
        "{\"type\":\"timer\",\"round\":~p,\"time_left\":7,\"phase\":\"minigame\",\"minigame\":\"~s\",\"history\":~s}",
        [Round, MinigameName, HistStr])),
    publish_to_queue("state_queue", Payload).

%% Pubblica stato minigame_start CON details per animazione asincrona
publish_minigame_start(Round, MinigameName, WinnerIndex, Details, History) ->
    HistStr = format_history(History),
    DetailsJSON = json_value(Details),
    Payload = lists:flatten(io_lib:format(
        "{\"type\":\"timer\",\"round\":~p,\"time_left\":16,\"phase\":\"minigame\",\"minigame\":\"~s\",\"winner_index\":~p,\"details\":~s,\"history\":~s}",
        [Round, MinigameName, WinnerIndex, DetailsJSON, HistStr])),
    publish_to_queue("state_queue", Payload).

%% Pubblica un messaggio su una coda RabbitMQ via HTTP Management API
publish_to_queue(QueueName, Payload) ->
    Url = "http://localhost:15672/api/exchanges/%2f/amq.default/publish",
    Headers = [{"Authorization", "Basic Z3Vlc3Q6Z3Vlc3Q="}],
    %% Escape le virgolette nel payload per il JSON wrapper
    EscapedPayload = lists:flatten(string:replace(Payload, "\"", "\\\"", all)),
    Body = "{\"properties\":{},\"routing_key\":\"" ++ QueueName ++ "\",\"payload\":\"" ++ EscapedPayload ++ "\",\"payload_encoding\":\"string\"}",
    case httpc:request(post, {Url, Headers, "application/json", Body}, [], []) of
        {ok, {{_, 200, _}, _, _}} -> ok;
        {ok, {{_, Code, _}, _, Resp}} ->
            io:format("[WHEEL] Errore RabbitMQ HTTP ~p: ~s~n", [Code, Resp]);
        {error, Reason} ->
            io:format("[WHEEL] Errore connessione RabbitMQ: ~p~n", [Reason])
    end.
