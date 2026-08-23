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

-include("game_engine.hrl").

-export([start_link/0, place_bet/1, get_state/0, force_segment/1, undo_bets/1, submit_choice/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(BET_DURATION, 10).   %% secondi per piazzare le scommesse
-define(COOLDOWN, 5000).     %% millisecondi di pausa tra round

-record(state, {
    phase = betting,          %% betting | spinning | minigame | cooldown
    time_left = ?BET_DURATION,
    round = 1,
    bets = [],                %% [BetMap, ...] scommesse accettate del round
    forced_segment = undefined,
    history = [],             %% [{Segment, Multiplier}, ...]
    minigame_choices = #{},   %% #{Username => Choice}
    active = false,           %% only leader processes game ticks
    %% Esito del round nello stato del processo: prima viveva solo dentro i
    %% messaggi send_after in volo, quindi un nuovo leader eletto dopo un crash
    %% non aveva modo di sapere su cosa si fosse fermata la ruota.
    winner_segment   = undefined,
    winner_index     = undefined,
    minigame_mod     = undefined,
    minigame_details = undefined,
    timer_ref        = undefined,  %% timer di fase, ispezionabile e cancellabile
    %% Stato del taglio Chandy-Lamport (vedi cl_recorder).
    cl = cl_recorder:new(),
    %% bet_id gia' liquidate negli ultimi round, ricaricate dai checkpoint.
    %% Serve alla regola R3: `bets` viene azzerato a ogni round, quindi da
    %% solo copre la riconsegna intra-round ma non quella che arriva dopo.
    settled_bet_ids = sets:new()
}).

%% Quanti round di storia tenere nell'insieme di deduplica.
-define(SETTLED_ROUNDS, 3).
%% Se il collector muore, il partecipante non deve restare in registrazione
%% per sempre: chiude da solo e riporta cio' che ha.
-define(CL_ABORT_TIMEOUT, 10000).

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

%% place_bet/1 — inoltro asincrono di una scommessa (usata dal worker locale
%% e dalla shell). L'esito torna come {bet_result, BetId, Verdict} al mittente.
place_bet(Bet) ->
    gen_server:cast(?MODULE, {bet, Bet, node()}).

get_state() ->
    gen_server:call(?MODULE, get_state).

force_segment(Seg) ->
    gen_server:cast(?MODULE, {force_segment, Seg}).

%% cast, non call: il worker lo invoca da un altro nodo e una call
%% andrebbe in timeout mentre il wheel e' bloccato nel minigioco.
%% Il rimborso non torna piu' come valore di ritorno: lo pubblica il wheel.
undo_bets(Username) ->
    gen_server:cast(?MODULE, {undo_bets, Username}).

submit_choice(Username, Choice) ->
    gen_server:cast(?MODULE, {minigame_choice, Username, Choice}).

%%====================================================================
%% Callbacks
%%====================================================================
init([]) ->
    io:format("~n========================================~n"),
    io:format("  WHEEL PROCESS avviato (In attesa elezione)~n"),
    io:format("========================================~n~n"),
    {ok, #state{active = false}}.

%% --- GET STATE ---
handle_call(get_state, _From, State) ->
    Reply = #{
        phase => State#state.phase,
        time_left => State#state.time_left,
        round => State#state.round,
        num_bets => length(State#state.bets),
        winner_segment => State#state.winner_segment,
        winner_index => State#state.winner_index
    },
    {reply, Reply, State};

handle_call(_Req, _From, State) ->
    {reply, {error, unknown}, State}.

%% --- SCOMMESSA (asincrona, dal worker che l'ha consumata dal broker) ---
%%
%% L'esito torna al worker mittente: solo lui possiede il delivery tag da
%% ackare. Tre verdetti: accepted, rejected (con rimborso), not_leader
%% (il worker rimette la scommessa in coda invece di scartarla).
%% Taglio in corso e canale ancora aperto: la scommessa e' IN TRANSITO.
%% Entra nello stato del canale e verra' unita alle bet del round quando il
%% taglio si chiude; l'esito al worker parte da li'. Aggiungerla anche a
%% `bets` adesso significherebbe contarla due volte.
handle_cast({bet, BetMap, FromNode}, State = #state{active = true, cl = CL}) when CL =/= undefined ->
    case lists:member({worker, FromNode}, cl_recorder:in_open(CL)) of
        true ->
            io:format("[WHEEL] Bet ~s in transito al taglio: registrata sul canale~n",
                      [fmt_id(bet_id(BetMap))]),
            {noreply, State#state{cl = cl_recorder:on_app_msg({worker, FromNode}, BetMap, CL)}};
        false ->
            handle_bet(BetMap, FromNode, State)
    end;

handle_cast({bet, BetMap, FromNode}, State) ->
    handle_bet(BetMap, FromNode, State);

%% --- MARKER dal worker: chiude il canale entrante di quel nodo ---
handle_cast({cl_marker, SnapId, {worker, N}}, State = #state{cl = CL}) ->
    case cl_recorder:id(CL) =:= SnapId of
        false ->
            io:format("[WHEEL] Marker per un taglio non attivo (~p), ignorato~n", [SnapId]),
            {noreply, State};
        true ->
            CL1 = cl_recorder:close({worker, N}, CL),
            case cl_recorder:is_complete(CL1) of
                true  -> {noreply, close_cut(SnapId, CL1, false, State)};
                false -> {noreply, State#state{cl = CL1}}
            end
    end;

handle_cast({force_segment, <<"NONE">>}, State) ->
    io:format("[WHEEL] Annullamento forzatura segmento (esito casuale)~n"),
    {noreply, State#state{forced_segment = undefined}};
handle_cast({force_segment, Seg}, State) ->
    io:format("[WHEEL] Forzando segmento per il prossimo giro: ~s~n", [Seg]),
    {noreply, State#state{forced_segment = Seg}};
    
%% maps:get/3 con default: una bet malformata non deve far crashare il processo.
%% Il rimborso viene pubblicato da qui e non dal worker: con undo_bets
%% diventata un cast, il worker non riceve piu' il totale annullato e senza
%% questo l'annullamento smetterebbe di restituire i soldi.
handle_cast({undo_bets, Username}, State = #state{active = true, phase = betting, bets = Bets}) ->
    UserBets  = lists:filter(fun(B) -> maps:get(<<"username">>, B, <<"">>) == Username end, Bets),
    OtherBets = lists:filter(fun(B) -> maps:get(<<"username">>, B, <<"">>) =/= Username end, Bets),
    TotalRefund = lists:foldl(fun(B, Acc) -> Acc + maps:get(<<"amount">>, B, 0.0) end, 0.0, UserBets),
    case TotalRefund > 0 of
        true ->
            io:format("[WHEEL] Scommesse annullate per ~s: totale ~p~n", [Username, TotalRefund]),
            %% Un evento per ogni bet_id annullato: il rimborso aggregato per
            %% importo non permetteva di sapere QUALE puntata veniva chiusa.
            lists:foreach(fun(B) ->
                publish_bet_rejected(bet_id(B), State#state.round, <<"undo">>)
            end, UserBets);
        false ->
            io:format("[WHEEL] Nessuna scommessa da annullare per ~s~n", [Username])
    end,
    {noreply, State#state{bets = OtherBets}};
handle_cast({undo_bets, Username}, State) ->
    io:format("[WHEEL] UNDO ignorato per ~s (fase: ~p, active: ~p)~n",
              [Username, State#state.phase, State#state.active]),
    {noreply, State};

handle_cast({minigame_choice, Username, Choice}, State) ->
    NewChoices = maps:put(Username, Choice, State#state.minigame_choices),
    {noreply, State#state{minigame_choices = NewChoices}};

handle_cast(activate, State = #state{active = false}) ->
    io:format("[WHEEL] ACTIVATO come leader — avvio game loop~n"),
    erlang:send_after(1000, self(), tick),
    publish_timer(?BET_DURATION, State#state.round, State#state.history),
    %% Regola R3: il nuovo leader ricarica dai checkpoint le bet gia'
    %% liquidate, altrimenti le riconsegne che arrivano subito dopo un
    %% crash verrebbero rigiocate.
    {noreply, State#state{active = true, phase = betting, time_left = ?BET_DURATION,
                          settled_bet_ids = reload_settled()}};
handle_cast(activate, State = #state{active = true}) ->
    {noreply, State};  %% Gia' attivo

handle_cast(deactivate, State) ->
    io:format("[WHEEL] DISATTIVATO — in standby~n"),
    {noreply, State#state{active = false}};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% --- TICK (ignora se in standby) ---
handle_info(tick, State = #state{active = false}) ->
    {noreply, State};

%% --- TICK durante BETTING (countdown > 0) ---
handle_info(tick, State = #state{active = true, phase = betting, time_left = T}) when T > 1 ->
    NewTime = T - 1,
    publish_timer(NewTime, State#state.round, State#state.history),
    erlang:send_after(1000, self(), tick),
    {noreply, State#state{time_left = NewTime}};

%% --- TICK durante BETTING (countdown = 1 → spin!) ---
handle_info(tick, State = #state{active = true, phase = betting, time_left = 1}) ->
    io:format("~n--- ROUND #~p: NO MORE BETS! SPINNING... ---~n", [State#state.round]),
    publish_timer(0, State#state.round, State#state.history),

    Segments = wheel_segments(),
    
    {WinnerIndex, WinnerSeg} = case State#state.forced_segment of
        undefined ->
            Idx = rand:uniform(54) - 1,
            {Idx, lists:nth(Idx + 1, Segments)};
        ForcedSeg ->
            case find_segment_index(ForcedSeg, Segments, 0) of
                undefined ->
                    io:format("[WHEEL] WARNING: Forced segment '~s' not found, using random~n", [ForcedSeg]),
                    Idx = rand:uniform(54) - 1,
                    {Idx, lists:nth(Idx + 1, Segments)};
                Idx ->
                    {Idx, ForcedSeg}
            end
    end,

    io:format("[WHEEL] La ruota si ferma su: ~s (indice ~p)~n", [WinnerSeg, WinnerIndex]),

    %% === TAGLIO CHANDY-LAMPORT ===
    %% Si avvia QUI, dopo l'estrazione: cosi' il taglio cattura insieme le
    %% puntate e l'esito, e un nuovo leader eletto dopo un crash puo'
    %% completare il round invece di annullarlo.
    StateCut = start_cut(WinnerSeg, WinnerIndex, State),
    
    %% Esito e timer finiscono nello stato: senza, esistono solo dentro il
    %% messaggio send_after in volo e un nuovo leader non potrebbe recuperarli.
    State1 = StateCut#state{forced_segment = undefined,
                         winner_segment = WinnerSeg,
                         winner_index = WinnerIndex,
                         minigame_mod = undefined,
                         minigame_details = undefined},

    %% Determina se è un moltiplicatore diretto o un minigioco
    case segment_type(WinnerSeg) of
        {multiplier, Value} ->
            %% Pubblica spinning state con winner_index per l'animazione
            publish_spinning(State1#state.round, WinnerIndex, WinnerSeg, State1#state.history),
            %% Dopo 10.5s (tempo per l'animazione), risolvi il round
            TRef = erlang:send_after(10500, self(), {resolve_multiplier, WinnerSeg, Value, WinnerIndex}),
            {noreply, State1#state{phase = spinning, time_left = 0, timer_ref = TRef}};
        {minigame, Module} ->
            io:format("[WHEEL] BONUS! Entriamo in fase minigame: ~p~n", [Module]),
            %% Pubblica spinning state con winner_index
            publish_spinning(State1#state.round, WinnerIndex, WinnerSeg, State1#state.history),
            %% Dopo 10.5 secondi (tempo per l'animazione della ruota nel frontend), avvia il minigioco
            TRef = erlang:send_after(10500, self(), {start_minigame, WinnerSeg, Module, WinnerIndex}),
            {noreply, State1#state{phase = spinning, time_left = 0,
                                   minigame_mod = Module, timer_ref = TRef}}
    end;

%% --- ABORT del taglio: il collector non ha risposto ---
handle_info({cl_abort, SnapId}, State = #state{cl = CL}) ->
    case cl_recorder:id(CL) =:= SnapId andalso cl_recorder:is_recording(CL) of
        true ->
            io:format("[WHEEL] Taglio ~p non chiuso entro il timeout: chiusura forzata~n", [SnapId]),
            {noreply, close_cut(SnapId, cl_recorder:close_all(CL), true, State)};
        false ->
            {noreply, State}
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

    %% Timeout esplicito: un minigioco bloccato non deve impallare wheel_process a tempo indefinito
    case gen_server:call(Module, {play, BonusBets}, 10000) of
        {async_minigame, Details} ->
            WaitTimeAsync = case Module of 
                crazytime -> 16000; 
                cashhunt -> 30000; 
                _ -> 16000 
            end,
            TimeLeftSec = WaitTimeAsync div 1000,
            io:format("[WHEEL] Mini-game ~p in corso (attesa scelte utente per ~ps)...~n", [Module, TimeLeftSec]),
            publish_minigame_start(State#state.round, SegName, WinnerIndex, Details, State#state.history, TimeLeftSec),
            %% Schedula la risoluzione vera e propria
            TRef = erlang:send_after(WaitTimeAsync, self(), {resolve_async_minigame, SegName, Details, BonusBets, WinnerIndex}),
            {noreply, State#state{phase = minigame, time_left = TimeLeftSec,
                                  minigame_details = Details, timer_ref = TRef}};
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

%% Risolve i minigiochi asincroni (CrazyTime, CashHunt) dopo il tempo di attesa per le scelte
handle_info({resolve_async_minigame, SegName, Details, BonusBets, WinnerIndex}, State) ->
    io:format("[WHEEL] Risoluzione async minigame ~p! Calcolo vincite...~n", [SegName]),
    
    Payouts = case SegName of
        <<"CrazyTime">> -> crazytime:compute_payouts(Details, BonusBets, State#state.minigame_choices);
        <<"CashHunt">> -> cashhunt:compute_payouts(Details, BonusBets, State#state.minigame_choices);
        _ -> []
    end,
    
    io:format("[DEBUG] resolve_async_minigame - SegName: ~p, BonusBets length: ~p, Payouts length: ~p~n", [SegName, length(BonusBets), length(Payouts)]),
    
    %% -1 come multiplier: segnala al gateway Java di usare l'array payouts invece del campo multiplier
    Payload = build_result_json(SegName, <<"async_minigame">>, -1, WinnerIndex, Details, Payouts, State#state.round),
    publish_to_queue("results_queue", Payload),
    
    %% Add to history
    HistoryMult = case SegName of
        <<"CrazyTime">> -> maps:get(blue_multiplier, Details, 0);
        <<"CashHunt">> -> 
            Grid = maps:get(grid, Details, []),
            DefaultCell = maps:get(default_cell, Details, 0),
            if length(Grid) > DefaultCell -> lists:nth(DefaultCell + 1, Grid); true -> 0 end;
        _ -> 0
    end,
    NewHistory = lists:sublist([{SegName, HistoryMult} | State#state.history], 21),
    
    erlang:send_after(?COOLDOWN, self(), new_round),
    {noreply, State#state{phase = cooldown, time_left = 0, history = NewHistory}};

%% (Non c'è più bisogno di finish_minigame perché lo facciamo sincrono)

%% --- NEW ROUND ---
handle_info(new_round, State = #state{active = false}) ->
    {noreply, State};
handle_info(new_round, State) ->
    NewRound = State#state.round + 1,
    io:format("~n========================================~n"),
    io:format("  NUOVO ROUND #~p — BETTING APERTO~n", [NewRound]),
    io:format("========================================~n~n"),
    publish_timer(?BET_DURATION, NewRound, State#state.history),
    erlang:send_after(1000, self(), tick),
    {noreply, State#state{phase = betting, time_left = ?BET_DURATION, round = NewRound,
                          bets = [], minigame_choices = #{},
                          winner_segment = undefined, winner_index = undefined,
                          minigame_mod = undefined, minigame_details = undefined,
                          timer_ref = undefined}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal Functions
%%====================================================================

%% undefined su un miss, cosicché un segmento forzato inesistente non venga confuso con l'indice 0
find_segment_index(Target, [Target|_], Idx) -> Idx;
find_segment_index(Target, [_|T], Idx) -> find_segment_index(Target, T, Idx+1);
find_segment_index(_, [], _) -> undefined.

%% Clausola catch-all: un segmento sconosciuto non deve far crashare il gen_server
segment_type(<<"1">>)         -> {multiplier, 1};
segment_type(<<"2">>)         -> {multiplier, 2};
segment_type(<<"5">>)         -> {multiplier, 5};
segment_type(<<"10">>)        -> {multiplier, 10};
segment_type(<<"Pachinko">>)  -> {minigame, pachinko};
segment_type(<<"CoinFlip">>)  -> {minigame, coinflip};
segment_type(<<"CashHunt">>)  -> {minigame, cashhunt};
segment_type(<<"CrazyTime">>) -> {minigame, crazytime};
segment_type(Unknown) ->
    io:format("[WHEEL] WARNING: Unknown segment '~p', defaulting to 1x~n", [Unknown]),
    {multiplier, 1}.

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
                {true, #{username => Username, bet => Amount, payout => Payout,
                         bet_id => maps:get(<<"bet_id">>, Bet, <<"">>)}};
            false ->
                false
        end
    end, [B || B <- Bets, is_map(B)]).

%% Costruisce il JSON del risultato con winner_index e dettagli minigioco
build_result_json(Winner, Type, Multiplier, WinnerIndex, Details, Payouts, Round) ->
    PayoutsJson = lists:map(fun(P) ->
        io_lib:format("{\"username\":\"~s\",\"bet_id\":\"~s\",\"bet\":~p,\"payout\":~p}",
                      [escape_json_string(maps:get(username, P)),
                       maps:get(bet_id, P, <<"">>),
                       maps:get(bet, P), maps:get(payout, P)])
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
%% Distingue una charlist (stringa) da una lista vera, altrimenti finirebbe serializzata come array di interi
json_value(L) when is_list(L) ->
    case io_lib:printable_unicode_list(L) of
        true ->
            %% It's a string — serialize as JSON string
            "\"" ++ L ++ "\"";
        false ->
            %% It's a proper list — serialize as JSON array
            Items = lists:map(fun(I) -> json_value(I) end, L),
            "[" ++ string:join(Items, ",") ++ "]"
    end;
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

%% Il tempo di attesa varia per minigioco (CrazyTime, CashHunt, ...), va passato esplicitamente
publish_minigame_start(Round, MinigameName, WinnerIndex, Details, History, TimeLeftSec) ->
    HistStr = format_history(History),
    DetailsJSON = json_value(Details),
    Payload = lists:flatten(io_lib:format(
        "{\"type\":\"timer\",\"round\":~p,\"time_left\":~p,\"phase\":\"minigame\",\"minigame\":\"~s\",\"winner_index\":~p,\"details\":~s,\"history\":~s}",
        [Round, TimeLeftSec, MinigameName, WinnerIndex, DetailsJSON, HistStr])),
    publish_to_queue("state_queue", Payload).

%%====================================================================
%% Taglio Chandy-Lamport (lato wheel: e' l'iniziatore)
%%====================================================================

%% Avvia il taglio: congela i partecipanti, salva lo stato locale ed emette
%% i marker verso tutti i worker. I marker partono da QUESTO processo, non
%% dal collector: devono condividere mailbox e ordine FIFO con i {bet, ...}.
start_cut(WinnerSeg, WinnerIndex, State) ->
    Participants = participants(),
    SnapId = {State#state.round, node()},
    snapshot:begin_snapshot(SnapId, Participants),
    Local = #{round => State#state.round,
              bets => State#state.bets,
              phase => spinning,
              winner_segment => WinnerSeg,
              winner_index => WinnerIndex},
    InCh = [{worker, N} || N <- Participants],
    CL = cl_recorder:start(SnapId, Local, InCh),
    lists:foreach(fun(N) ->
        gen_server:cast({worker, N}, {cl_marker, SnapId, {wheel, node()}})
    end, Participants),
    erlang:send_after(?CL_ABORT_TIMEOUT, self(), {cl_abort, SnapId}),
    io:format("[WHEEL] Taglio ~p avviato verso ~p~n", [SnapId, Participants]),
    State#state{cl = CL}.

%% Chiusura del taglio: le bet in transito ENTRANO nel round (sono state
%% spedite prima che il worker apprendesse del taglio, quindi per il taglio
%% causale appartengono a questo round), i mittenti ricevono l'esito, e il
%% collector riceve la porzione di questo partecipante.
close_cut(SnapId, CL, Degraded, State) ->
    Channels = cl_recorder:channels(CL),
    InTransit = lists:append(maps:values(Channels)),
    lists:foreach(fun({{worker, N}, Msgs}) ->
        [reply_bet_result(N, bet_id(B), accepted) || B <- Msgs]
    end, maps:to_list(Channels)),
    gen_server:cast({snapshot, node()},
                    {cl_part, SnapId, {wheel, node()}, cl_recorder:local(CL), Channels}),
    case InTransit of
        [] -> ok;
        _  -> io:format("[WHEEL] Taglio ~p chiuso~s: ~p bet in transito entrano nel round~n",
                        [SnapId, case Degraded of true -> " (degradato)"; false -> "" end,
                         length(InTransit)])
    end,
    Bets = InTransit ++ State#state.bets,
    %% Regola R3: da qui in avanti queste bet non rientrano piu' in gioco,
    %% nemmeno se il broker le riconsegna nei round successivi.
    Settled = add_settled(Bets, State#state.settled_bet_ids),
    State#state{cl = cl_recorder:new(), bets = Bets, settled_bet_ids = Settled}.

participants() ->
    try cluster_manager:get_participants() of
        L when is_list(L) -> L
    catch
        _:_ -> [node()]
    end.

%%====================================================================
%% Deduplica (regola R3)
%%====================================================================

%% Ricarica dai checkpoint su Mnesia i bet_id degli ultimi round. Va fatto
%% all'attivazione come leader: e' subito dopo un crash che le riconsegne
%% del broker arrivano, quindi un nuovo leader con l'insieme vuoto e'
%% esattamente il caso in cui la regola serve di piu'.
reload_settled() ->
    Ids = lists:foldl(fun(Rec, Acc) ->
              [bet_id(B) || B <- Rec#snapshot_record.ledger] ++ Acc
          end, [], last_records(?SETTLED_ROUNDS)),
    Set = sets:from_list([I || I <- Ids, I =/= undefined]),
    case sets:size(Set) of
        0 -> ok;
        N -> io:format("[WHEEL] Deduplica: ricaricati ~p bet_id dai checkpoint~n", [N])
    end,
    Set.

last_records(N) -> last_records(N, mnesia_last(), []).

last_records(0, _Key, Acc) -> Acc;
last_records(_N, '$end_of_table', Acc) -> Acc;
last_records(N, Key, Acc) ->
    Recs = try mnesia:dirty_read(snapshot_record, Key) catch _:_ -> [] end,
    Prev = try mnesia:dirty_prev(snapshot_record, Key) catch _:_ -> '$end_of_table' end,
    last_records(N - 1, Prev, Recs ++ Acc).

mnesia_last() ->
    try mnesia:dirty_last(snapshot_record) catch _:_ -> '$end_of_table' end.

add_settled(Bets, Set) ->
    lists:foldl(fun(B, Acc) ->
        case bet_id(B) of
            undefined -> Acc;
            Id -> sets:add_element(Id, Acc)
        end
    end, Set, Bets).

fmt_id(undefined) -> "senza id";
fmt_id(Id) when is_binary(Id) -> binary_to_list(Id);
fmt_id(Id) -> lists:flatten(io_lib:format("~p", [Id])).

%% Identificativo della scommessa: undefined per i messaggi pubblicati a mano.
bet_id(BetMap) when is_map(BetMap) -> maps:get(<<"bet_id">>, BetMap, undefined);
bet_id(_) -> undefined.

%% Deduplica: una riconsegna del broker non deve far giocare due volte la
%% stessa puntata. Senza bet_id non si puo' decidere, quindi si accetta.
%%
%% Due termini, non uno: `bets` viene azzerato a ogni round e da solo copre
%% solo la riconsegna intra-round; `settled_bet_ids` copre quella che arriva
%% nei round successivi, che e' il caso frequente perche' nasce da un crash.
is_duplicate(undefined, _Bets, _Settled) -> false;
is_duplicate(BetId, Bets, Settled) ->
    sets:is_element(BetId, Settled) orelse
    lists:any(fun(B) -> bet_id(B) =:= BetId end, Bets).

%% Esito di una scommessa fuori dal taglio.
handle_bet(BetMap, FromNode, State = #state{active = false}) ->
    io:format("[WHEEL] Scommessa RIFIUTATA — non sono il leader~n"),
    reply_bet_result(FromNode, bet_id(BetMap), not_leader),
    {noreply, State};
handle_bet(BetMap, FromNode, State = #state{phase = betting, bets = Bets}) ->
    BetId = bet_id(BetMap),
    case is_duplicate(BetId, Bets, State#state.settled_bet_ids) of
        true ->
            %% Riconsegna del broker di una scommessa gia' accettata (l'ack
            %% precedente si e' perso). Va riackata, non rigiocata.
            io:format("[WHEEL] Bet ~s gia' liquidata: deduplicata~n", [fmt_id(BetId)]),
            reply_bet_result(FromNode, BetId, accepted),
            {noreply, State};
        false ->
            io:format("[WHEEL] Scommessa accettata: ~p~n", [BetMap]),
            reply_bet_result(FromNode, BetId, accepted),
            {noreply, State#state{bets = [BetMap | Bets]}}
    end;
handle_bet(BetMap, FromNode, State) ->
    %% Puntate chiuse: la scommessa e' arrivata tardi. Va rimborsata subito e
    %% in modo puntuale, altrimenti resterebbe PENDING con il saldo scalato.
    BetId = bet_id(BetMap),
    case is_duplicate(BetId, State#state.bets, State#state.settled_bet_ids) of
        true ->
            io:format("[WHEEL] Bet ~s gia' liquidata (fuori fase): deduplicata~n", [fmt_id(BetId)]),
            reply_bet_result(FromNode, BetId, accepted),
            {noreply, State};
        false ->
            io:format("[WHEEL] Scommessa RIFIUTATA — fase: ~p~n", [State#state.phase]),
            publish_bet_rejected(BetId, State#state.round, <<"betting_closed">>),
            reply_bet_result(FromNode, BetId, rejected),
            {noreply, State}
    end.

%% Esito verso il worker che ha consumato il messaggio dal broker.
reply_bet_result(FromNode, BetId, Verdict) ->
    gen_server:cast({worker, FromNode}, {bet_result, BetId, Verdict}).

%% Rifiuto puntuale di una singola scommessa, su results_queue.
%% Sostituisce il rimborso per importo su refunds_queue: due puntate di pari
%% importo su segmenti diversi erano indistinguibili lato gateway.
publish_bet_rejected(undefined, Round, Reason) ->
    io:format("[WHEEL] Bet senza bet_id rifiutata nel round ~p (~s): "
              "nessun rimborso pubblicabile~n", [Round, Reason]);
publish_bet_rejected(BetId, Round, Reason) ->
    Payload = lists:flatten(io_lib:format(
        "{\"type\":\"bet_rejected\",\"bet_id\":\"~s\",\"round\":~p,\"reason\":\"~s\"}",
        [escape_json_string(BetId), Round, Reason])),
    publish_to_queue("results_queue", Payload),
    io:format("[WHEEL] bet_rejected pubblicato per ~s (~s)~n", [BetId, Reason]).

%% Escaping minimale per gli username dentro il JSON.
escape_json_string(Str) when is_binary(Str) ->
    escape_json_string(binary_to_list(Str));
escape_json_string(Str) ->
    lists:flatmap(fun($") -> "\\\""; ($\\) -> "\\\\"; (C) -> [C] end, Str).

%% Pubblica un messaggio su una coda RabbitMQ via AMQP.
%% unicode:characters_to_binary/1 (e non list_to_binary/1) perche' i payload
%% contengono username arbitrari: un accento e' un codepoint > 255.
publish_to_queue(QueueName, Payload) ->
    case rabbitmq_manager:publish(list_to_binary(QueueName),
                                  unicode:characters_to_binary(Payload)) of
        ok -> ok;
        {error, Reason} ->
            io:format("[WHEEL] Publish fallita su ~s: ~p~n", [QueueName, Reason])
    end.
