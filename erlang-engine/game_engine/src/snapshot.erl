%%%-------------------------------------------------------------------
%% @doc Collector dello snapshot Chandy-Lamport.
%%
%%      NON e' un partecipante al taglio: i partecipanti sono il
%%      wheel_process e gli N worker, che si scambiano i marker sui
%%      canali applicativi reali. Questo processo si limita a:
%%        - registrare l'avvio di un taglio e congelare i partecipanti;
%%        - armare il timeout;
%%        - raccogliere le porzioni che i partecipanti gli spediscono;
%%        - comporre il record, persisterlo su Mnesia (replicato) e
%%          pubblicare il ledger autorevole del round.
%%
%%      Non interroga mai i partecipanti: sono loro a fare push. Una
%%      chiamata sincrona verso il wheel andrebbe in timeout ogni volta
%%      che quest'ultimo e' bloccato nella call al minigioco.
%%
%%      E' l'ULTIMO figlio del supervisore: con rest_for_one un suo
%%      crash non deve azzerare il round in corso.
%% @end
%%%-------------------------------------------------------------------
-module(snapshot).
-behaviour(gen_server).

-include("game_engine.hrl").

-export([start_link/0, begin_snapshot/2, get_last/0, get_for_round/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% Budget del taglio. Il round viene risolto molto piu' tardi (10,5 s per
%% l'animazione della ruota), quindi il costo in latenza e' nullo.
-define(SNAPSHOT_TIMEOUT, 5000).

%% Un taglio in corso.
-record(run, {
    round,
    initiator,
    expected = [] :: [term()],          %% partecipanti attesi, congelati all'avvio
    parts    = #{} :: #{term() => {term(), map()}},  %% Who => {Local, Channels}
    timer,
    degraded = false :: boolean()
}).

-record(state, {
    running = #{} :: #{term() => #run{}}   %% SnapId => run
}).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Avvio di un taglio. E' un CAST: lo SnapId arriva gia' calcolato dal wheel,
%% che ne ha bisogno subito per marcare i propri marker uscenti.
-spec begin_snapshot(term(), [node()]) -> ok.
begin_snapshot(SnapId, Participants) ->
    gen_server:cast(?MODULE, {begin_snapshot, SnapId, Participants}).

%% Ultimo checkpoint persistito (chiave ordered_set = {Round, Initiator}).
-spec get_last() -> {ok, #snapshot_record{}} | none.
get_last() ->
    try mnesia:dirty_last(snapshot_record) of
        '$end_of_table' -> none;
        Key ->
            case mnesia:dirty_read(snapshot_record, Key) of
                [Rec] -> {ok, Rec};
                _     -> none
            end
    catch
        _:_ -> none
    end.

%% Checkpoint di un round specifico (puo' essercene piu' di uno solo in caso
%% di split-brain: la chiave include l'iniziatore proprio per renderlo visibile).
-spec get_for_round(integer()) -> [#snapshot_record{}].
get_for_round(Round) ->
    try mnesia:dirty_match_object(#snapshot_record{round = Round, _ = '_'})
    catch _:_ -> []
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    io:format("~n=================================~n"),
    io:format("  Snapshot collector avviato~n"),
    io:format("=================================~n~n"),
    {ok, #state{}}.

handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast({begin_snapshot, SnapId, Participants}, State) ->
    {Round, Initiator} = SnapId,
    Expected = [{wheel, Initiator} | [{worker, N} || N <- Participants]],
    io:format("[SNAPSHOT ~p] Avviato. Partecipanti attesi: ~p~n", [SnapId, Expected]),
    TRef = erlang:send_after(?SNAPSHOT_TIMEOUT, self(), {snapshot_timeout, SnapId}),
    Run = #run{round = Round, initiator = Initiator, expected = Expected, timer = TRef},
    {noreply, State#state{running = maps:put(SnapId, Run, State#state.running)}};

handle_cast({cl_part, SnapId, Who, Local, Channels}, State) ->
    case maps:find(SnapId, State#state.running) of
        error ->
            %% Porzione arrivata dopo la chiusura (o taglio mai avviato qui).
            io:format("[SNAPSHOT ~p] Porzione tardiva da ~p, ignorata~n", [SnapId, Who]),
            {noreply, State};
        {ok, Run} ->
            Parts = maps:put(Who, {Local, Channels}, Run#run.parts),
            Run1 = Run#run{parts = Parts},
            Missing = Run1#run.expected -- maps:keys(Parts),
            case Missing of
                [] ->
                    {noreply, finalize(SnapId, Run1, State)};
                _ ->
                    {noreply, State#state{running = maps:put(SnapId, Run1, State#state.running)}}
            end
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({snapshot_timeout, SnapId}, State) ->
    case maps:find(SnapId, State#state.running) of
        error ->
            {noreply, State};
        {ok, Run} ->
            Missing = Run#run.expected -- maps:keys(Run#run.parts),
            io:format("[SNAPSHOT ~p] TIMEOUT: chiusura in modalita' degradata, "
                      "partecipanti mancanti: ~p~n", [SnapId, Missing]),
            {noreply, finalize(SnapId, Run#run{degraded = true}, State)}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%====================================================================
%% Internal
%%====================================================================

finalize(SnapId, Run, State) ->
    cancel_timer(Run#run.timer),
    {Round, Initiator} = SnapId,
    Parts = Run#run.parts,

    %% La porzione del wheel contiene le bet gia' accettate al taglio; gli
    %% stati dei canali contengono quelle in transito, spedite prima che il
    %% worker apprendesse del taglio. Per il taglio causale appartengono
    %% tutte a questo round.
    {WheelLocal, WheelChan} = maps:get({wheel, Initiator}, Parts, {#{}, #{}}),
    LocalBets = maps:get(bets, WheelLocal, []),
    InFlight = lists:append(maps:values(WheelChan)),
    Ledger = LocalBets ++ InFlight,

    Rec = #snapshot_record{
        id = SnapId,
        round = Round,
        taken_at = erlang:system_time(millisecond),
        initiator = Initiator,
        degraded = Run#run.degraded,
        phase = maps:get(phase, WheelLocal, undefined),
        winner_segment = maps:get(winner_segment, WheelLocal, undefined),
        winner_index = maps:get(winner_index, WheelLocal, undefined),
        local_states = maps:map(fun(_K, {L, _C}) -> L end, Parts),
        channel_states = maps:fold(fun(Who, {_L, C}, Acc) ->
                                       maps:fold(fun(From, Msgs, A) ->
                                                     maps:put({From, Who}, Msgs, A)
                                                 end, Acc, C)
                                   end, #{}, Parts),
        ledger = Ledger
    },

    persist(Rec),
    io:format("[SNAPSHOT ~p] COMPLETO. local_bets=~p in_flight_bets=~p degraded=~p~n",
              [SnapId, length(LocalBets), length(InFlight), Run#run.degraded]),

    %% Il ledger lo pubblica solo il leader: e' l'unico che ha il round
    %% autoritativo e l'unico che deve parlare col gateway.
    case Initiator =:= node() of
        true  -> publish_ledger(Round, Run#run.degraded, Ledger);
        false -> ok
    end,
    State#state{running = maps:remove(SnapId, State#state.running)}.

persist(Rec) ->
    case mnesia:transaction(fun() -> mnesia:write(Rec) end) of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
            io:format("[SNAPSHOT] Persistenza su Mnesia fallita: ~p~n", [Reason])
    end.

%% Ledger autorevole del round verso il gateway: l'elenco delle bet che
%% fanno parte di questo round, per bet_id. Java ci chiude sopra le
%% pendenti rimaste fuori.
publish_ledger(Round, Degraded, Ledger) ->
    Ids = [Id || B <- Ledger,
                 (Id = maps:get(<<"bet_id">>, B, undefined)) =/= undefined],
    IdsJson = "[" ++ string:join(["\"" ++ binary_to_list(I) ++ "\"" || I <- Ids], ",") ++ "]",
    Payload = lists:flatten(io_lib:format(
        "{\"type\":\"round_ledger\",\"round\":~p,\"degraded\":~s,\"bet_ids\":~s}",
        [Round, atom_to_list(Degraded), IdsJson])),
    case rabbitmq_manager:publish(<<"results_queue">>, unicode:characters_to_binary(Payload)) of
        ok ->
            io:format("[SNAPSHOT] Ledger del round ~p pubblicato (~p bet)~n", [Round, length(Ids)]);
        {error, Reason} ->
            io:format("[SNAPSHOT] Pubblicazione del ledger fallita: ~p~n", [Reason])
    end.

cancel_timer(undefined) -> ok;
cancel_timer(TRef) -> erlang:cancel_timer(TRef), ok.
