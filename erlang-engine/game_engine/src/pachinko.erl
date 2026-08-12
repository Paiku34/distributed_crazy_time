%%%-------------------------------------------------------------------
%%% @doc Pachinko mini-game process.
%%%      Simulates realistic Pachinko drops, DOUBLE mechanics, and paths.
%%%-------------------------------------------------------------------
-module(pachinko).
-behaviour(gen_server).

-export([start_link/0, play/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

play(Bets) ->
    gen_server:call(?MODULE, {play, Bets}).

init([]) ->
    {ok, #{}}.

handle_call({play, Bets}, _From, State) ->
    InitialSlots = generate_initial_slots(),
    Drops = simulate_drops(InitialSlots, []),
    
    %% L'ultimo drop contiene il risultato finale
    LastDrop = lists:last(Drops),
    WinnerIdx = maps:get(landed_index, LastDrop),
    FinalSlots = maps:get(slots, LastDrop),
    FinalMultiplier = lists:nth(WinnerIdx + 1, FinalSlots),

    Details = #{
        drops => Drops
    },

    io:format("[PACHINKO] Completati ~p lanci. Moltiplicatore finale: x~p (~p scommesse)~n",
              [length(Drops), FinalMultiplier, length(Bets)]),
    {reply, {ok, FinalMultiplier, Details}, State};

handle_call(_Req, _From, State) -> {reply, {error, unknown_request}, State}.
handle_cast(_Msg, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% Genera gli 8 slot iniziali, mescolando moltiplicatori normali e 1-2 DOUBLE
generate_initial_slots() ->
    Bases = [2, 3, 5, 7, 10, 15, 20, 25, 50, 100],
    %% Scegliamo 7 valori casuali e 1 DOUBLE (per garantire almeno un po' di probabilità)
    RandomBases = [lists:nth(rand:uniform(length(Bases)), Bases) || _ <- lists:seq(1, 7)],
    Slots = RandomBases ++ [<<"DOUBLE">>],
    %% Mischia gli slot
    shuffle(Slots).

shuffle(List) ->
    [X || {_,X} <- lists:sort([{rand:uniform(), N} || N <- List])].

%% Simula lanci finché non si atterra su un numero (non DOUBLE)
simulate_drops(Slots, AccDrops) ->
    %% Sceglie una zona di caduta da 0 a 15
    DZ = rand:uniform(16) - 1,
    {FinalPos, Path} = generate_path(DZ, 15, []),
    
    %% FinalPos è dispari (1,3,5,7,9,11,13,15) grazie alla matematica (15 passi da pari a pari o da dispari a dispari).
    %% Mappiamo FinalPos dispari all'indice dello slot 0..7
    SlotIdx = FinalPos div 2, 
    LandedVal = lists:nth(SlotIdx + 1, Slots),
    
    DropData = #{
        drop_zone => DZ,
        path => Path,
        slots => Slots,
        landed_index => SlotIdx,
        landed_value => LandedVal
    },
    
    case LandedVal of
        <<"DOUBLE">> ->
            NewSlots = double_slots(Slots),
            simulate_drops(NewSlots, AccDrops ++ [DropData]);
        _Num ->
            AccDrops ++ [DropData]
    end.

%% Raddoppia tutti i numeri, mantenendo i DOUBLE inalterati
double_slots(Slots) ->
    lists:map(fun
        (<<"DOUBLE">>) -> <<"DOUBLE">>;
        (Num) -> Num * 2
    end, Slots).

%% Genera un percorso di 15 passi (rimbalzi sui pioli)
generate_path(Pos, 0, Acc) ->
    {Pos, lists:reverse(Acc)};
generate_path(Pos, StepsLeft, Acc) ->
    Dir = if
        Pos =< 0 -> 1;       %% muro sinistro -> rimbalza a destra
        Pos >= 15 -> -1;     %% muro destro -> rimbalza a sinistra
        true -> 
            case rand:uniform(2) of
                1 -> -1;
                2 -> 1
            end
    end,
    generate_path(Pos + Dir, StepsLeft - 1, [Dir | Acc]).
