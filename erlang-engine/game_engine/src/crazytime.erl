%%%-------------------------------------------------------------------
%% @doc Crazy Time mini-game process.
%%      The ultimate bonus. A giant secondary wheel with very high
%%      multipliers is spun. This is the most rewarding (and rarest)
%%      mini-game in the system.
%%      Returns {ok, Multiplier, Details} with boost info for animation.
%% @end
%%%-------------------------------------------------------------------
-module(crazytime).
-behaviour(gen_server).

-export([start_link/0, play/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

play(Bets) ->
    gen_server:call(?MODULE, {play, Bets}).

init([]) ->
    io:format("[CRAZYTIME] Mini-game process avviato.~n"),
    {ok, #{}}.

handle_call({play, Bets}, _From, State) ->
    %% La ruota secondaria del Crazy Time ha moltiplicatori molto alti
    Segments = [5, 10, 15, 20, 25, 50, 100, 200],
    Weights  = [25, 20, 18, 15, 10,  7,   4,   1],  %% su 100
    WinnerIdx = weighted_random_index(Weights),
    BaseMultiplier = lists:nth(WinnerIdx, Segments),
    %% Possibilità di un "Double" o "Triple" che raddoppia/triplica
    {FinalMultiplier, Boost} = maybe_boost(BaseMultiplier),
    Details = #{
        base_multiplier => BaseMultiplier,
        boost => Boost,
        segments => Segments,
        winner_index => WinnerIdx - 1
    },
    io:format("[CRAZYTIME] Ruota secondaria! Base=x~p, Boost=~s, Finale=x~p (~p scommesse)~n",
              [BaseMultiplier, Boost, FinalMultiplier, length(Bets)]),
    {reply, {ok, FinalMultiplier, Details}, State};

handle_call(_Req, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% Internal
weighted_random_index(Weights) ->
    Total = lists:sum(Weights),
    R = rand:uniform(Total),
    pick_index(Weights, R, 0, 1).

pick_index([W | _], R, Acc, Idx) when R =< Acc + W ->
    Idx;
pick_index([W | Ws], R, Acc, Idx) ->
    pick_index(Ws, R, Acc + W, Idx + 1).

%% 20% di probabilità di Double (x2), 5% di Triple (x3)
maybe_boost(Mult) ->
    Roll = rand:uniform(100),
    if
        Roll =< 5  -> {Mult * 3, <<"triple">>};
        Roll =< 25 -> {Mult * 2, <<"double">>};
        true        -> {Mult, <<"none">>}
    end.
