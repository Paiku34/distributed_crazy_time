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

-export([start_link/0, play/1, compute_payouts/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

play(Bets) ->
    gen_server:call(?MODULE, {play, Bets}).

init([]) ->
    io:format("[CRAZYTIME] Mini-game process avviato.~n"),
    {ok, #{}}.

handle_call({play, Bets}, _From, State) ->
    Segments = [200, 25, 50, 20, 25, 50, 15, 100, 50, 10, 25, 10, 50, 100, 20, 50, 15, 25, 50, 10, 25, <<"DOUBLE">>, 20, 10, 15, 50, 10, 25, 100, 50, 10, 25, 20, 15, 50, 100, 25, 50, 10, 25, 10, 25, 10, 50, 10, 25, 15, 25, 50, 10, 20, 50, 25, 100, 50, 15, 25, 50, 10, 25, 50, 10, 25, 15],
    Len = length(Segments),
    WinnerIdx = rand:uniform(Len),
    
    %% Helper function to resolve segment value
    ResolveVal = fun(Idx) ->
        %% Modulo circolare 1-indexed
        RealIdx = ((Idx - 1) rem Len + Len) rem Len + 1,
        Val = lists:nth(RealIdx, Segments),
        case Val of <<"DOUBLE">> -> 100; Num -> Num end
    end,

    NumSegments = Len,
    Spacing = NumSegments div 3,  %% ~21 segments apart
    BlueMult  = ResolveVal(WinnerIdx),
    GreenMult = ResolveVal(WinnerIdx + Spacing),
    YellowMult = ResolveVal(WinnerIdx + 2 * Spacing),

    Details = #{
        blue_multiplier => BlueMult,
        green_multiplier => GreenMult,
        yellow_multiplier => YellowMult,
        segments => Segments,
        winner_index => WinnerIdx - 1
    },
    io:format("[CRAZYTIME] Ruota secondaria! Blue=x~p, Green=x~p, Yellow=x~p~n",
              [BlueMult, GreenMult, YellowMult]),
    {reply, {async_minigame, Details}, State};

handle_call(_Req, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% compute_payouts/3
%% Called by wheel_process after the 16s wait
compute_payouts(Details, Bets, Choices) ->
    %% FORCE RECOMPILE
    io:format("[CRAZYTIME] Calcolo vincite per ~p scommesse...~n", [length(Bets)]),
    BlueMult = maps:get(blue_multiplier, Details),
    GreenMult = maps:get(green_multiplier, Details),
    YellowMult = maps:get(yellow_multiplier, Details),

    Result = lists:filtermap(fun(Bet) ->
        Seg = maps:get(<<"segment">>, Bet, <<>>),
        case Seg of
            <<"CrazyTime">> ->
                Username = maps:get(<<"username">>, Bet, <<"unknown">>),
                Amount = maps:get(<<"amount">>, Bet, 0),
                
                %% Determine which multiplier this user gets
                UserChoice = maps:get(Username, Choices, <<"blue">>),
                NormalizedChoice = string:lowercase(UserChoice),
                UserMult = case NormalizedChoice of
                    <<"green">> -> GreenMult;
                    <<"yellow">> -> YellowMult;
                    _ -> BlueMult
                end,

                {true, #{
                    username => Username,
                    bet => Amount,
                    payout => Amount + (Amount * UserMult)
                }};
            _ -> false
        end
    end, Bets).

