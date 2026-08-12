%%%-------------------------------------------------------------------
%% @doc Coin Flip mini-game process.
%%      A coin is flipped to choose between two sides, each with
%%      a different multiplier. Simple but dramatic.
%%      Returns {ok, Multiplier, Details} with side info for animation.
%% @end
%%%-------------------------------------------------------------------
-module(coinflip).
-behaviour(gen_server).

-export([start_link/0, play/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

play(Bets) ->
    gen_server:call(?MODULE, {play, Bets}).

init([]) ->
    io:format("[COINFLIP] Mini-game process avviato.~n"),
    {ok, #{}}.

handle_call({play, Bets}, _From, State) ->
    %% Due lati della moneta con moltiplicatori diversi
    {SideA, SideB} = generate_sides(),
    WinnerSide = case rand:uniform(2) of
        1 -> <<"heads">>;
        2 -> <<"tails">>
    end,
    Winner = case WinnerSide of
        <<"heads">> -> SideA;
        <<"tails">> -> SideB
    end,
    Details = #{
        side_a => SideA,
        side_b => SideB,
        winner_side => WinnerSide
    },
    io:format("[COINFLIP] Lancio moneta! Lato A=x~p, Lato B=x~p -> Vincitore: ~s x~p (~p scommesse)~n",
              [SideA, SideB, WinnerSide, Winner, length(Bets)]),
    {reply, {ok, Winner, Details}, State};

handle_call(_Req, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% Internal: genera due moltiplicatori casuali per le due facce
generate_sides() ->
    Choices = [2, 3, 5, 7, 10, 15, 20, 25, 50, 100],
    V1 = lists:nth(rand:uniform(length(Choices)), Choices),
    V2 = lists:nth(rand:uniform(length(Choices)), Choices),
    case V1 =:= V2 of
        true -> generate_sides();
        false -> {V1, V2}
    end.
