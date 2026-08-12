%%%-------------------------------------------------------------------
%% @doc Cash Hunt mini-game process.
%%      A grid of hidden multipliers is shuffled, one cell is randomly
%%      selected, revealing the payout for all players who bet on
%%      Cash Hunt.
%%      Returns {ok, Multiplier, Details} with grid and cell info.
%% @end
%%%-------------------------------------------------------------------
-module(cashhunt).
-behaviour(gen_server).

-export([start_link/0, play/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

play(Bets) ->
    gen_server:call(?MODULE, {play, Bets}).

init([]) ->
    io:format("[CASHHUNT] Mini-game process avviato.~n"),
    {ok, #{}}.

handle_call({play, Bets}, _From, State) ->
    %% Griglia 4x4 di moltiplicatori nascosti
    Grid = [5, 10, 15, 2, 3, 8, 20, 50, 25, 10, 5, 3, 2, 15, 100, 75],
    Shuffled = [X || {_, X} <- lists:sort([{rand:uniform(), V} || V <- Grid])],
    %% Il sistema seleziona una cella casuale (0-indexed per il frontend)
    CellIndex = rand:uniform(length(Shuffled)) - 1,
    Multiplier = lists:nth(CellIndex + 1, Shuffled),
    Details = #{
        grid => Shuffled,
        cell_index => CellIndex
    },
    io:format("[CASHHUNT] Griglia mescolata. Cella #~p selezionata -> Moltiplicatore: x~p (~p scommesse)~n",
              [CellIndex, Multiplier, length(Bets)]),
    {reply, {ok, Multiplier, Details}, State};

handle_call(_Req, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
