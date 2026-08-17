%%%-------------------------------------------------------------------
%% @doc Cash Hunt mini-game process.
%%      A 9x12 grid of hidden multipliers. Players pick a cell
%%      to reveal their prize. Realistic multiplier distribution.
%%      Returns {ok, Multiplier, Details} with grid info.
%% @end
%%%-------------------------------------------------------------------
-module(cashhunt).
-behaviour(gen_server).

-export([start_link/0, play/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([compute_payouts/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

play(Bets) ->
    gen_server:call(?MODULE, {play, Bets}).

init([]) ->
    io:format("[CASHHUNT] Mini-game process avviato.~n"),
    {ok, #{}}.

handle_call({play, Bets}, _From, State) ->
    %% Griglia 9 colonne x 12 righe = 108 celle
    Grid = generate_grid(),
    %% Mescola la griglia iniziale (mostrata prima dello shuffle)
    InitialGrid = [X || {_, X} <- lists:sort([{rand:uniform(), V} || V <- Grid])],
    %% Mescola nuovamente la griglia (valori reali finali su cui il server paga)
    FinalGrid = [X || {_, X} <- lists:sort([{rand:uniform(), V} || V <- Grid])],
    %% Il server sceglie una cella di default (per chi non sceglie)
    DefaultCell = rand:uniform(108) - 1,
    
    Details = #{
        initial_grid => InitialGrid,
        grid => FinalGrid,
        cols => 9,
        rows => 12,
        default_cell => DefaultCell
    },
    io:format("[CASHHUNT] Griglia 9x12 generata. Cella default #~p. Attendiamo scelte utente... (~p scommesse)~n",
              [DefaultCell, length(Bets)]),
    {reply, {async_minigame, Details}, State};

handle_call(_Req, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% compute_payouts/3
%% Called by wheel_process after the wait time (19s)
compute_payouts(Details, Bets, Choices) ->
    Grid = maps:get(grid, Details),
    DefaultCell = maps:get(default_cell, Details),
    
    lists:filtermap(fun(Bet) ->
        Seg = maps:get(<<"segment">>, Bet, <<>>),
        case Seg of
            <<"CashHunt">> ->
                Username = maps:get(<<"username">>, Bet, <<"unknown">>),
                Amount = maps:get(<<"amount">>, Bet, 0),
                
                %% Determine which cell this user gets
                UserChoiceStr = maps:get(Username, Choices, undefined),
                CellIndex = case UserChoiceStr of
                    undefined -> DefaultCell;
                    ChoiceStr -> 
                        %% Parse string to integer
                        try binary_to_integer(ChoiceStr) of
                            Int when Int >= 0, Int < 108 -> Int;
                            _ -> DefaultCell
                        catch
                            _:_ -> DefaultCell
                        end
                end,
                
                %% Get the multiplier for that cell
                UserMult = lists:nth(CellIndex + 1, Grid),

                {true, #{
                    username => Username,
                    bet => Amount,
                    payout => Amount + (Amount * UserMult)
                }};
            _ -> false
        end
    end, Bets).

handle_cast(_Msg, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% Genera una griglia 108 celle con distribuzione realistica:
%%   ~60 celle da x2-x5 (comuni)
%%   ~25 celle da x7-x10 (medi)
%%   ~12 celle da x15-x20 (buoni)
%%   ~6 celle da x25-x50 (rari)
%%   ~3 celle da x75 (molto rari)
%%   ~1 cella da x100 (rarissima)
%%   ~1 cella da x200 (leggendaria, non sempre presente)
generate_grid() ->
    Common    = lists:flatten([lists:duplicate(20, 2), lists:duplicate(20, 3), lists:duplicate(20, 5)]),
    Medium    = lists:flatten([lists:duplicate(13, 7), lists:duplicate(12, 10)]),
    Good      = lists:flatten([lists:duplicate(6, 15), lists:duplicate(6, 20)]),
    Rare      = lists:flatten([lists:duplicate(3, 25), lists:duplicate(3, 50)]),
    VeryRare  = lists:duplicate(3, 75),
    Epic      = [100],
    Legendary = case rand:uniform(3) of 1 -> [200]; _ -> [100] end,
    All = Common ++ Medium ++ Good ++ Rare ++ VeryRare ++ Epic ++ Legendary,
    %% Prendi esattamente 108 celle
    Trimmed = lists:sublist(All, 108),
    Trimmed.
