%%%-------------------------------------------------------------------
%% @doc Pachinko mini-game process.
%%      When the main wheel lands on "Pachinko", the wheel_process
%%      delegates to this gen_server. It simulates a Pachinko drop
%%      that determines a random multiplier for the payout.
%%      Returns {ok, Multiplier, Details} with animation data.
%% @end
%%%-------------------------------------------------------------------
-module(pachinko).
-behaviour(gen_server).

-export([start_link/0, play/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% API
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% play/1 receives a list of bets that were placed on Pachinko.
%% Returns {ok, Multiplier, Details} where Details contains animation info.
play(Bets) ->
    gen_server:call(?MODULE, {play, Bets}).

%% Callbacks
init([]) ->
    io:format("[PACHINKO] Mini-game process avviato.~n"),
    {ok, #{}}.

handle_call({play, Bets}, _From, State) ->
    %% Pachinko: la pallina cade attraverso i pioli e atterra su un moltiplicatore.
    %% Distribuzione pesata: i moltiplicatori alti sono meno probabili.
    Multipliers = [2, 3, 5, 8, 10, 15, 25, 50, 100],
    Weights     = [30, 25, 18, 10,  7,  5,  3,  1,   1],  %% su 100
    WinnerIdx = weighted_random_index(Weights),
    Multiplier = lists:nth(WinnerIdx, Multipliers),
    %% Genera un percorso casuale di rimbalzi (8 livelli, L=sinistra R=destra)
    Path = [case rand:uniform(2) of 1 -> <<"L">>; 2 -> <<"R">> end || _ <- lists:seq(1, 8)],
    Details = #{
        slot_index => WinnerIdx - 1,
        slots => Multipliers,
        path => Path
    },
    io:format("[PACHINKO] Pallina caduta! Moltiplicatore: x~p (~p scommesse)~n",
              [Multiplier, length(Bets)]),
    {reply, {ok, Multiplier, Details}, State};

handle_call(_Req, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal: weighted random selection returning 1-based index
weighted_random_index(Weights) ->
    Total = lists:sum(Weights),
    R = rand:uniform(Total),
    pick_index(Weights, R, 0, 1).

pick_index([W | _], R, Acc, Idx) when R =< Acc + W ->
    Idx;
pick_index([W | Ws], R, Acc, Idx) ->
    pick_index(Ws, R, Acc + W, Idx + 1).
