%%%-------------------------------------------------------------------
%% @doc Supervisor for the 4 mini-game processes.
%%      Matches the "MiniGames Supervisor" box in the PDF architecture.
%%      Strategy: one_for_one — if one mini-game crashes, only that
%%      one is restarted; the others keep running.
%% @end
%%%-------------------------------------------------------------------
-module(minigames_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 10
    },
    ChildSpecs = [
        #{id => pachinko,  start => {pachinko,  start_link, []}, restart => permanent, type => worker},
        #{id => coinflip,  start => {coinflip,  start_link, []}, restart => permanent, type => worker},
        #{id => cashhunt,  start => {cashhunt,  start_link, []}, restart => permanent, type => worker},
        #{id => crazytime, start => {crazytime, start_link, []}, restart => permanent, type => worker}
    ],
    {ok, {SupFlags, ChildSpecs}}.
