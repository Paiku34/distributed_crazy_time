%%%-------------------------------------------------------------------
%% @doc game_engine public API
%% @end
%%%-------------------------------------------------------------------

-module(game_engine_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    game_engine_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
