%%%-------------------------------------------------------------------
%% @doc game_engine top level supervisor.
%%      Matches the "Main Supervisor" box in the PDF architecture.
%%
%%      Two-level supervision tree:
%%        game_engine_sup (one_for_one)
%%          ├── wheel_process   (the main wheel gen_server)
%%          ├── minigames_sup   (child supervisor for 4 mini-games)
%%          └── worker          (RabbitMQ poller)
%% @end
%%%-------------------------------------------------------------------
-module(game_engine_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_one,   %% se un figlio crasha, riavvia solo quello
        intensity => 5,
        period => 10
    },
    ChildSpecs = [
        %% 1. Il processo della ruota principale
        #{id => wheel_process,
          start => {wheel_process, start_link, []},
          restart => permanent,
          type => worker},
        %% 2. Il supervisor dei mini-giochi (secondo livello di supervisione)
        #{id => minigames_sup,
          start => {minigames_sup, start_link, []},
          restart => permanent,
          type => supervisor},
        %% 3. Il worker che ascolta RabbitMQ
        #{id => worker,
          start => {worker, start_link, []},
          restart => permanent,
          type => worker}
    ],
    {ok, {SupFlags, ChildSpecs}}.
