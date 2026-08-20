%%%-------------------------------------------------------------------
%% @doc game_engine top level supervisor.
%%      Matches the "Main Supervisor" box in the PDF architecture.
%%
%%      Two-level supervision tree:
%%        game_engine_sup (rest_for_one)
%%          ├── rabbitmq_manager (connessione AMQP — deve partire per primo)
%%          ├── wheel_process   (the main wheel gen_server)
%%          ├── minigames_sup   (child supervisor for 4 mini-games)
%%          └── worker          (consumer AMQP di bets_queue)
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
        %% rest_for_one: wheel_process e worker dipendono dal rabbitmq_manager,
        %% quindi se cade lui vanno riavviati anche loro (nell'ordine).
        strategy => rest_for_one,
        intensity => 5,
        period => 10
    },
    ChildSpecs = [
        %% 0. La connessione AMQP: primo a partire, ultimo a fermarsi
        #{id => rabbitmq_manager,
          start => {rabbitmq_manager, start_link, []},
          restart => permanent,
          type => worker},
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
