%%%-------------------------------------------------------------------
%% @doc game_engine top level supervisor.
%%      Matches the "Main Supervisor" box in the PDF architecture.
%%
%%      Supervision tree (rest_for_one):
%%        game_engine_sup
%%          ├── rabbitmq_manager (connessione AMQP — deve partire per primo)
%%          ├── cluster_manager  (discovery nodi e monitoraggio cluster)
%%          ├── wheel_process    (the main wheel gen_server)
%%          ├── minigames_sup    (child supervisor for 4 mini-games)
%%          ├── worker           (consumer AMQP di bets_queue, su ogni nodo)
%%          └── snapshot         (collector Chandy-Lamport — ULTIMO)
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
        %% rest_for_one: i processi dipendono da quelli sopra —
        %% se cade rabbitmq_manager tutti ripartono; se cade
        %% cluster_manager ripartono wheel, minigames e worker.
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
        %% 1. Cluster manager: discovery dei nodi peer e monitoraggio topologia
        #{id => cluster_manager,
          start => {cluster_manager, start_link, []},
          restart => permanent,
          type => worker},
        %% 1.5. Leader election: elezione Bully Algorithm
        #{id => leader_election,
          start => {leader_election, start_link, []},
          restart => permanent,
          type => worker},
        %% 2. Il processo della ruota principale
        #{id => wheel_process,
          start => {wheel_process, start_link, []},
          restart => permanent,
          type => worker},
        %% 3. Il supervisor dei mini-giochi (secondo livello di supervisione)
        #{id => minigames_sup,
          start => {minigames_sup, start_link, []},
          restart => permanent,
          type => supervisor},
        %% 4. Il worker che ascolta RabbitMQ
        #{id => worker,
          start => {worker, start_link, []},
          restart => permanent,
          type => worker},
        %% 5. Il collector dello snapshot: ULTIMO figlio di proposito.
        %% Con rest_for_one un crash riavvia tutti i figli sotto di se':
        %% mettendolo in coda, un suo crash non azzera il round in corso.
        #{id => snapshot,
          start => {snapshot, start_link, []},
          restart => permanent,
          type => worker}
    ],
    {ok, {SupFlags, ChildSpecs}}.

