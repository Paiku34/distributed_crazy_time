%%%-------------------------------------------------------------------
%% @doc Leader Election - Implementazione dell'algoritmo Bully.
%%
%%      Questo gen_server si occupa di eleggere un leader tra i nodi
%%      del cluster Erlang e di assegnare il ruolo di `leader` o `standby`.
%%      Il nodo con il nome (atomo) lessicograficamente piu' alto vince l'elezione.
%%
%%      Quando un nodo diventa leader attiva i processi del gioco
%%      (wheel_process, worker) inviando un cast `activate`.
%%      I nodi standby ricevono un cast `deactivate` per rimanere dormienti.
%% @end
%%%-------------------------------------------------------------------
-module(leader_election).
-behaviour(gen_server).

-export([start_link/0, start_election/0, get_leader/0, is_leader/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    leader = undefined :: node() | undefined,
    election_in_progress = false :: boolean(),
    election_timer = undefined :: reference() | undefined,
    role = standby :: leader | standby
}).

-define(ELECTION_TIMEOUT, 3000).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_election() ->
    gen_server:cast(?MODULE, start_election).

get_leader() ->
    gen_server:call(?MODULE, get_leader).

is_leader() ->
    gen_server:call(?MODULE, is_leader).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    io:format("~n=================================~n"),
    io:format("  Leader Election avviato~n"),
    io:format("=================================~n~n"),
    {ok, #state{}}.

handle_call(get_leader, _From, State) ->
    {reply, {ok, State#state.leader}, State};

handle_call(is_leader, _From, State) ->
    {reply, State#state.role =:= leader, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

%% Inizia un'elezione: invia {election, MyNode} a tutti i nodi piu' alti
handle_cast(start_election, State) ->
    MyNode = node(),
    AllNodes = [node() | nodes()],
    HigherNodes = [N || N <- AllNodes, N > MyNode],
    
    case HigherNodes of
        [] ->
            %% Sono il nodo piu' alto — dichiaro vittoria
            declare_victory(MyNode),
            {noreply, State#state{leader = MyNode, role = leader, 
                                  election_in_progress = false}};
        _ ->
            %% Invia messaggio di elezione ai nodi piu' alti
            lists:foreach(fun(N) ->
                gen_server:cast({?MODULE, N}, {election, MyNode})
            end, HigherNodes),
            %% Imposta timeout — se non rispondono, vinco
            TRef = erlang:send_after(?ELECTION_TIMEOUT, self(), election_timeout),
            {noreply, State#state{election_in_progress = true, 
                                  election_timer = TRef}}
    end;

%% Ricevuto messaggio di elezione da un nodo piu' basso — risponde "Sono vivo" e inizia propria elezione
handle_cast({election, FromNode}, State) ->
    gen_server:cast({?MODULE, FromNode}, {alive, node()}),
    %% Inizia elezione se non e' gia' in corso (o forzala comunque)
    self() ! trigger_election,
    {noreply, State};

%% Ricevuta risposta "alive" — un nodo piu' alto esiste, ferma l'elezione
handle_cast({alive, _HigherNode}, State) ->
    cancel_timer(State#state.election_timer),
    {noreply, State#state{election_in_progress = false, election_timer = undefined}};

%% Ricevuto annuncio del nuovo coordinatore (leader) — accetta il leader
handle_cast({coordinator, Leader}, State) ->
    cancel_timer(State#state.election_timer),
    io:format("[ELECTION] Nuovo leader eletto: ~p~n", [Leader]),
    NewRole = case Leader =:= node() of true -> leader; false -> standby end,
    apply_role(NewRole),  %% Attiva o disattiva i processi di gioco
    {noreply, State#state{leader = Leader, role = NewRole, 
                          election_in_progress = false}};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% Timeout elezione — nessun nodo piu' alto ha risposto, dichiaro vittoria!
handle_info(election_timeout, State) ->
    declare_victory(node()),
    {noreply, State#state{leader = node(), role = leader, 
                          election_in_progress = false,
                          election_timer = undefined}};

handle_info(trigger_election, State) ->
    gen_server:cast(?MODULE, start_election),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

declare_victory(MyNode) ->
    io:format("~n*** [ELECTION] Sono il nuovo LEADER: ~p ***~n~n", [MyNode]),
    AllNodes = nodes(),
    lists:foreach(fun(N) ->
        gen_server:cast({?MODULE, N}, {coordinator, MyNode})
    end, AllNodes),
    apply_role(leader).

apply_role(leader) ->
    io:format("[ROLE] Questo nodo ora e' l'ACTIVE DEALER~n"),
    %% Dice al wheel_process di attivarsi (inizia ad accettare scommesse, turni ecc)
    gen_server:cast(wheel_process, activate),
    %% Dice al worker di iniziare a consumare dalla bets_queue
    gen_server:cast(worker, activate);

apply_role(standby) ->
    io:format("[ROLE] Questo nodo ora e' in STANDBY~n"),
    %% Dice al wheel_process di andare in dormienza (ferma timer, rifiuta scommesse)
    gen_server:cast(wheel_process, deactivate),
    %% Dice al worker di smettere di consumare
    gen_server:cast(worker, deactivate).

cancel_timer(undefined) -> ok;
cancel_timer(TRef) -> erlang:cancel_timer(TRef).
