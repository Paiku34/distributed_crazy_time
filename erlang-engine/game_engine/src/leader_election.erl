%%%-------------------------------------------------------------------
%% @doc Leader Election - Implementazione dell'algoritmo Bully.
%%
%%      Elegge un leader fra i nodi del cluster e assegna il ruolo di
%%      `leader` o `standby'. Vince il nodo con il nome (atomo)
%%      lessicograficamente piu' alto.
%%
%%      Cosa e' leader-only e cosa no:
%%        - wheel_process gira SOLO sul leader: riceve activate/deactivate;
%%        - il worker gira su TUTTI i nodi (ingestione distribuita delle
%%          scommesse) e riceve invece {set_leader, Node}, cosi' sa a chi
%%          inoltrare i messaggi che consuma da RabbitMQ.
%%
%%      Guardia di quorum: un nodo si dichiara leader solo se vede la
%%      maggioranza dei nodi CONFIGURATI, e un leader gia' in carica che
%%      finisce in minoranza si autoretrocede a standby. Senza il secondo
%%      controllo una partizione 2-1 produrrebbe due leader, due ledger
%%      divergenti per lo stesso round e scritture concorrenti su Mnesia.
%% @end
%%%-------------------------------------------------------------------
-module(leader_election).
-behaviour(gen_server).

-include("game_engine.hrl").

-export([start_link/0, start_election/0, get_leader/0, is_leader/0, node_down/1,
         cancel_round/1]).
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

%% Annulla un round: rimborsa le sue bet pendenti TRANNE quelle che i worker
%% superstiti hanno ancora in mano, che il broker riconsegnera' da solo.
%% Chiamata anche dal wheel quando un round interrotto non e' completabile.
cancel_round(Round) ->
    spawn(fun() -> do_cancel_round(Round) end),
    ok.

%% Notifica della caduta di un nodo, inviata da cluster_manager.
%% E' un cast: il chiamante non deve mai bloccarsi su questa decisione.
node_down(Node) ->
    gen_server:cast(?MODULE, {node_down, Node}).

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

%% Un'elezione e' gia' in corso: non se ne avvia una seconda. Senza questa
%% guardia tre nodi che partono insieme producono una raffica di elezioni.
handle_cast(start_election, State = #state{election_in_progress = true}) ->
    {noreply, State};

%% Inizia un'elezione: invia {election, MyNode} a tutti i nodi piu' alti
handle_cast(start_election, State) ->
    MyNode = node(),
    HigherNodes = [N || N <- nodes(), N > MyNode],

    case HigherNodes of
        [] ->
            %% Sono il nodo piu' alto — provo a dichiarare vittoria
            {noreply, resolve_victory(State)};
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

%% Ricevuto messaggio di elezione da un nodo piu' basso — risponde "Sono vivo"
%% e inizia la propria elezione, se non ne ha gia' una in corso.
handle_cast({election, FromNode}, State) ->
    gen_server:cast({?MODULE, FromNode}, {alive, node()}),
    handle_cast(start_election, State);

%% Ricevuta risposta "alive" — un nodo piu' alto esiste, ferma l'elezione
handle_cast({alive, _HigherNode}, State) ->
    cancel_timer(State#state.election_timer),
    {noreply, State#state{election_in_progress = false, election_timer = undefined}};

%% Ricevuto annuncio del nuovo coordinatore (leader) — accetta il leader
handle_cast({coordinator, Leader}, State) ->
    cancel_timer(State#state.election_timer),
    io:format("[ELECTION] Nuovo leader eletto: ~p~n", [Leader]),
    NewRole = case Leader =:= node() of true -> leader; false -> standby end,
    apply_role(NewRole),          %% attiva o disattiva il SOLO wheel_process
    maybe_recover(State#state.role, NewRole),
    set_local_leader(Leader),     %% il worker locale deve sapere a chi inoltrare
    {noreply, State#state{leader = Leader, role = NewRole,
                          election_in_progress = false, election_timer = undefined}};

%% Caduta di un nodo, notificata da cluster_manager.
%% Il ramo che conta e' il primo: un leader gia' in carica finito nella
%% minoranza non ripassa mai da declare_victory/1 e resterebbe attivo.
handle_cast({node_down, Node}, State = #state{role = leader}) ->
    case has_quorum() of
        true ->
            {noreply, maybe_reelect(Node, State)};
        false ->
            io:format("[ELECTION] *** QUORUM PERSO — retrocessione a standby ***~n"),
            apply_role(standby),
            broadcast_leader(undefined),
            {noreply, State#state{role = standby, leader = undefined}}
    end;

handle_cast({node_down, Node}, State) ->
    case has_quorum() of
        true ->
            {noreply, maybe_reelect(Node, State)};
        false ->
            io:format("[ELECTION] Quorum assente dopo la caduta di ~p: nessuna elezione~n", [Node]),
            set_local_leader(undefined),
            {noreply, State#state{leader = undefined}}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

%% Timeout elezione — nessun nodo piu' alto ha risposto, provo a vincere
handle_info(election_timeout, State) ->
    {noreply, resolve_victory(State#state{election_in_progress = false,
                                          election_timer = undefined})};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

%% Il recovery parte SOLO alla transizione standby -> leader, non a ogni
%% rielezione: le elezioni si ripetono a ogni nodo che entra o esce, e
%% rieseguirlo su un leader gia' in carica annullerebbe il round in corso.
maybe_recover(leader, leader) -> ok;
maybe_recover(_OldRole, leader) ->
    %% Fuori dal gen_server: raccoglie le bet non ackate dai worker con
    %% chiamate che possono attendere fino a 2 s ciascuna, e l'elezione non
    %% deve restare bloccata nel frattempo.
    spawn(fun() -> recover_from_checkpoint() end),
    ok;
maybe_recover(_OldRole, _NewRole) -> ok.

%% Esito di un tentativo di vittoria, con la guardia di quorum applicata.
resolve_victory(State) ->
    case declare_victory(node()) of
        leader ->
            maybe_recover(State#state.role, leader),
            State#state{leader = node(), role = leader,
                        election_in_progress = false, election_timer = undefined};
        standby ->
            State#state{leader = undefined, role = standby,
                        election_in_progress = false, election_timer = undefined}
    end.

declare_victory(MyNode) ->
    case has_quorum() of
        false ->
            io:format("[ELECTION] Quorum assente: non mi dichiaro leader, resto standby~n"),
            apply_role(standby),
            broadcast_leader(undefined),
            standby;
        true ->
            io:format("~n*** [ELECTION] Sono il nuovo LEADER: ~p ***~n~n", [MyNode]),
            lists:foreach(fun(N) ->
                gen_server:cast({?MODULE, N}, {coordinator, MyNode})
            end, nodes()),
            apply_role(leader),
            broadcast_leader(MyNode),
            leader
    end.

%% Rielezione solo se serve davvero: il nodo caduto era il leader, oppure
%% non abbiamo un leader noto.
maybe_reelect(DownNode, State = #state{leader = DownNode}) ->
    io:format("[ELECTION] *** LEADER ~p CADUTO! Elezione d'emergenza ***~n", [DownNode]),
    gen_server:cast(?MODULE, start_election),
    State#state{leader = undefined};
maybe_reelect(_DownNode, State = #state{leader = undefined}) ->
    gen_server:cast(?MODULE, start_election),
    State;
maybe_reelect(_DownNode, State) ->
    State.

%% Maggioranza sui nodi CONFIGURATI, non su nodes(): una lista che si
%% restringe da sola durante una partizione renderebbe la guardia inutile.
%% Anche il numeratore e' intersecato con la lista statica, altrimenti una
%% shell diagnostica attaccata al lato di minoranza gli regalerebbe il quorum.
has_quorum() ->
    try cluster_manager:configured_nodes() of
        Cfg when is_list(Cfg), length(Cfg) > 1 ->
            Live = [N || N <- [node() | nodes()], lists:member(N, Cfg)],
            Ok = length(Live) * 2 > length(Cfg),
            case Ok of
                true  -> ok;
                false -> io:format("[ELECTION] Quorum ~p/~p non raggiunto~n",
                                   [length(Live), length(Cfg)])
            end,
            Ok;
        _ ->
            %% Nodo singolo (sviluppo, oppure peer_nodes non configurato):
            %% non c'e' partizione possibile, il quorum non si applica.
            true
    catch
        _:_ ->
            %% cluster_manager non disponibile: non e' una partizione, e'
            %% il nostro stesso nodo che sta ripartendo.
            io:format("[ELECTION] cluster_manager non raggiungibile: quorum non verificabile~n"),
            true
    end.

%% Solo wheel_process e' leader-only. Il worker resta attivo su tutti i nodi.
apply_role(leader) ->
    io:format("[ROLE] Questo nodo ora e' l'ACTIVE DEALER~n"),
    gen_server:cast(wheel_process, activate);

apply_role(standby) ->
    io:format("[ROLE] Questo nodo ora e' in STANDBY~n"),
    gen_server:cast(wheel_process, deactivate).

%% Comunica a TUTTI i worker del cluster chi e' il leader corrente.
broadcast_leader(Leader) ->
    lists:foreach(fun(N) ->
        gen_server:cast({worker, N}, {set_leader, Leader})
    end, [node() | nodes()]).

%% Comunica il leader al solo worker locale (quando l'annuncio arriva da altri).
set_local_leader(Leader) ->
    gen_server:cast(worker, {set_leader, Leader}).

%%====================================================================
%% Recovery al cambio di leader
%%====================================================================

%% Due rami, decisi leggendo l'ultimo checkpoint su Mnesia.
%%
%%   A. checkpoint presente e risultato NON ancora pubblicato
%%      -> il leader precedente e' morto dopo il gong ma prima di pagare:
%%         il round si COMPLETA riusando l'esito catturato nel taglio.
%%
%%   B. nessun checkpoint da completare
%%      -> il crash e' avvenuto in fase di puntata, quindi non esiste esito.
%%         Si annulla il SOLO round interrotto, e non tutte le sue bet: quelle
%%         che i worker hanno ancora in mano rientrano dal broker e verranno
%%         giocate nel round successivo. Rimborsarle sarebbe un doppio gioco.
recover_from_checkpoint() ->
    timer:sleep(500),   %% lascia al wheel il tempo di attivarsi
    case snapshot:get_last() of
        {ok, #snapshot_record{result_published = false} = Rec} ->
            io:format("[RECOVERY] Checkpoint del round ~p con risultato non pubblicato~n",
                      [Rec#snapshot_record.round]),
            gen_server:cast(wheel_process,
                            {complete_round,
                             Rec#snapshot_record.round,
                             Rec#snapshot_record.ledger,
                             Rec#snapshot_record.winner_segment,
                             Rec#snapshot_record.winner_index});
        {ok, #snapshot_record{round = LastRound}} ->
            io:format("[RECOVERY] Ultimo round completato: ~p. "
                      "Annullo l'eventuale round interrotto ~p~n", [LastRound, LastRound + 1]),
            do_cancel_round(LastRound + 1);
        none ->
            io:format("[RECOVERY] Nessun checkpoint: niente da recuperare~n"),
            ok
    end.

do_cancel_round(Round) ->
    Excluded = collect_inflight(Round),
    Payload = lists:flatten(io_lib:format(
        "{\"type\":\"round_cancelled\",\"round\":~p,\"exclude_bet_ids\":~s}",
        [Round, json_list(Excluded)])),
    case rabbitmq_manager:publish(<<"results_queue">>, unicode:characters_to_binary(Payload)) of
        ok ->
            io:format("[RECOVERY] Round ~p annullato. Bet escluse dal rimborso (rientrano "
                      "dal broker): ~p~n", [Round, length(Excluded)]);
        {error, Reason} ->
            io:format("[RECOVERY] Pubblicazione dell'annullamento fallita: ~p~n", [Reason])
    end.

%% Bet consumate dal broker e mai ackate, chieste a tutti i worker vivi.
%% Sono l'insieme che distingue le puntate DAVVERO perse (da rimborsare) da
%% quelle che il broker rigiochera' da solo.
collect_inflight(Round) ->
    Nodes = try cluster_manager:get_participants() of
                L when is_list(L) -> L
            catch _:_ -> [node()]
            end,
    lists:usort(lists:flatmap(fun(N) ->
        try gen_server:call({worker, N}, {collect_inflight, Round}, 2000) of
            Ids when is_list(Ids) -> Ids;
            _ -> []
        catch
            _:_ ->
                io:format("[RECOVERY] Worker su ~p non raggiungibile per collect_inflight~n", [N]),
                []
        end
    end, Nodes)).

json_list(Ids) ->
    "[" ++ string:join(["\"" ++ binary_to_list(I) ++ "\"" || I <- Ids, is_binary(I)], ",") ++ "]".

cancel_timer(undefined) -> ok;
cancel_timer(TRef) -> erlang:cancel_timer(TRef), ok.
