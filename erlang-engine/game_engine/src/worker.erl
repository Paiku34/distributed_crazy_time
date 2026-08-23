%%%-------------------------------------------------------------------
%% @doc Worker process — consuma le scommesse da bets_queue via AMQP e
%%      le inoltra al wheel_process del LEADER.
%%
%%      Il worker gira su TUTTI i nodi del cluster: i worker sono
%%      competing consumers della stessa coda, quindi l'ingestione delle
%%      scommesse e' distribuita. Solo wheel_process e' leader-only.
%%
%%      Ne segue che ogni messaggio consumato va instradato al nodo
%%      leader, non al wheel_process locale: su uno standby quel processo
%%      e' dormiente e il messaggio sparirebbe in silenzio. L'identita'
%%      del leader arriva da leader_election con {set_leader, Node}.
%%
%%      Se non c'e' un leader noto (nessuna elezione conclusa, oppure
%%      nodo finito nella minoranza di una partizione) il messaggio viene
%%      rimesso in coda: restera' nel broker finche' un nodo che puo'
%%      servirlo lo consumera'.
%%
%%      ACK DIFFERITO: il canale verso il wheel e' asincrono, quindi l'ack
%%      AMQP non puo' piu' seguire il ritorno di una call. La scommessa
%%      resta in `inflight` finche' il wheel non risponde {bet_result, ...};
%%      solo allora si acka. Se il leader muore prima di rispondere, il
%%      timeout rimette la scommessa nel broker, che la fara' servire a un
%%      nodo vivo: nessuna puntata pagata e mai giocata.
%% @end
%%%-------------------------------------------------------------------
-module(worker).
-behaviour(gen_server).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(BETS_QUEUE, <<"bets_queue">>).

%% Quanto si attende l'esito di una scommessa prima di rimetterla in coda.
%% Non puo' scendere sotto i ~12 s: il wheel resta bloccato fino a 10 s nella
%% call al minigioco e in quella finestra non elabora i messaggi in arrivo.
-define(INFLIGHT_TIMEOUT, 15000).

-record(state, {
    leader   = undefined :: node() | undefined,
    %% Stato del taglio Chandy-Lamport: il worker e' un PARTECIPANTE, non
    %% un osservatore. Ha un solo canale entrante (dal wheel), quindi il
    %% suo taglio si chiude appena riceve il marker.
    cl       = cl_recorder:new(),
    %% #{BetId => {DeliveryTag, BetMap, TimerRef}} — scommesse consumate dal
    %% broker e inoltrate al wheel, in attesa di esito. Non sono ancora ackate.
    inflight = #{} :: #{binary() => {term(), map(), reference()}}
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    io:format("~n=================================~n"),
    io:format("  Worker RabbitMQ (AMQP) avviato~n"),
    io:format("  Consumer su bets_queue (tutti i nodi)~n"),
    io:format("=================================~n~n"),
    ok = rabbitmq_manager:subscribe(?BETS_QUEUE, self()),
    %% Dopo un restart del worker nessuno ri-annuncia il leader: ce lo
    %% facciamo dire subito da leader_election, che parte prima di noi.
    Leader = current_leader(),
    {ok, #state{leader = Leader}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({set_leader, Node}, State) ->
    case Node =:= State#state.leader of
        true  -> ok;
        false -> io:format("[WORKER] Leader corrente: ~p~n", [Node])
    end,
    {noreply, State#state{leader = Node}};

%% --- MARKER dal wheel: apre e chiude subito il taglio locale ---
%%
%% Il marker uscente parte da QUI, non dal collector: deve viaggiare sullo
%% stesso canale applicativo dei {bet, ...} per condividerne l'ordine FIFO.
%% Tutto cio' che il worker ha spedito prima appartiene al taglio, tutto
%% cio' che spedisce dopo no.
handle_cast({cl_marker, SnapId, {wheel, WheelNode} = From}, State) ->
    InCh = [From],
    %% Stato locale: le scommesse consumate dal broker e non ancora ackate.
    %% E' l'informazione che al crash del leader distingue le bet perse da
    %% quelle che il broker riconsegnera'.
    Local = #{unacked => [{Id, B} || {Id, {_Tag, B, _T}} <- maps:to_list(State#state.inflight)]},
    {CL, Kind} = cl_recorder:on_marker(SnapId, From, InCh, Local, State#state.cl),
    case Kind of
        first_marker ->
            gen_server:cast({wheel_process, WheelNode},
                            {cl_marker, SnapId, {worker, node()}});
        subsequent ->
            ok
    end,
    %% Un solo canale entrante: il taglio e' gia' completo, si riporta subito.
    case cl_recorder:is_complete(CL) of
        true ->
            gen_server:cast({snapshot, WheelNode},
                            {cl_part, SnapId, {worker, node()},
                             cl_recorder:local(CL), cl_recorder:channels(CL)}),
            io:format("[WORKER] Taglio ~p: riportate ~p bet non ackate~n",
                      [SnapId, length(maps:get(unacked, Local))]),
            {noreply, State#state{cl = cl_recorder:new()}};
        false ->
            {noreply, State#state{cl = CL}}
    end;

%% Esito di una scommessa dal wheel del leader: e' l'UNICO punto in cui si acka.
handle_cast({bet_result, BetId, Verdict}, State) ->
    case maps:take(BetId, State#state.inflight) of
        {{Tag, _BetMap, TRef}, Rest} ->
            erlang:cancel_timer(TRef),
            case Verdict of
                not_leader ->
                    %% Il leader e' cambiato mentre la bet era in volo: torna
                    %% al broker, che la fara' servire da chi puo'.
                    io:format("[WORKER] Bet ~s: nodo non leader, rimessa in coda~n", [BetId]),
                    rabbitmq_manager:reject(Tag, true);
                _ ->
                    io:format("[WORKER] Bet ~s: ~p (ack)~n", [BetId, Verdict]),
                    rabbitmq_manager:ack(Tag)
            end,
            {noreply, State#state{inflight = Rest}};
        error ->
            %% Esito arrivato dopo lo scadere del timeout: la bet e' gia'
            %% tornata al broker e verra' riconsegnata. La deduplica per
            %% bet_id lato wheel impedisce che venga giocata due volte.
            io:format("[WORKER] Esito tardivo per ~s, ignorato~n", [BetId]),
            {noreply, State}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

%% Conferme del broker alla sottoscrizione / cancellazione: nulla da fare.
handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};
handle_info(#'basic.cancel_ok'{}, State) ->
    {noreply, State};
handle_info(#'basic.cancel'{}, State) ->
    io:format("[WORKER] Consumer cancellato dal broker.~n"),
    {noreply, State};

%% Nessun leader noto: il messaggio torna al broker, che lo fara' servire
%% da un nodo in grado di elaborarlo. E' l'unico caso in cui si rifiuta.
handle_info({#'basic.deliver'{delivery_tag = Tag}, _Msg}, State = #state{leader = undefined}) ->
    io:format("[WORKER] Nessun leader eletto: messaggio rimesso in coda~n"),
    rabbitmq_manager:reject(Tag, true),
    {noreply, State};

handle_info({#'basic.deliver'{delivery_tag = Tag}, #amqp_msg{payload = Payload}}, State) ->
    %% process_message/3 decide da se' se ackare subito, rimettere in coda o
    %% mettere la bet in attesa di esito. Su errore di elaborazione si acka
    %% comunque: un messaggio malformato rimesso in coda verrebbe
    %% riconsegnato all'infinito.
    NewState =
        try
            process_message(binary_to_list(Payload), Tag, State)
        catch
            Class:Err:Stack ->
                io:format("[WORKER] Errore elaborazione messaggio: ~p:~p~nStacktrace: ~p~n",
                          [Class, Err, Stack]),
                rabbitmq_manager:ack(Tag),
                State
        end,
    {noreply, NewState};

%% Il wheel non ha risposto in tempo: il leader e' morto, oppure era bloccato
%% nel minigioco. La scommessa torna nel broker invece di sparire.
handle_info({inflight_timeout, BetId}, State) ->
    case maps:take(BetId, State#state.inflight) of
        {{Tag, _BetMap, _TRef}, Rest} ->
            io:format("[WORKER] Nessun esito per la bet ~s entro ~ps: rimessa in coda~n",
                      [BetId, ?INFLIGHT_TIMEOUT div 1000]),
            rabbitmq_manager:reject(Tag, true),
            {noreply, State#state{inflight = Rest}};
        error ->
            {noreply, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal — parsing e forwarding
%%====================================================================

%% Chiede a leader_election chi e' il leader corrente. Usata solo all'avvio.
current_leader() ->
    try leader_election:get_leader() of
        {ok, Node} -> Node;
        _          -> undefined
    catch
        _:_ -> undefined
    end.

%% PayloadStr e' il JSON della scommessa cosi' come arriva dal broker.
%% Tutti i percorsi vengono instradati al wheel_process del LEADER: se
%% finissero al wheel_process locale, su uno standby sparirebbero in
%% silenzio (con 3 nodi succede circa 2 volte su 3).
process_message(PayloadStr, Tag, State = #state{leader = Leader}) ->
    case parse_bet_json(PayloadStr) of
        {ok, BetMap} ->
            io:format("[WORKER] Messaggio ricevuto: ~p~n", [BetMap]),
            case maps:get(<<"type">>, BetMap, <<"bet">>) of
                <<"force_segment">> ->
                    cast_to_wheel(Leader, {force_segment, maps:get(<<"segment">>, BetMap)}),
                    rabbitmq_manager:ack(Tag),
                    State;
                <<"minigame_choice">> ->
                    cast_to_wheel(Leader, {minigame_choice,
                                           maps:get(<<"username">>, BetMap),
                                           maps:get(<<"choice">>, BetMap)}),
                    rabbitmq_manager:ack(Tag),
                    State;
                _ ->
                    case maps:get(<<"segment">>, BetMap, <<>>) of
                        <<"UNDO_BETS">> ->
                            Username = maps:get(<<"username">>, BetMap),
                            io:format("[WORKER] Comando UNDO per utente: ~s~n", [Username]),
                            %% cast, non call: come call cross-nodo andrebbe in
                            %% timeout quando il wheel e' bloccato nel minigioco.
                            %% Il rimborso lo pubblica il wheel, che conosce gli
                            %% importi annullati.
                            cast_to_wheel(Leader, {undo_bets, Username}),
                            rabbitmq_manager:ack(Tag),
                            State;
                        <<"FORCE_", Seg/binary>> ->
                            io:format("[WORKER] Comando FORZATURA segmento: ~s~n", [Seg]),
                            cast_to_wheel(Leader, {force_segment, Seg}),
                            rabbitmq_manager:ack(Tag),
                            State;
                        _ ->
                            forward_bet(BetMap, Tag, State)
                    end
            end;
        {error, Reason} ->
            io:format("[WORKER] Errore parsing bet: ~p~n", [Reason]),
            rabbitmq_manager:ack(Tag),
            State
    end.

cast_to_wheel(Leader, Msg) ->
    gen_server:cast({wheel_process, Leader}, Msg).

%% Inoltro asincrono della scommessa al wheel del leader.
%%
%% Il messaggio porta il nodo mittente, cosi' il wheel sa a quale worker
%% rispondere: i worker sono N e solo chi ha consumato quel messaggio dal
%% broker possiede il delivery tag da ackare.
forward_bet(BetMap, Tag, State = #state{leader = Leader}) ->
    %% Ritardo artificiale, SOLO per i test: rende deterministico il caso
    %% "bet in transito al momento del taglio", che altrimenti dipende da una
    %% finestra di pochi millisecondi. Con un ritardo maggiore del tempo che
    %% il wheel impiega a emettere i marker, la scommessa arriva a taglio gia'
    %% aperto e finisce nello stato del canale. Default 0 = disattivato.
    case application:get_env(game_engine, bet_forward_delay, 0) of
        0 -> ok;
        Delay -> timer:sleep(Delay)
    end,
    gen_server:cast({wheel_process, Leader}, {bet, BetMap, node()}),
    case maps:get(<<"bet_id">>, BetMap, undefined) of
        undefined ->
            %% Senza bet_id non possiamo ne' tracciare l'esito ne' deduplicare
            %% una eventuale riconsegna: si acka subito, come prima dell'ack
            %% differito. Riguarda solo messaggi pubblicati a mano: il gateway
            %% mette sempre un UUID.
            io:format("[WORKER] Bet senza bet_id: ack immediato, nessun tracciamento~n"),
            rabbitmq_manager:ack(Tag),
            State;
        BetId ->
            TRef = erlang:send_after(?INFLIGHT_TIMEOUT, self(), {inflight_timeout, BetId}),
            Inflight = maps:put(BetId, {Tag, BetMap, TRef}, State#state.inflight),
            State#state{inflight = Inflight}
    end.

%% Parser semplice per JSON
parse_bet_json(JsonStr) ->
    try
        TypeMatch = extract_string_field(JsonStr, "type"),
        Type = case TypeMatch of {ok, T} -> list_to_binary(T); _ -> <<>> end,

        UsernameMatch = extract_string_field(JsonStr, "username"),
        Username = case UsernameMatch of {ok, U} -> list_to_binary(U); _ -> <<"unknown">> end,

        case Type of
            <<"minigame_choice">> ->
                ChoiceMatch = extract_string_field(JsonStr, "choice"),
                Choice = case ChoiceMatch of {ok, C} -> list_to_binary(C); _ -> <<"blue">> end,
                {ok, #{<<"type">> => Type,
                       <<"username">> => Username,
                       <<"choice">> => Choice}};
            <<"force_segment">> ->
                SegmentMatch = extract_string_field(JsonStr, "segment"),
                Segment = case SegmentMatch of {ok, S} -> list_to_binary(S); _ -> <<>> end,
                {ok, #{<<"type">> => Type,
                       <<"segment">> => Segment}};
            _ ->
                AmountMatch = extract_number_field(JsonStr, "amount"),
                SegmentMatch = extract_string_field(JsonStr, "segment"),
                %% bet_id: presente su ogni scommessa vera, assente sui comandi
                %% (UNDO_BETS, FORCE_*) e sui messaggi pubblicati a mano.
                BetId = case extract_string_field(JsonStr, "bet_id") of
                            {ok, B} -> list_to_binary(B);
                            _ -> undefined
                        end,
                case {AmountMatch, SegmentMatch} of
                    {{ok, A}, {ok, S}} ->
                        {ok, #{<<"type">> => Type,
                               <<"username">> => Username,
                               <<"amount">> => A,
                               <<"bet_id">> => BetId,
                               <<"segment">> => list_to_binary(S)}};
                    _ ->
                        io:format("[WORKER] DEBUG: missing_fields in JSON: ~p~n", [JsonStr]),
                        {error, {missing_fields, JsonStr}}
                end
        end
    catch
        Class:Err:Stack ->
            io:format("[WORKER] EXCEPTION in parse_bet_json: ~p:~p~nStacktrace: ~p~n", [Class, Err, Stack]),
            {error, Err}
    end.

extract_string_field(Json, FieldName) ->
    Pattern = "\"" ++ FieldName ++ "\"\\s*:\\s*\"([^\"]+)\"",
    case re:run(Json, Pattern, [{capture, all_but_first, list}]) of
        {match, [Value]} -> {ok, Value};
        _ -> {error, not_found}
    end.

extract_number_field(Json, FieldName) ->
    Pattern = "\"" ++ FieldName ++ "\"\\s*:\\s*([0-9]+\\.?[0-9]*)",
    case re:run(Json, Pattern, [{capture, all_but_first, list}]) of
        {match, [Value]} ->
            case string:find(Value, ".") of
                nomatch -> {ok, list_to_integer(Value)};
                _ -> {ok, list_to_float(Value)}
            end;
        _ -> {error, not_found}
    end.

%% I rimborsi non passano piu' da qui: e' il wheel a pubblicare un evento
%% bet_rejected per singolo bet_id, sia per la scommessa in ritardo sia per
%% l'annullamento. La coda refunds_queue non viene piu' usata.
