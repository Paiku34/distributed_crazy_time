# Snapshot Implementation Plan — Chandy-Lamport in Distributed Crazy Time

## Context

`snapshot_analisi.md` conclude che lo snapshot previsto dalla Fase 4 di `implementation_plan.md` è ridondante: cattura uno stato già disponibile in locale sul leader, su canali vuoti per costruzione, e nessuno ne consuma il risultato. La tesi è stata verificata contro il codice: **è corretta**.

Questo piano definisce le modifiche a `implementation_plan.md` (Fasi 2-6) e al codice per rendere lo snapshot **load-bearing**: un algoritmo il cui risultato non è ottenibile in altro modo e che alimenta due decisioni reali del sistema (ledger del round verso Java, recovery al crash del dealer).

**Scelte già prese** (rispondendo alle domande poste):
- ingestione bet distribuita: tutti i nodi consumano da `bets_queue` come competing consumers e inoltrano **asincroni** al wheel del leader;
- `bet_id` UUID generato da Java + `round` autoritativo assegnato da Erlang;
- persistenza degli snapshot su **Mnesia replicata** (colma anche il gap col diagramma architetturale della specifica, dove Mnesia c'è ma non è mai stata usata).

---

## Parte 1 — Architettura risultante

Il rifacimento nasce da una sola idea: **spostare i marker sui canali applicativi reali e rendere quei canali asincroni**, così che ci sia davvero qualcosa da catturare.

```
   [bets_queue]  ──competing consumers──┬──> worker@game1 ─┐
                                        ├──> worker@game2 ─┼─{bet, B}──> wheel_process@LEADER
                                        └──> worker@game3 ─┘
                                                  ^
                                                  └────{bet_result, BetId, accepted|rejected}────┘
```

- Grafo **fortemente connesso** (stella bidirezionale), canali **asincroni** e **FIFO** (garanzia Erlang).
- Il canale `worker_i → wheel` contiene le **bet in transito** al momento del gong: l'unica cosa che la specifica chiede di catturare (§1.2) e l'unica non disponibile in locale da nessuna parte.
- Il canale `wheel → worker_i` contiene gli ack/reject in volo.

**Partecipanti allo snapshot** = `wheel_process` e gli N `worker`. **Non** il modulo `snapshot`, che diventa un puro **collector**: assegna l'id, congela la lista dei partecipanti, arma il timeout, raccoglie le porzioni, persiste su Mnesia, pubblica il ledger. Il collector non chiama mai i partecipanti — sono loro a fare push. Questo elimina l'accoppiamento fragile segnalato dall'analisi.

**Costo in latenza: zero.** Lo snapshot parte al gong e ha un budget di 5 s; la risoluzione del round è già schedulata a 10,5 s per l'animazione della ruota ([wheel_process.erl:174](erlang-engine/game_engine/src/wheel_process.erl#L174) per il ramo moltiplicatore, [:181](erlang-engine/game_engine/src/wheel_process.erl#L181) per il ramo minigioco). Il taglio è chiuso molto prima che serva.

**Ordine nel tick del gong** (importante per il recovery): si estrae **prima** il segmento vincente, **poi** si inizia lo snapshot. Così il taglio cattura `{bets, winner_segment, winner_index}` insieme, e un nuovo leader eletto dopo il crash ha sia l'insieme autorevole delle puntate sia l'esito autorevole: può **completare** il round anziché annullarlo.

---

## Parte 2 — Durabilità dell'ingestione e quorum

Due proprietà che l'architettura della Parte 1 **non** garantisce da sola. Vanno risolte insieme allo
snapshot, perché senza di esse il ledger e l'audit trail perdono valore probatorio.

### La finestra di perdita fra ack AMQP e arrivo al leader

Il worker consuma dalla coda, acka, e poi inoltra al wheel del leader. Fra l'ack e l'arrivo del `cast`
la bet **non esiste in nessuno stato replicato**: è uscita dal broker, il wallet è già stato addebitato
([WalletController.java:148-149](java-gateway/src/main/java/com/crazytime/controller/WalletController.java#L148-L149)),
e la mappa `inflight` del worker è locale e viene letta solo durante uno snapshot. Se il leader crasha
in fase `betting` non esiste alcun checkpoint e la bet è persa in silenzio: il giocatore ha pagato, il
sistema non sa che esiste.

**Rimedio: differire l'ack fino a `{bet_result, BetId, _}`.** L'ack diventa la conferma che il wheel ha
deciso, non che il worker ha ricevuto. Le conseguenze sono tutte favorevoli:

- se il leader muore prima di rispondere, i messaggi non ackati vengono **automaticamente riconsegnati**
  dal broker a un worker vivo → la bet non è persa, entra nel round successivo. È strettamente meglio
  del rimborso;
- il broker torna a essere il buffer durevole che è, invece di essere scavalcato;
- il rischio introdotto — riconsegna di una bet già accettata il cui ack si è perso — è **già coperto**
  dal `bet_id` UUID: il wheel deduplica e riacka. Senza `bet_id` questo rimedio non sarebbe praticabile.

Il costo è nullo: `prefetch_count` è già impostato (vedi §Parte 7) e limita di per sé quante bet un
worker può tenere non ackate.

### Split-brain: due leader, due ledger, Mnesia da riparare a mano

La Fase 3 fa scattare l'elezione su `nodedown` senza distinguere **crash** da **partizione di rete**.
In una partizione 2-1 entrambi i lati vedono `nodedown` ed entrambi eleggono un leader. Finché il danno
era il solo RNG duplicato era contenuto; con l'architettura di questo piano diventa grave:

- entrambi i lati consumano da `bets_queue` (il broker è raggiungibile da tutti) e pubblicano **ledger
  concorrenti e divergenti** per lo stesso numero di round;
- entrambi scrivono `snapshot_record` su Mnesia `disc_copies`; alla riconnessione Mnesia rileva
  `inconsistent_database` e **richiede riparazione manuale**;
- UC3 perde ogni valore: un audit trail che ammette due verità sullo stesso round non prova nulla.

**Rimedio: guardia di quorum prima di `declare_victory/1`.**

```erlang
%% Il denominatore e' la lista statica dei nodi configurati, non nodes().
%% Anche il NUMERATORE va intersecato con quella lista: nodes() restituisce ogni
%% nodo Erlang connesso, comprese le shell diagnostiche (§Parte 7 ne apre una per
%% ispezionare Mnesia). Una shell attaccata al lato di minoranza gli regalerebbe
%% il quorum proprio durante il test che deve dimostrare il contrario.
has_quorum() ->
    Cfg  = cluster_manager:configured_nodes(),
    Live = [N || N <- [node() | nodes()], lists:member(N, Cfg)],
    length(Live) * 2 > length(Cfg).
```

- **due punti di controllo, non uno.** `declare_victory/1` copre solo chi *sta diventando* leader; il
  leader **già in carica** finito nel lato di minoranza non esegue mai `declare_victory` e resterebbe
  attivo. Serve quindi la stessa guardia anche su `nodedown`:
  ```erlang
  %% in leader_election.erl
  handle_info({nodedown, _Node}, State = #state{role = leader}) ->
      case has_quorum() of
          true  -> {noreply, State};
          false ->
              io:format("[LEADER] Quorum perso — retrocessione a standby~n"),
              apply_role(standby),
              {noreply, State#state{role = standby, leader = undefined}}
      end;
  handle_info({nodedown, _Node}, State) -> {noreply, State}.
  ```
  Senza questo ramo la guardia è inefficace proprio nello scenario che deve coprire: è il **vecchio**
  leader isolato a produrre il ledger divergente, non il nuovo;
- il nodo retrocesso azzera `leader`, così i suoi worker passano al ramo `reject{requeue = true}`;
- i worker della minoranza hanno `leader = undefined` e fanno `rabbitmq_manager:reject(Tag, true)`: le bet
  restano nel broker e vengono servite dalla maggioranza. Nessuna bet persa, nessun ledger divergente;
- solo il leader scrive `snapshot_record`, e con il quorum garantito esiste al più un leader → nessuna
  scrittura Mnesia concorrente.

Due accorgimenti a corredo:
- chiave di `snapshot_record` = `{Round, Initiator}` anziché un intero monotono: due leader concorrenti
  produrrebbero record **distinti e diagnosticabili** invece di sovrascriversi silenziosamente;
- sottoscrivere `mnesia:subscribe(system)` e loggare `{inconsistent_database, _, _}` in modo rumoroso.

> Con 3 nodi il quorum è 2. Un cluster a **2 nodi non può avere quorum utile**: la partizione 1-1 blocca
> entrambi i lati. È una limitazione onesta da dichiarare nella relazione — è il teorema CAP, non un
> difetto implementativo: qui si sceglie la consistenza sulla disponibilità, che per un ledger di
> scommesse è la scelta giusta.

---

## Parte 3 — Le tre regole di riconciliazione

L'ack differito della §Parte 2 e il `round_cancelled` della Fase 5 si contraddicono se lasciati
impliciti: il primo dice che le bet non ackate vengono **rigiocate** nel round successivo, il secondo che
le bet `PENDING` del round caduto vengono **rimborsate**. Una bet che cade in entrambe le descrizioni
verrebbe rimborsata *e* giocata: il denaro torna nel wallet e la scommessa gira lo stesso.

Al crash del leader in fase `betting`, ogni bet del round R sta in **uno solo** di tre insiemi:

| Insieme | Come si riconosce | Destino corretto |
| :--- | :--- | :--- |
| **Ackata** — accettata dal wheel morto, ack già inviato | assente dalle `inflight` dei worker, assente dal broker | **Rimborso**: nessuno la possiede più, è l'unico caso realmente perso |
| **Non ackata** — consumata ma senza `bet_result` | presente nelle `inflight` di un worker vivo | **Replay**: il broker la riconsegna, entra nel round R+1. Nessun rimborso |
| **Mai consumata** | ancora nella coda | **Replay**: idem |

`{collect_inflight, R}` serve esattamente a separare il primo insieme dagli altri due. Da qui le tre
regole che vanno scritte nel piano una volta e rispettate ovunque:

> **R1 — Chi rimborsa.** Si rimborsa una bet solo se è assente da ogni ledger **e** assente da
> `exclude_bet_ids`. In dubbio non si rimborsa: la bet resta `PENDING` e sarà chiusa dal ledger del round
> in cui verrà rigiocata.
>
> **R2 — Chi è autoritativo.** `Bet.status` lato Java è autoritativo sui **movimenti di denaro**; il
> ledger Erlang è autoritativo sull'**esito di gioco**. Una bet già `REFUNDED` che ricompare in un ledger
> successivo (caso residuo: il worker che la teneva è morto insieme al leader, quindi non è finita in
> `exclude_bet_ids`) **non viene mai pagata**: `PayoutListener` interroga solo le `PENDING`, quindi la
> regola è già rispettata dalla query. `LedgerListener` deve però loggarla come `replay_after_refund` e
> chiuderla in stato terminale, perché è l'unico caso in cui l'utente vede sulla ruota una puntata che
> gli è stata restituita. Nessuna duplicazione di denaro, solo un'anomalia visiva da documentare.
>
> **R3 — L'insieme di dedup è l'unione dei ledger persistiti, non `bets` in memoria.** Una bet già
> presente in un ledger **non rientra mai in gioco**, in nessun round successivo: il wheel risponde
> `{bet_result, BetId, accepted}` senza rigiocarla, il worker acka e la scarta.

R3 è la regola che il resto della §Parte 2 dà per scontata senza enunciarla, ed è quella che rende
davvero sicuro l'ack differito. Il motivo è che `bets` viene azzerato a ogni round
([implementation_plan.md:1603-1617](implementation_plan.md#L1603-L1617)): una dedup che guarda solo
`bets` è idempotente **soltanto dentro la finestra del round**. Percorso concreto, che non richiede
nemmeno un crash:

1. il wheel accetta `BetId = X` nel round R e casta `{bet_result, X, accepted}`;
2. X entra nel ledger di R, pubblicato e persistito su Mnesia;
3. il worker muore prima di ackare — **oppure** il suo `inflight_timeout` da 15 s scatta perché il wheel
   era bloccato nella `gen_server:call` del minigioco, e rimette in coda una bet già accettata e già a
   ledger;
4. il broker riconsegna X nel round R+1;
5. senza R3, `bets` di R+1 non contiene X → **accettata di nuovo, giocata due volte, pagata due volte**.
   `Σ wallet` cresce dal nulla, e `PayoutListener` filtrato per round paga sia in R sia in R+1 perché
   `LedgerListener` avrà nel frattempo riscritto `Bet.round`.

R2 non copre questo caso: parla di una bet `REFUNDED` che ricompare, non di una `PENDING` già a ledger
che ricompare.

**Implementazione.** Il wheel mantiene in `#state{}` un set `settled_bet_ids` con i `bet_id` degli ultimi
3 round, ripopolato dai `snapshot_record` all'avvio e a ogni elezione (il nuovo leader ne ha bisogno
subito: è proprio dopo un crash che le riconsegne arrivano). Il fallback, se il set è vuoto o incerto, è
una scansione all'indietro di `snapshot_record`, banale con `ordered_set` e chiave `{Round, Initiator}`.

Le tre regole rendono ogni percorso deterministico e sono ciò che i test «Kill del leader in fase
`betting`», «Replay dopo rimborso» e «Riconsegna cross-round» della §Parte 7 verificano.

---

## Parte 4 — Modifiche a `implementation_plan.md`

### Fase 2 — Multi-Node Erlang Cluster
- Aggiungere il bootstrap **Mnesia**. Non si può usare `mnesia:create_schema(AllNodes)`: quella forma
  esige Mnesia **arrestata su tutti i nodi elencati**, condizione che non si verifica mai con nodi che
  si avviano progressivamente. Serve il join dinamico, in due rami distinti — sempre **dopo** la
  formazione del cluster, mai in `init/1`:

  **Primo nodo** (nessun peer raggiungibile con la tabella):
  ```erlang
  mnesia:create_schema([node()]),          %% ignorare {error,{_,{already_exists,_}}}
  mnesia:start(),
  mnesia:create_table(snapshot_record,
      [{attributes, record_info(fields, snapshot_record)},
       {type, ordered_set},
       {disc_copies, [node()]}]).
  ```

  **Nodo che si aggiunge** a un cluster dove la tabella esiste già:
  ```erlang
  mnesia:start(),
  {ok, _} = mnesia:change_config(extra_db_nodes, [MasterNode]),
  mnesia:change_table_copy_type(schema, node(), disc_copies),   %% altrimenti resta ram_copies
  mnesia:add_table_copy(snapshot_record, node(), disc_copies),
  ok = mnesia:wait_for_tables([snapshot_record], 5000).
  ```
  `MasterNode` = un nodo qualsiasi già nel cluster che possiede la tabella; discriminare i due rami
  interrogando `mnesia:table_info(snapshot_record, disc_copies)` via `rpc:call/4` sui peer raggiungibili.
  La `change_table_copy_type` dello schema è il passo che si dimentica più spesso: senza, il nodo tiene
  lo schema in RAM e **perde la propria copia a ogni riavvio**, vanificando `disc_copies`.
- `cluster_manager` espone `get_participants/0` che restituisce la lista **ordinata e stabile** dei nodi vivi; è la lista che lo snapshot congela.
- `cluster_manager` espone anche `configured_nodes/0` (lista **statica**, non `nodes()`): è il denominatore del quorum. Usare `nodes()` renderebbe la guardia inutile, perché in partizione si riduce da sola. La lista va aggiunta come nuova chiave dell'`env` in [game_engine.app.src:14-24](erlang-engine/game_engine/src/game_engine.app.src#L14-L24), che oggi contiene solo `rabbitmq`: `{nodes, ['game1@localhost','game2@localhost','game3@localhost']}`, sovrascrivibile per nodo con `-game_engine nodes '[...]'`.
- `mnesia:subscribe(system)` + log rumoroso su `{inconsistent_database, _, _}`.
- Nota da aggiungere: con `rest_for_one` ([game_engine_sup.erl:28](erlang-engine/game_engine/src/game_engine_sup.erl#L28)), `cluster_manager` è prima di `wheel_process`; un suo crash azzera il round in corso. Accettabile, ma non per la ragione più ovvia: il checkpoint Mnesia esiste **solo dopo il gong**, quindi copre il crash in fase `spinning`/`minigame`. Un crash in fase `betting` non ha checkpoint e ricade sullo stesso percorso del crash del leader — `round_cancelled` con `exclude_bet_ids` più ack differito (§Parte 3) — che è ciò che impedisce la perdita delle puntate. Le due protezioni sono complementari, nessuna delle due basta da sola.

### Fase 3 — Bully Leader Election — **modifica sostanziale**
- **`apply_role(standby)` NON deve più disattivare il `worker`** ([implementation_plan.md:1248-1253](implementation_plan.md#L1248-L1253)). L'ingestione delle bet è replicata su tutti i nodi; solo `wheel_process` resta leader-only. Rimuovere `gen_server:cast(worker, deactivate)` da `apply_role(standby)` e `gen_server:cast(worker, activate)` da `apply_role(leader)`.
- Il `worker` riceve invece `{set_leader, LeaderNode}` da `leader_election` (broadcast a tutti i nodi in `declare_victory/1`) e instrada a `{wheel_process, LeaderNode}` **tutti** i percorsi di `process_message/1`, non solo le bet: vedi il punto 4 della §`worker.erl`. È la conseguenza meno ovvia dell'ingestione distribuita e la più facile da dimenticare.
- **Guardia di quorum in `declare_victory/1` E in `handle_info({nodedown, _}, #state{role = leader})`** (§Parte 2): senza maggioranza il nodo non si dichiara leader e, se lo era già, si autoretrocede. Il secondo punto è quello che conta davvero: il leader isolato nella minoranza non passa mai da `declare_victory`. Senza questa guardia l'ingestione distribuita introdotta dalla Fase 3 rende lo split-brain distruttivo anziché fastidioso.
- La sezione «Modified Files» va aggiornata: il flag `active` del worker (riga 1320) non serve più; servono un campo `leader` e la guardia di quorum.
- Il flag `active` di `wheel_process` resta invariato.

### Fase 4 — Chandy-Lamport Snapshot — **riscritta**
Sostituire integralmente [implementation_plan.md:1330-1538](implementation_plan.md#L1330-L1538). Il contenuto è la Parte 5 di questo piano. Punti che cambiano rispetto al testo attuale:
- i marker viaggiano sui canali applicativi, emessi da `worker` e `wheel_process`, non fra istanze di `snapshot`: è la correzione che rende l'algoritmo effettivamente Chandy-Lamport;
- `snapshot.erl` è un collector, non un partecipante; `initiate_snapshot` diventa un **cast**, mai una `call`;
- lista partecipanti congelata all'avvio;
- handler mancanti aggiunti, più un abort timer **locale a ciascun partecipante** (se il collector muore, i partecipanti non restano in registrazione per sempre);
- `snapshot` va messo come **ultimo** figlio del supervisore, non prima di `wheel_process` come dice [implementation_plan.md:1533](implementation_plan.md#L1533): con `rest_for_one`, un crash del collector non deve azzerare il round.

### Fase 5 — Fault Tolerance — **modifica sostanziale**
- Sostituire «annulla il round e rimborsa tutte le PENDING» con un percorso a due rami, letto dall'ultimo `snapshot_record` su Mnesia:
  - **checkpoint presente per il round R e risultato non ancora pubblicato** → il nuovo leader **completa** il round R: ricarica `bets` dal ledger, riusa `winner_segment`/`winner_index` del taglio, calcola i payout, pubblica su `results_queue`. Nessun rimborso.
  - **nessun checkpoint per R** (crash durante la fase betting) → `round_cancelled` per il **solo** round R,
    ma **non** per tutte le sue bet: quelle ancora vive nel broker verranno rigiocate e non vanno rimborsate.
    Vedi §Parte 3 per il perché; la procedura è:
    1. in `declare_victory/1` il nuovo leader invia `{collect_inflight, R}` a tutti i worker superstiti e
       ne raccoglie le `inflight` entro 2 s: sono bet consumate ma **non ackate**, che il broker
       riconsegnerà. AMQP non ha un timeout di inflight lato broker: la riconsegna avviene per il
       `reject(Tag, true)` che il worker emette allo scadere del proprio `inflight_timeout`
       (§Parte 5, §`worker.erl` punto 7), oppure per caduta del canale. Vanno **escluse** dal rimborso;
    2. il messaggio diventa
       `{"type":"round_cancelled","round":R,"exclude_bet_ids":[...]}` — `exclude_bet_ids` è l'insieme
       raccolto al passo 1;
    3. Java rimborsa `findByRoundAndStatus(R, "PENDING")` **meno** `exclude_bet_ids`. Il `round` usato è
       quello **provvisorio** che `WalletController` assegna già oggi da `gameStateCache`
       ([WalletController.java:152,158](java-gateway/src/main/java/com/crazytime/controller/WalletController.java#L152)):
       stima locale soggetta a lag al confine di round, ammessa **solo** in questo ramo di fallback, ed è
       la ragione per cui `round` deve viaggiare esplicito nel messaggio.
- Rimuovere da [implementation_plan.md:1633](implementation_plan.md#L1633) il `findByStatus("PENDING")` globale: rimborsa bet di round estranei.
- `wheel_process:handle_cast(activate, ...)` non deve più resettare incondizionatamente a `bets = []` ([implementation_plan.md:1603-1617](implementation_plan.md#L1603-L1617)): prima consulta il checkpoint.

### Fase 6 — Integration Testing
Sostituire la riga *«Snapshot during "No more bets" → Snapshot log shows consistent state capture»* (non falsificabile: il log si stampa anche con canali vuoti) con i test della Parte 7.

### Fase 0 / Java — aggiunta
`bet_id` UUID sul messaggio AMQP e sull'entity `Bet`. È il prerequisito di UC2 e la correzione alla radice dei bug 0.1.3 / 0.1.4, che le patch attuali affrontano solo per sintomo.

---

## Parte 5 — Modifiche al codice

### [NEW] `erlang-engine/game_engine/src/cl_recorder.erl`
Modulo di **funzioni pure** (nessun processo) con la logica Chandy-Lamport lato partecipante, condivisa da `wheel_process` e `worker` per non duplicarla.

```erlang
-record(cl, {
    id      = undefined,   %% undefined = non sta registrando
    local   = undefined,   %% stato locale salvato al taglio
    in_open = [],          %% canali entranti ancora in registrazione, [{Role, Node}]
    chan    = #{}          %% #{{Role,Node} => [Msg]} messaggi in transito registrati
}).

-export([new/0, start/3, is_recording/1, on_marker/5, on_app_msg/3, close/2, is_complete/1]).
```

- `start(SnapId, LocalState, InChannels) -> NewCL` — ingresso dell'**iniziatore**, che non riceve mai un
  marker e quindi non può passare da `on_marker/5`: salva `local` e apre la registrazione su *tutti* gli
  `InChannels`. È la funzione che il wheel chiama al gong.
- `on_marker(SnapId, From, InChannels, LocalState, CL) -> {NewCL, first_marker | subsequent}` — al primo marker salva `local`, apre la registrazione su `InChannels -- [From]`; ai successivi chiude il canale `From`. Il chiamante è responsabile di inviare i propri marker uscenti (deve farlo *lui*, perché il mittente deve essere il processo applicativo).
- `on_app_msg(From, Msg, CL) -> NewCL` — accoda a `chan[From]` **solo se** sta registrando e `From ∈ in_open`.
- `close(From, CL) -> NewCL` — chiude un singolo canale entrante. Usata dal percorso di abort
  (`{cl_abort, SnapId}`) per chiudere in blocco ciò che resta aperto quando il collector non risponde.
- `is_complete(CL) -> boolean()` — `in_open == []`.

### [MODIFY] `erlang-engine/game_engine/src/rabbitmq_manager.erl`
L'ack differito ha bisogno di rifiutare messaggi, ma oggi il modulo esporta solo
`ack/1` e tiene `lookup_channel/1` privata ([rabbitmq_manager.erl:22](erlang-engine/game_engine/src/rabbitmq_manager.erl#L22),
[:298](erlang-engine/game_engine/src/rabbitmq_manager.erl#L298)). Aggiungere `reject/2`, modellata su
`ack/1` ([:84-88](erlang-engine/game_engine/src/rabbitmq_manager.erl#L84-L88)) — stesso accesso via ETS,
quindi lock-free e senza passare dal gen_server:
```erlang
-export([start_link/0, publish/2, subscribe/2, ack/1, reject/2, is_connected/0]).

reject(Tag, Requeue) when is_boolean(Requeue) ->
    case lookup_channel(cons_ch) of
        {ok, Ch} -> amqp_channel:cast(Ch, #'basic.reject'{delivery_tag = Tag,
                                                          requeue = Requeue});
        Error    -> Error
    end.
```
Il worker usa `rabbitmq_manager:reject(Tag, true)`, mai `amqp_channel` direttamente: il pid del canale
cambia a ogni riconnessione e solo il manager sa qual è quello valido.

### [MODIFY] `erlang-engine/game_engine/src/worker.erl`
1. State record: `#state{leader = undefined, cl = cl_recorder:new(), inflight = #{}}`,
   dove `inflight :: #{BetId => {DeliveryTag, BetMap}}` — serve sia allo snapshot sia all'ack differito.
2. `handle_cast({set_leader, Node})` — memorizza il leader.
3. **`process_message/1` non fa più `gen_server:call(wheel_process, ...)`** ([worker.erl:104](erlang-engine/game_engine/src/worker.erl#L104)): diventa
   `gen_server:cast({wheel_process, Leader}, {bet, BetMap})`.
   **L'ack AMQP NON è più immediato**: il `DeliveryTag` viene conservato in `inflight` e l'ack parte solo
   alla ricezione di `{bet_result, BetId, accepted | rejected}` dal wheel. Motivazione in §Parte 2.
   Se `leader = undefined` (nessun leader eletto, o nodo in minoranza dopo una partizione) la bet non
   viene inoltrata: `rabbitmq_manager:reject(Tag, true)`, così resta nel broker e verrà consegnata a un
   nodo che può servirla.
4. **Instradare al leader anche gli altri quattro percorsi di `process_message/1`.** Il punto 3
   converte il solo ramo bet, ma la stessa funzione instrada oggi **verso il `wheel_process` locale**:
   `type: "force_segment"` ([worker.erl:82](erlang-engine/game_engine/src/worker.erl#L82)) e
   `FORCE_<Seg>` ([:102](erlang-engine/game_engine/src/worker.erl#L102)),
   `type: "minigame_choice"` ([:84-86](erlang-engine/game_engine/src/worker.erl#L84-L86)),
   `segment: "UNDO_BETS"` ([:89-99](erlang-engine/game_engine/src/worker.erl#L89-L99)).
   Appena la Fase 3 lascia il `worker` attivo su tutti i nodi, un `minigame_choice` consumato da uno
   standby chiama il `wheel_process` **dormiente di quel nodo** e la scelta del giocatore sparisce in
   silenzio; con 3 nodi succede circa 2 volte su 3, perché i worker sono competing consumer. Idem per il
   pannello dev dell'admin e per l'UNDO. Tutti e quattro vanno a `{wheel_process, Leader}`.

   Dei tre, `force_segment/1` e `submit_choice/2` sono **già** `cast`
   ([wheel_process.erl:64-65](erlang-engine/game_engine/src/wheel_process.erl#L64-L65),
   [:70-71](erlang-engine/game_engine/src/wheel_process.erl#L70-L71)) e basta indirizzarli al nodo giusto.
   `undo_bets/1` no: è una `gen_server:call`
   ([wheel_process.erl:67-68](erlang-engine/game_engine/src/wheel_process.erl#L67-L68)) e va **convertita
   a `cast`**, perché come call cross-nodo ricadrebbe nel limite noto del punto 8 di
   §`wheel_process.erl` — il wheel è bloccato fino a 10 s dentro la `gen_server:call` del minigioco — e un
   UNDO che capita in fase `minigame` andrebbe in timeout facendo crollare il worker. Con l'UNDO diventato
   `cast`, il rimborso non torna più come valore di ritorno: è il wheel a pubblicarlo, vedi il punto 8.
5. **Nuovo handler marker** — sul canale entrante `wheel → worker`:
   ```erlang
   handle_info({cl_marker, SnapId, From}, S) ->
       InCh = [{wheel, S#state.leader}],
       Local = #{unacked => maps:values(S#state.inflight)},
       {CL, Kind} = cl_recorder:on_marker(SnapId, From, InCh, Local, S#state.cl),
       case Kind of
           first_marker ->
               %% marker uscente sul MIO canale applicativo verso il wheel:
               %% stesso mittente, stessa mailbox dei {bet,...} => FIFO garantito
               gen_server:cast({wheel_process, S#state.leader},
                               {cl_marker, SnapId, {worker, node()}});
           subsequent -> ok
       end,
       maybe_report(SnapId, CL, S)
   ```
   Il worker ha un solo canale entrante, quindi termina immediatamente e riporta al collector:
   `gen_server:cast({snapshot, InitiatorNode}, {cl_part, SnapId, {worker, node()}, Local, ChanStates})`.
6. `handle_cast({bet_result, BetId, Verdict})` — passa da `cl_recorder:on_app_msg/3` se in registrazione,
   poi `rabbitmq_manager:ack(Tag)` e rimuove da `inflight`. È l'unico punto in cui si acka.
7. **Timeout dell'inflight**: `{inflight_timeout, BetId}` armato a 15 s al momento del cast. Se scade
   (leader morto prima di rispondere) → `rabbitmq_manager:reject(Tag, true)`: la bet torna nel broker e
   sarà riconsegnata a un worker vivo. La perdita silenziosa sparisce.
8. **`publish_refund/1` va riscritto, non rimosso** ([worker.erl:184-195](erlang-engine/game_engine/src/worker.erl#L184-L195)).
   Attenzione: ha **due** chiamanti, non uno. Oltre al caso `betting_closed`
   ([worker.erl:109](erlang-engine/game_engine/src/worker.erl#L109)) c'è l'**UNDO**
   ([worker.erl:96](erlang-engine/game_engine/src/worker.erl#L96)), che pubblica un totale **aggregato su
   più bet** con `segment = <<"REFUND">>`. Eliminando `refunds_queue` senza sostituire quel percorso,
   l'UNDO smette di accreditare il saldo: il giocatore annulla, le bet spariscono dal wheel, i soldi non
   tornano. È una regressione funzionale silenziosa, non un dettaglio.
   Rimedio, nella stessa direzione del resto del piano: `undo_bets/1` restituisce la **lista dei `bet_id`
   annullati** invece del totale, e il wheel emette un `bet_rejected` per ciascuno con
   `"reason":"undo"`. L'handler Java del punto 6 di §Java lo gestisce già senza modifiche.
   Quanto al caso `betting_closed`: delegare *tutti* i rimborsi al ledger lascerebbe scoperte le bet rifiutate **dopo** la pubblicazione
   del ledger: una puntata in ritardo che arriva durante `spinning` riceve `rejected`, il worker acka e
   la scarta, ma il ledger del round R è già stato pubblicato al gong e `PayoutListener` interroga per
   round — la bet resterebbe `PENDING` per sempre con il saldo scalato. Serve quindi un evento esplicito
   e puntuale, che è ciò che `publish_refund/1` già faceva; ciò che va eliminato è il **match per
   importo** lato Java, non l'evento. Nuova forma, pubblicata dal **wheel** (unico punto di decisione,
   e conosce il round autoritativo) su `results_queue`:
   ```json
   {"type":"bet_rejected","bet_id":"<uuid>","round":R,"reason":"betting_closed"}
   ```
   La coda `refunds_queue` non viene più usata da Erlang.
9. `parse_bet_json/1` — estrarre anche `<<"bet_id">>`. I comandi `force_segment` e `minigame_choice` non
   ne hanno uno: `bet_id = undefined` deve saltare sia la dedup di R3 sia la mappa `inflight`, e questi
   messaggi restano ad **ack immediato** — l'ack differito riguarda solo le bet.

### [MODIFY] `erlang-engine/game_engine/src/wheel_process.erl`
1. **Prerequisito** — promuovere lo stato del round nel record (oggi vive solo dentro i messaggi `send_after`):
   ```erlang
   -record(state, {
       phase, time_left, round, bets, forced_segment, history, minigame_choices,
       active = false,
       winner_segment = undefined,   %% NEW
       winner_index   = undefined,   %% NEW
       minigame_mod   = undefined,   %% NEW
       minigame_details = undefined, %% NEW
       timer_ref      = undefined,   %% NEW  (send_after cancellabile/ispezionabile)
       settled_bet_ids = sets:new(), %% NEW  bet_id degli ultimi 3 round — regola R3
       cl = cl_recorder:new()        %% NEW
   }).
   ```
   Ogni `erlang:send_after` di fase salva la ref in `timer_ref`. Senza questo, nessun recovery è possibile: oggi il segmento vincente esiste solo dentro un messaggio in volo.
   `settled_bet_ids` va ripopolato dai `snapshot_record` all'avvio **e a ogni elezione**: è subito dopo un
   crash che le riconsegne arrivano, quindi un nuovo leader con il set vuoto è esattamente il caso in cui
   R3 serve di più.
2. `handle_cast({bet, BetMap}, S)` — sostituisce `handle_call({place_bet, ...})`.
   **Deduplicare per `bet_id` prima di tutto** (regola R3, §Parte 3): con l'ack differito una riconsegna
   del broker può ripresentare una bet già accettata (ack perso). Se `BetId` è già in `bets` **oppure in
   `settled_bet_ids`** → rispondere `{bet_result, BetId, accepted}` senza inserirla di nuovo. È l'`bet_id`
   UUID a rendere l'operazione idempotente: senza di esso l'ack differito introdurrebbe duplicati.
   Il secondo termine del test non è pleonastico: `bets` è azzerato a ogni round, quindi da solo copre la
   riconsegna intra-round ma **non** quella che arriva nel round successivo, che è il caso frequente
   perché nasce da un crash. Vedi R3 per il percorso completo.
   Altrimenti:
   - `phase = betting, active = true` → accetta, `cast` di `{bet_result, BetId, accepted}` al worker mittente;
   - **in registrazione e canale aperto** → `cl_recorder:on_app_msg/3` e **basta**: la bet finisce nello stato del canale e verrà unita a `bets` alla chiusura del taglio. Non va aggiunta anche a `bets` qui, altrimenti si conta due volte;
   - altrimenti → `{bet_result, BetId, rejected}`.
   Mantenere `handle_call({place_bet,...})` come alias deprecato non serve: nessun altro chiamante.
3. **Trigger del taglio** in `handle_info(tick, #state{phase = betting, time_left = 1})` ([wheel_process.erl:142-183](erlang-engine/game_engine/src/wheel_process.erl#L142-L183)) — **prima** estrarre il vincitore, **poi** iniziare:
   ```erlang
   %% ... estrazione WinnerIndex / WinnerSeg come oggi ...
   Participants = cluster_manager:get_participants(),          %% CONGELATA qui, una volta sola
   %% SnapId calcolato in loco, NON restituito da begin_snapshot: quello e' un cast
   %% e ritorna ok, mentre lo SnapId serve subito per marcare i marker uscenti.
   SnapId = {S#state.round, node()},
   snapshot:begin_snapshot(SnapId, Participants),              %% cast, non call
   Local = #{round => Round, bets => Bets, phase => spinning,
             winner_segment => WinnerSeg, winner_index => WinnerIndex},
   InCh  = [{worker, N} || N <- Participants],
   CL1   = cl_recorder:start(SnapId, Local, InCh),
   [gen_server:cast({worker, N}, {cl_marker, SnapId, {wheel, node()}}) || N <- Participants],
   erlang:send_after(10000, self(), {cl_abort, SnapId}),       %% abort locale se il collector muore
   ```
4. `handle_info({cl_marker, SnapId, {worker, N}}, S)` — chiude il canale `{worker,N}`; se `cl_recorder:is_complete/1`:
   - `Bets' = Bets ++ lists:append(maps:values(Chan))` — **le bet in transito entrano nel round** (sono state spedite prima che il worker apprendesse del taglio: per il taglio causale appartengono al round R);
   - `[gen_server:cast({worker,N}, {bet_result, BetId, accepted}) || ...]` per ciascuna;
   - report al collector: `gen_server:cast({snapshot, node()}, {cl_part, SnapId, {wheel, node()}, Local, Chan})`.
5. `handle_info({cl_abort, SnapId}, S)` — chiude forzatamente la registrazione, riporta ciò che ha, logga `degraded`.
6. `build_result_json/7` ([wheel_process.erl:358-367](erlang-engine/game_engine/src/wheel_process.erl#L358-L367)) — aggiungere `bet_id` a ogni entry dell'array `payouts`.
7. **Ogni `{bet_result, BetId, rejected}` va accompagnato da un `bet_rejected` su `results_queue`**
   (formato in §`worker.erl` punto 8). È il solo percorso che chiude le bet arrivate dopo la pubblicazione
   del ledger; senza, restano `PENDING` a saldo scalato. `publish_to_queue/2` è lock-free (ETS +
   `amqp_channel:cast`), quindi non blocca il wheel.
8. **Non toccare** la `gen_server:call(Module, {play, BonusBets}, 10000)` verso i mini-game: fuori scope per la scelta fatta. Va però **documentato come limite noto**: durante il minigioco il wheel è bloccato fino a 10 s e non risponde ad alcuna `call`. Le conseguenze sono tre, e vanno scritte tutte: (a) il wheel non può partecipare a uno snapshot in quella finestra — innocuo oggi, perché l'unico trigger è al gong in fase `betting`, ma è ciò che impedisce di aggiungere in futuro il trigger «transizione di fase» di UC1; (b) è la ragione per cui `undo_bets/1` va convertita da `call` a `cast` (§`worker.erl` punto 4); (c) è la ragione per cui l'`inflight_timeout` del worker non può scendere sotto i ~12 s.
9. **Spostare `escape_json_string/1` qui da [worker.erl:178-181](erlang-engine/game_engine/src/worker.erl#L178-L181)**, dove resterebbe senza chiamanti dopo la riscrittura di `publish_refund/1`. Non è solo igiene: [wheel_process.erl:359-360](erlang-engine/game_engine/src/wheel_process.erl#L359-L360) costruisce oggi le entry di `payouts` con `~s` sull'username **senza escaping** — lo stesso difetto che il punto 2 di §Java corregge lato gateway. Poiché `build_result_json/7` va comunque toccata per aggiungere `bet_id`, costa una riga infilarcelo.

### [NEW] `erlang-engine/game_engine/src/snapshot.erl` — collector
```erlang
-record(state, {
    running = #{}   %% #{SnapId => #run{round, participants, expected, parts, timer, degraded}}
}).                 %% SnapId = {Round, node()}: nessun contatore locale da tenere in sync
-export([start_link/0, begin_snapshot/2, get_last/0, get_for_round/1]).
```
- `begin_snapshot(SnapId, Participants)` — **cast**; `SnapId = {Round, node()}` arriva già calcolato dal wheel (vedi §`wheel_process.erl` punto 3: un cast non può restituirlo). Registra `expected = [{wheel, Leader} | [{worker,N} || N <- Participants]]`, arma `send_after(5000, {snapshot_timeout, SnapId})`.
- `handle_cast({cl_part, SnapId, Who, Local, Chan}, S)` — accumula; quando `parts == expected` chiama `finalize/2`.
- `handle_info({snapshot_timeout, SnapId}, S)` — `finalize/2` con `degraded = true` e l'elenco dei partecipanti mancanti. Il ledger resta deterministico: bet locali del leader + transiti effettivamente registrati.
- `finalize/2`:
  1. compone `#snapshot_record{}`;
  2. `mnesia:transaction(fun() -> mnesia:write(Rec) end)` → replica automatica su tutti i nodi;
  3. **solo sul leader**: `rabbitmq_manager:publish(<<"results_queue">>, LedgerJson)`;
  4. log con conteggi separati: `local_bets`, `in_flight_bets`, `degraded`.

### [NEW] tabella Mnesia `snapshot_record`
```erlang
-record(snapshot_record, {
    id,              %% chiave = {Round, Initiator} — vedi §Parte 2: due leader concorrenti
                     %% producono record distinti e diagnosticabili invece di sovrascriversi
    round,
    taken_at,        %% erlang:system_time(millisecond)
    initiator,       %% node()
    degraded,        %% boolean()
    phase,           %% fase al taglio
    winner_segment, winner_index,
    local_states,    %% #{Participant => term()}
    channel_states,  %% #{{From,To} => [term()]}
    ledger           %% [BetMap] insieme autorevole delle bet del round
}).
```
Tabella di tipo `ordered_set`, così `mnesia:dirty_last/1` restituisce l'ultimo round e
`mnesia:dirty_read({Round, Node})` il record di un round specifico.
`local_states` + `channel_states` sono conservati integralmente: senza di essi il post-mortem non può dire cosa fosse in volo (UC3).

### [MODIFY] `game_engine_sup.erl`
```
rabbitmq_manager → cluster_manager → leader_election → wheel_process
  → minigames_sup → worker → snapshot      ← ULTIMO
```

### [MODIFY] Java — `bet_id` e riconciliazione per ledger
1. **`entity/Bet.java`** — nuovo campo `@Column(unique = true) String betId`. `ddl-auto=update` ([application.properties:13](java-gateway/src/main/resources/application.properties#L13)) crea la colonna senza migrazione manuale.
2. **`WalletController.java:161-165`** — generare `UUID.randomUUID().toString()`, persisterlo sulla `Bet` e includerlo nel JSON. Sostituire la concatenazione di stringhe con Jackson (l'username oggi non è escapato).
3. **`BetRepository.java`** — aggiungere `findByBetId(String)`, `findByRoundAndStatus(Integer, String)`.
4. **`GameResultListener.java:38-44`** — **dispatchare sul campo `type`**, oggi completamente ignorato: qualunque messaggio su `results_queue` viene trattato come risultato di round. Instradare `result` → `PayoutListener`, `round_ledger` → nuovo `LedgerListener`, `round_cancelled` → cancellazione per-round, `bet_rejected` → rimborso puntuale per `bet_id`.
5. **[NEW] `rabbitmq/LedgerListener.java`** — `@Transactional`, implementa R1 e R2 della §Parte 3:
   - per ogni `bet_id` nel ledger con `Bet` `PENDING`: setta `Bet.round = R`, lascia `PENDING`;
   - per ogni `bet_id` nel ledger con `Bet` già `REFUNDED` (caso R2): **non** riaprire, **non** pagare;
     loggare `replay_after_refund` e chiudere in stato terminale;
   - per ogni `Bet` `PENDING` con `round = R` **assente** dal ledger: `REFUNDED` + accredito saldo,
     idempotente sul `betId`.
6. **[NEW] handler `bet_rejected`** — `@Transactional`: `findByBetId(id)`, e **solo se** `PENDING` →
   `REFUNDED` + accredito. L'idempotenza è la guardia sullo stato, non serve altro: una riconsegna del
   messaggio trova la bet già `REFUNDED` e non fa nulla. È la sostituzione deterministica del match per
   importo di `RefundListener`.
7. **handler `round_cancelled`** — rimborsa `findByRoundAndStatus(R,"PENDING")` **meno**
   `exclude_bet_ids` (regola R1).
8. **`PayoutListener.java:53`** — `findByRoundAndStatus(round, "PENDING")` al posto del `findByStatus("PENDING")` globale; match per `bet_id` invece che per `username` ([PayoutListener.java:64](java-gateway/src/main/java/com/crazytime/rabbitmq/PayoutListener.java#L64)); rimuovere `it.remove()`.
9. **`RefundListener.java`** — **rimuovere** classe e coda: la sua funzione è assorbita dall'handler
   `bet_rejected`, che identifica la bet per `bet_id` invece che per importo
   ([RefundListener.java:65-76](java-gateway/src/main/java/com/crazytime/rabbitmq/RefundListener.java#L65-L76)).
   Prerequisito: il percorso UNDO deve già passare da `bet_rejected` (§`worker.erl` punto 8), altrimenti
   la rimozione della coda rompe l'annullamento delle puntate.
   Togliere `refunds_queue` da `?QUEUES` ([rabbitmq_manager.erl:30](erlang-engine/game_engine/src/rabbitmq_manager.erl#L30)) e il bean
   `refundsQueue` da [GatewayApplication.java:26-29](java-gateway/src/main/java/com/crazytime/GatewayApplication.java#L26-L29).
10. **Rimuovere** il `catch` che inghiotte le eccezioni in `PayoutListener:107-109` e `RefundListener:77-79`: essendo i metodi `@Transactional`, l'eccezione catturata non provoca rollback e i `save` parziali vengono committati.

### [MODIFY] `app.js`
Gestire `type === 'round_cancelled'` sul WebSocket (notifica + `fetchBalance()`), come già previsto dalla Fase 5.

---

## Parte 6 — Come risponde alla specifica

| Requisito (`idea_distributed.md` §1.2) | Copertura |
| :--- | :--- |
| «Snapshot per catturare lo stato globale consistente delle bet accettate sui **worker nodes**» | Ora letterale: N worker su nodi distinti, bet realmente in transito catturate negli stati dei canali. |
| «Garantire che nessuna puntata ulteriore venga processata» | Imposto dalla guardia di fase; lo snapshot **certifica** il taglio e ne produce l'artefatto verificabile. Il limite va dichiarato apertamente nella relazione: CL dà consistenza causale, non una barriera temporale. |
| «Eleggere un nuovo Dealer e riprendere il gioco senza corruzione di stato» | Il checkpoint su Mnesia permette di **completare** il round anziché annullarlo. Il rimborso resta come fallback, esplicitamente permesso dalla specifica. |
| «Migliaia di richieste concorrenti» | Competing consumers su `bets_queue` distribuiti su N nodi. |
| Mnesia nel diagramma architetturale (§1.3.1) | Finalmente usata. |

---

## Parte 7 — Verifica

**Compilazione e unit test**
```bash
cd erlang-engine/game_engine && ../../rebar3 compile && ../../rebar3 eunit
cd java-gateway && mvn test
```

**Bootstrap Mnesia** — avviare i nodi **uno alla volta** e verificare che il secondo e il terzo passino
dal ramo `add_table_copy`; poi riavviare un nodo secondario e controllare che conservi la propria copia
su disco (`mnesia:table_info(snapshot_record, disc_copies)` deve elencarlo). Se non lo elenca, manca la
`change_table_copy_type(schema, ...)`.

**Test unitari nuovi su `cl_recorder`** (funzioni pure, banali da testare): primo marker apre i canali giusti; marker successivo chiude solo il proprio; `on_app_msg` accoda solo sui canali aperti; `is_complete` scatta esattamente quando tutti i canali sono chiusi.

**Precondizione già soddisfatta — `basic_qos`.** Il test «Canali non vuoti» richiede che le bet si
distribuiscano davvero fra i worker. Il `prefetch_count` **è già impostato** dalla Fase 1:
`amqp_channel:call(ConsCh, #'basic.qos'{prefetch_count = maps:get(prefetch, Cfg)})`
([rabbitmq_manager.erl:218-219](erlang-engine/game_engine/src/rabbitmq_manager.erl#L218-L219)), valore 10,
configurabile per nodo da [game_engine.app.src:21](erlang-engine/game_engine/src/game_engine.app.src#L21).
Non serve aggiungerlo. Va però **abbassato a 1 durante il test di distribuzione**: con 10 un burst breve
può finire quasi tutto sul primo consumer con credito disponibile e far sembrare rotto un refactoring
corretto. In esercizio si torna a 10.

**End-to-end, 3 nodi.** Avvio con `-sname game{1,2,3}@localhost -setcookie crazytime`, RabbitMQ attivo, gateway Java su :8080.

| Test | Risultato atteso |
| :--- | :--- |
| Bet piazzate da 3 browser durante `betting` | Distribuite sui 3 worker (visibile nei log per-nodo), tutte accettate dal wheel del leader |
| **Comandi non-bet consumati da uno standby** | Ripetere `minigame_choice`, `UNDO_BETS` e `force_segment` finché i log mostrano che li ha presi un worker **non** sul leader (con 3 nodi capita ~2 volte su 3). Attesa: effetto identico a quando li prende il leader. Se una scelta di minigioco sparisce in silenzio, l'instradamento del punto 4 non è stato fatto |
| **UNDO durante `minigame`** | Il worker **non** crasha e non va in timeout: `undo_bets` è ora un `cast`, non una `call` cross-nodo verso un wheel bloccato fino a 10 s |
| **UNDO, saldo accreditato** | Un `bet_rejected` con `"reason":"undo"` per **ogni** `bet_id` annullato, saldo riaccreditato per intero. È il test che protegge dalla regressione introdotta dalla rimozione di `refunds_queue` |
| **Canali non vuoti** — piazzare bet negli ultimi 200 ms della fase betting | Il log dello snapshot mostra `in_flight_bets > 0` su almeno un canale. **È il test che falsifica la vacuità denunciata dall'analisi**: se resta sempre 0, la rifattorizzazione non ha prodotto canali reali. Forzabile deterministicamente con un `timer:sleep/1` iniettato nel worker sotto flag di debug |
| Ledger su `results_queue` | `type:"round_ledger"` con `bet_id` di **tutte** le bet, incluse quelle in transito |
| Bet in transito | Presente nel ledger **e** pagata correttamente; nessun refund |
| Bet piazzata dopo la chiusura del taglio | `rejected`, assente dal ledger, `REFUNDED` da `LedgerListener`, **mai** `WON` |
| Doppio importo, segmenti diversi | Nessuno scambio di attribuzione: verifica diretta del match per importo di `RefundListener` |
| **Kill del leader dopo il gong, prima del payout** | Nuovo leader eletto, legge `snapshot_record` da Mnesia, **completa** il round col vincitore del taglio. Nessun rimborso, nessun round perso |
| Kill del leader durante `betting` | Nessun checkpoint per R → `round_cancelled` con i soli `bet_id` del round R; bet di round precedenti **non** toccate |
| Kill di un worker standby durante lo snapshot | Snapshot completato in `degraded` entro 5 s, ledger comunque pubblicato e deterministico |
| Kill del collector `snapshot` durante il taglio | `{cl_abort, _}` scatta sui partecipanti entro 10 s; nessun processo resta in registrazione |
| **Kill del leader in fase `betting`** (finestra di perdita) | Le bet non ackate sono riconsegnate dal broker a un worker vivo ed entrano nel round successivo. **Nessuna bet persa, nessun addebito senza scommessa**. Verificare che `Σ wallet` non cali |
| **Rimborso + replay della stessa bet** (regola R1) | Nessuna bet del round R è contemporaneamente `REFUNDED` in Java e presente nel ledger di R+1. È il test che falsifica la contraddizione fra ack differito e `round_cancelled`: se fallisce, `exclude_bet_ids` non sta funzionando |
| **Replay dopo rimborso** (regola R2, caso residuo) | Uccidere leader **e** un worker insieme: la bet di quel worker non finisce in `exclude_bet_ids`, viene rimborsata, poi riappare nel ledger di R+1. Attesa: log `replay_after_refund`, bet **non** pagata, `Σ wallet` invariato |
| **Bet in ritardo durante `spinning`** (dopo la pubblicazione del ledger) | `bet_rejected` su `results_queue`, bet `REFUNDED` entro pochi secondi. **Nessuna bet deve restare `PENDING` a fine sessione**: query di controllo `SELECT * FROM bets WHERE status='PENDING'` a gioco fermo deve tornare vuota |
| Riconsegna del messaggio `bet_rejected` | La bet è già `REFUNDED`: nessun secondo accredito |
| Riconsegna di una bet già accettata, **stesso round** (ack perso) | Il wheel deduplica per `bet_id` su `bets`, riacka, **non** la inserisce due volte: il ledger contiene una sola riga |
| **Riconsegna cross-round** (regola R3) | Uccidere il worker **dopo** che il wheel ha accettato la bet e pubblicato il ledger di R, ma prima dell'ack. Il broker la riconsegna nel round R+1. Attesa: il wheel la trova in `settled_bet_ids`, risponde `accepted` **senza rigiocarla**, il ledger di R+1 **non** la contiene e `Σ wallet` è invariato. È il test che falsifica R3: se la bet compare in due ledger, la dedup sta guardando solo `bets` |
| **Partizione di rete 2-1, isolando il LEADER in carica** | È il caso che `declare_victory` non copre: il vecchio leader deve autoretrocedersi da `handle_info({nodedown,_})` entro il tempo di rilevazione. Un solo ledger pubblicato. Se il vecchio leader continua a pubblicare, la guardia è nel posto sbagliato |
| Partizione di rete 2-1, isolando uno standby | Il lato di minoranza non elegge un leader; i suoi worker fanno `reject{requeue=true}` e le bet vengono servite dalla maggioranza |
| Riconnessione dopo la partizione | **Non** attendersi l'assenza di `inconsistent_database`: Mnesia emette `{inconsistent_database, running_partitioned_network, _}` alla rilevazione della **partizione**, non della divergenza dei dati, e tutti i nodi hanno una replica di `snapshot_record` quindi Mnesia attiva. L'evento è normale e va solo loggato. Ciò che si verifica è l'assenza di **divergenza**: esattamente una scrittura di `snapshot_record` per round, ed esattamente un `type:"round_ledger"` su `results_queue` per round. Due ledger per lo stesso round = split-brain non contenuto. Con `disc_copies` senza opzione `majority` la riparazione resta manuale se la divergenza si verifica davvero: il quorum la rende improbabile, non impossibile |
| Invariante di conservazione | Su ogni `snapshot_record`: `Σ wallet + Σ bet_bloccate + Σ payout_in_volo` costante fra snapshot consecutivi |

**Ispezione Mnesia** — da una shell su un nodo standby, `mnesia:dirty_last(snapshot_record)` deve mostrare lo stesso record scritto dal leader: prova diretta della replica del checkpoint.

> **Attenzione durante i test di partizione:** una shell distribuita attaccata al cluster compare in
> `nodes()`. Se `has_quorum/0` non intersecasse con `configured_nodes/0` (§Parte 2), la shell regalerebbe
> il quorum al lato di minoranza proprio nel test che deve dimostrare il contrario. Usare `-hidden`, o
> restare fuori dal cluster e passare da `rpc:call/4`.

---

## Parte 8 — Ordine di esecuzione

1. **Prerequisiti** (indipendenti, nessuno dipende dallo snapshot): stato del round nel record di `wheel_process`; `bet_id` UUID lato Java; `rabbitmq_manager:reject/2`; `worker → wheel` da `call` a `cast` con `{bet_result, ...}` di ritorno, **ack differito** + dedup per `bet_id` (§Parte 2 e R3 di §Parte 3), ed evento `bet_rejected` con il relativo handler Java, **incluso il percorso UNDO** (`undo_bets/1` restituisce i `bet_id`, `reason:"undo"`). L'ordine interno conta: `bet_id` prima dell'ack differito (altrimenti si introducono duplicati); `reject/2` prima di tutto il resto (senza, il worker non ha modo di rimettere una bet in coda); il percorso UNDO su `bet_rejected` prima della rimozione di `refunds_queue` al passo 5 (altrimenti l'annullamento smette di rimborsare).
2. **Fase 2 estesa**: bootstrap Mnesia + `cluster_manager:get_participants/0`.
3. **Fase 3 corretta**: worker attivo su tutti i nodi, `{set_leader, N}` e instradamento al leader di **tutti** i percorsi di `process_message/1` — bet, `force_segment`, `minigame_choice`, `UNDO_BETS` — con `undo_bets`/`submit_choice` convertite a `cast`; **guardia di quorum** con numeratore intersecato a `configured_nodes/0`. Il quorum va prima della Fase 4: senza, gli `snapshot_record` concorrenti di due leader corrompono Mnesia al primo test di partizione.
4. **Fase 4 riscritta**: `cl_recorder` → partecipanti → collector `snapshot.erl` → persistenza Mnesia → pubblicazione ledger. Con Mnesia in piedi si chiude anche **R3**: `settled_bet_ids` ripopolato dai `snapshot_record` all'avvio e a ogni elezione. Fino a qui la dedup copre solo l'intra-round; è l'unico punto del percorso in cui il sistema è temporaneamente esposto alla doppia giocata, ed è per questo che R3 non può slittare oltre.
5. **UC2 lato Java**: dispatch su `type`, `LedgerListener` con le regole R1/R2, `PayoutListener` per-round, rimozione di `RefundListener` e di `refunds_queue` (solo dopo il passo 1).
6. **Fase 5 corretta**: recovery dal checkpoint; fallback `round_cancelled` con `exclude_bet_ids`, che dipende dal `{collect_inflight, R}` introdotto al passo 1.
7. **Fase 6**: test della Parte 7.

I passi 1-4 lasciano il sistema funzionante a ogni tappa. Il passo 5 è quello che rende lo snapshot **consumato**: fino ad allora resta un artefatto di log, cioè esattamente la critica dell'analisi.