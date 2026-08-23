# Snapshot Implementation Plan — Chandy-Lamport in Distributed Crazy Time

## Context

`snapshot_analisi.md` conclude che lo snapshot previsto dalla Fase 4 di `implementation_plan.md` è ridondante: cattura uno stato già disponibile in locale sul leader, su canali vuoti per costruzione, e nessuno ne consuma il risultato. La tesi è stata verificata contro il codice: **è corretta**.

Questo piano definisce le modifiche al codice e a `implementation_plan.md` per rendere lo snapshot **load-bearing**: un algoritmo il cui risultato non è ottenibile in altro modo e che alimenta due decisioni reali del sistema (ledger del round verso Java, recovery al crash del dealer).

**Scelte già prese** (rispondendo alle domande poste):
- ingestione bet distribuita: tutti i nodi consumano da `bets_queue` come competing consumers e inoltrano **asincroni** al wheel del leader;
- `bet_id` UUID generato da Java + `round` autoritativo assegnato da Erlang;
- persistenza degli snapshot su **Mnesia replicata** (colma anche il gap col diagramma architetturale della specifica, dove Mnesia c'è ma non è mai stata usata).

---

## Stato del codice — baseline di questo piano

> [!IMPORTANT]
> **Questo documento è stato riscritto sul codice reale al commit `6ef3b73`, e poi aggiornato dopo il retrofit.** Gli Step 1-12 di [ordine_implementazione.md](ordine_implementazione.md) — Blocchi **A, B, C e D** — sono stati **implementati e verificati** su cluster a 3 nodi con broker e gateway attivi: i punti relativi sono marcati ✅ qui sotto. La prima stesura era ancorata al codice della sola **Fase 1** (AMQP nativo). Nel frattempo sono state implementate anche la **Fase 2** (`cluster_manager.erl`) e la **Fase 3** (`leader_election.erl`) **seguendo la stesura originale** di `implementation_plan.md`, cioè senza applicare prima le correzioni strutturali che questo piano richiedeva. Parte del lavoro delle Fasi 2-3 va quindi **corretta**, non aggiunta: i punti interessati sono marcati inline con 🔧.

| Componente | Stato |
| :--- | :--- |
| [rabbitmq_manager.erl](erlang-engine/game_engine/src/rabbitmq_manager.erl) | ✅ Fase 1 + `refunds_queue` rimossa da `?QUEUES`. `publish/2` lock-free via ETS, `subscribe/2` col PID del consumer, `ack/1`, `reject/2` ([:91-95](erlang-engine/game_engine/src/rabbitmq_manager.erl#L91-L95)), riconnessione automatica |
| [cluster_manager.erl](erlang-engine/game_engine/src/cluster_manager.erl) | ✅ Completo. Discovery, `monitor_nodes(true, [{node_type, all}])`, reconnect, **`configured_nodes/0` e `get_participants/0`**, delega a `leader_election:node_down/1`, **bootstrap Mnesia** a due rami, `subscribe(system)`, `force_load_snapshots/0` |
| [leader_election.erl](erlang-engine/game_engine/src/leader_election.erl) | ✅ Fase 3 + retrofit. Bully completo, `apply_role/1` che tocca **solo** il wheel, `broadcast_leader/1` con `{set_leader, N}`, **guardia di quorum** in `declare_victory/1` e `node_down/1`, guardia su `election_in_progress` |
| [worker.erl](erlang-engine/game_engine/src/worker.erl) | ✅ Completo: consumer su **ogni** nodo, instradamento al leader, **ack differito** con `inflight` + timeout, **partecipante al taglio** (marker e riporto al collector) |
| [wheel_process.erl](erlang-engine/game_engine/src/wheel_process.erl) | ✅ Iniziatore del taglio: trigger al gong dopo l'estrazione, marker verso i worker, chiusura con merge delle bet in transito, abort locale, `settled_bet_ids` (R3), `payouts` con `bet_id`. ❌ resta `complete_round` (recovery) |
| [game_engine.app.src](erlang-engine/game_engine/src/game_engine.app.src) | ✅ `amqp_client` e **`mnesia`** fra le `applications`, `snapshot` fra i `registered`, config broker, `peer_nodes` con tutti e 3 i nodi |
| Java gateway | ✅ `bet_id` UUID, dispatch sul campo `type`, `BetRejectionHandler` idempotente, **`LedgerListener` con R1/R2**, `PayoutListener` per round e per `bet_id`, `RefundListener` rimosso. ❌ resta il ramo `round_cancelled` |

### Divergenze fra questo piano e il codice attuale

| # | Punto del piano (prima stesura) | Stato nel codice | Azione |
|---|---|---|---|
| 1 | Aggiungere `rabbitmq_manager:reject/2` | ✅ **già fatto** ([:91-95](erlang-engine/game_engine/src/rabbitmq_manager.erl#L91-L95)) | Prerequisito chiuso. Resta solo da usarla nel percorso giusto (ack differito), non nel ramo standby |
| 2 | Far consumare `bets_queue` a tutti i nodi | ✅ **già fatto**: `subscribe` avviene in `init/1` su ogni nodo ([worker.erl](erlang-engine/game_engine/src/worker.erl)) | La topologia competing-consumer esiste già a livello AMQP: il retrofit della Fase 3 è più piccolo del previsto |
| 3 ✅ | `apply_role(standby)` non deve disattivare il worker | 🔧 **il contrario**: `apply_role/1` casta `activate`/`deactivate` al worker ([leader_election.erl](erlang-engine/game_engine/src/leader_election.erl)) | Rimuovere i due cast verso `worker` e sostituirli con `{set_leader, Node}` |
| 4 ✅ | Il worker inoltra al leader o rifiuta se non c'è leader | 🔧 il worker in standby fa `reject(Tag, true)` su **ogni** delivery ([worker.erl](erlang-engine/game_engine/src/worker.erl)) | È un **loop caldo di requeue**: il messaggio rimbalza fra broker e standby finché non capita sul leader. La condizione diventa `leader =:= undefined` |
| 5 ✅ | Nuova chiave `env` `{nodes, [...]}` come denominatore del quorum | 🔧 non serve: `peer_nodes` esiste già e contiene **tutti e 3** i nodi ([app.src:27](erlang-engine/game_engine/src/game_engine.app.src#L27)) | `configured_nodes/0` legge `peer_nodes`. Attenzione: `cluster_manager` filtra se stesso in `init/1` ([cluster_manager.erl](erlang-engine/game_engine/src/cluster_manager.erl)), quindi la funzione deve **ri-aggiungere** `node()` |
| 6 ✅ | Guardia di quorum su `nodedown` dentro `leader_election` | 🔧 `nodedown` è rilevato da `cluster_manager` ([cluster_manager.erl](erlang-engine/game_engine/src/cluster_manager.erl)), non da `leader_election` | La guardia si aggancia lì: `cluster_manager` chiama `leader_election:node_down/1`, che demota se il quorum è perso |
| 7 ✅ | «Il worker consuma, acka, poi inoltra al wheel» | La finestra di perdita non si è mai aperta: il canale è passato a `cast` **insieme** all'ack differito | L'ack parte solo su `{bet_result, …}`; allo scadere di 15 s senza esito la scommessa torna al broker |
| 8 | Persistenza su Mnesia | 🔧 `mnesia` non è fra le `applications` ([app.src:7-12](erlang-engine/game_engine/src/game_engine.app.src#L7-L12)) | Aggiungerla, altrimenti l'avvio dipende da un `mnesia:start/0` implicito |
| 9 | «Modifiche a `implementation_plan.md` Fasi 2-3» | 🔧 le Fasi 2-3 sono **codice scritto**, non più carta | La §Parte 4 si divide in *correzioni al codice esistente* (A) e *modifiche ancora solo documentali* (B) |

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

> [!NOTE]
> Metà di questa topologia **esiste già**: `worker.erl` si sottoscrive a `bets_queue` in `init/1` ([worker.erl](erlang-engine/game_engine/src/worker.erl)) su ogni nodo, indipendentemente dal ruolo. Ciò che manca è che lo standby *serva* davvero i messaggi invece di rimbalzarli al broker ([worker.erl](erlang-engine/game_engine/src/worker.erl)), e che il canale verso il wheel sia asincrono e **cross-nodo**.

**Partecipanti allo snapshot** = `wheel_process` e gli N `worker`. **Non** il modulo `snapshot`, che diventa un puro **collector**: assegna l'id, congela la lista dei partecipanti, arma il timeout, raccoglie le porzioni, persiste su Mnesia, pubblica il ledger. Il collector non chiama mai i partecipanti — sono loro a fare push. Questo elimina l'accoppiamento fragile segnalato dall'analisi.

**Costo in latenza: zero.** Lo snapshot parte al gong e ha un budget di 5 s; la risoluzione del round è già schedulata a 10,5 s per l'animazione della ruota ([wheel_process.erl](erlang-engine/game_engine/src/wheel_process.erl) per il ramo moltiplicatore, [wheel_process.erl](erlang-engine/game_engine/src/wheel_process.erl) per il ramo minigioco). Il taglio è chiuso molto prima che serva.

**Ordine nel tick del gong** (importante per il recovery): si estrae **prima** il segmento vincente ([wheel_process.erl](erlang-engine/game_engine/src/wheel_process.erl)), **poi** si inizia lo snapshot. Così il taglio cattura `{bets, winner_segment, winner_index}` insieme, e un nuovo leader eletto dopo il crash ha sia l'insieme autorevole delle puntate sia l'esito autorevole: può **completare** il round anziché annullarlo.

---

## Parte 2 — Durabilità dell'ingestione e quorum

Due proprietà che l'architettura della Parte 1 **non** garantisce da sola. Vanno risolte insieme allo snapshot, perché senza di esse il ledger e l'audit trail perdono valore probatorio.

### La finestra di perdita che il passaggio ad asincrono aprirebbe

> 🔧 **Riformulato rispetto alla prima stesura.** Nel codice attuale la finestra **non esiste**: dopo il retrofit il worker inoltra con una `gen_server:call` sincrona al wheel del **leader** (con timeout esplicito di 15 s) ([worker.erl](erlang-engine/game_engine/src/worker.erl)) e acka solo dopo che `process_message/1` è ritornato ([worker.erl](erlang-engine/game_engine/src/worker.erl)). L'ack è quindi già oggi la conferma che il wheel *ha deciso*. È il passaggio a `cast` cross-nodo richiesto dalla Parte 1 che aprirebbe la finestra — e l'ack differito è ciò che la chiude **prima** che si apra.

Con il `cast`, se l'ack restasse dov'è, fra l'ack e l'arrivo del messaggio al leader la bet **non esisterebbe in nessuno stato replicato**: è uscita dal broker, il wallet è già stato addebitato ([WalletController.java:148-149](java-gateway/src/main/java/com/crazytime/controller/WalletController.java#L148-L149)), e la mappa `inflight` del worker è locale. Se il leader crasha in fase `betting` non esiste alcun checkpoint e la bet è persa in silenzio: il giocatore ha pagato, il sistema non sa che esiste.

**Rimedio: differire l'ack fino a `{bet_result, BetId, _}`.** L'ack conserva il significato che ha oggi — «il wheel ha deciso» — anche dopo che il canale è diventato asincrono. Le conseguenze sono tutte favorevoli:

- se il leader muore prima di rispondere, i messaggi non ackati vengono **automaticamente riconsegnati** dal broker a un worker vivo → la bet non è persa, entra nel round successivo. È strettamente meglio del rimborso;
- il broker torna a essere il buffer durevole che è, invece di essere scavalcato;
- il rischio introdotto — riconsegna di una bet già accettata il cui ack si è perso — è **coperto** dal `bet_id` UUID: il wheel deduplica e riacka. Senza `bet_id` questo rimedio non sarebbe praticabile.

Il costo è nullo: `prefetch_count` è già impostato ([rabbitmq_manager.erl:225-226](erlang-engine/game_engine/src/rabbitmq_manager.erl#L225-L226)) e limita di per sé quante bet un worker può tenere non ackate.

### Split-brain: due leader, due ledger, Mnesia da riparare a mano

La Fase 3 implementata fa scattare l'elezione su `nodedown` senza distinguere **crash** da **partizione di rete**: `cluster_manager` chiama `leader_election:start_election/0` a ogni evento di topologia, sia in `nodeup` ([cluster_manager.erl](erlang-engine/game_engine/src/cluster_manager.erl)) sia in `nodedown` ([cluster_manager.erl](erlang-engine/game_engine/src/cluster_manager.erl)), **incondizionatamente**. In una partizione 2-1 entrambi i lati vedono `nodedown` ed entrambi eleggono un leader. Finché il danno era il solo RNG duplicato era contenuto; con l'architettura di questo piano diventa grave:

- entrambi i lati consumano da `bets_queue` (il broker è raggiungibile da tutti) e pubblicano **ledger concorrenti e divergenti** per lo stesso numero di round;
- entrambi scrivono `snapshot_record` su Mnesia `disc_copies`; alla riconnessione Mnesia rileva `inconsistent_database` e **richiede riparazione manuale**;
- UC3 perde ogni valore: un audit trail che ammette due verità sullo stesso round non prova nulla.

**Rimedio: guardia di quorum prima di `declare_victory/1`.** ✅ **Implementata** (Step 3 del Blocco A) e verificata: con 1 nodo su 3 il nodo superstite logga `Quorum 1/3 non raggiunto` e resta standby invece di autoeleggersi.

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

- **due punti di controllo, non uno.** `declare_victory/1` copre solo chi *sta diventando* leader; il leader **già in carica** finito nel lato di minoranza non esegue mai `declare_victory` e resterebbe attivo. Serve quindi la stessa guardia sul `nodedown`.
  🔧 **Rispetto alla prima stesura cambia dove**: `leader_election` non sottoscrive `net_kernel:monitor_nodes/2` — lo fa `cluster_manager` ([cluster_manager.erl](erlang-engine/game_engine/src/cluster_manager.erl)). Anziché duplicare la sottoscrizione, `cluster_manager` notifica l'elezione:
  ```erlang
  %% in cluster_manager.erl, dentro handle_info({nodedown, Node, _}, State):
  %% sostituisce maybe_start_election_on_nodedown/1 (:212-228)
  leader_election:node_down(Node),

  %% in leader_election.erl
  handle_cast({node_down, _Node}, State = #state{role = leader}) ->
      case has_quorum() of
          true  -> {noreply, start_election_if_leader_lost(State)};
          false ->
              io:format("[LEADER] Quorum perso — retrocessione a standby~n"),
              apply_role(standby),
              {noreply, State#state{role = standby, leader = undefined}}
      end;
  handle_cast({node_down, _Node}, State) ->
      %% standby: elezione solo se abbiamo il quorum, altrimenti resteremmo
      %% a eleggere un leader dentro la minoranza.
      case has_quorum() of
          true  -> {noreply, start_election_if_leader_lost(State)};
          false -> {noreply, State#state{leader = undefined}}
      end.
  ```
  Senza il primo ramo la guardia è inefficace proprio nello scenario che deve coprire: è il **vecchio** leader isolato a produrre il ledger divergente, non il nuovo;
- il nodo retrocesso azzera `leader` e propaga `{set_leader, undefined}` ai propri worker, che passano al ramo `reject{requeue = true}`;
- i worker della minoranza hanno `leader = undefined` e fanno `rabbitmq_manager:reject(Tag, true)`: le bet restano nel broker e vengono servite dalla maggioranza. Nessuna bet persa, nessun ledger divergente;
- solo il leader scrive `snapshot_record`, e con il quorum garantito esiste al più un leader → nessuna scrittura Mnesia concorrente.

Due accorgimenti a corredo:
- chiave di `snapshot_record` = `{Round, Initiator}` anziché un intero monotono: due leader concorrenti produrrebbero record **distinti e diagnosticabili** invece di sovrascriversi silenziosamente;
- sottoscrivere `mnesia:subscribe(system)` e loggare `{inconsistent_database, _, _}` in modo rumoroso.

> Con 3 nodi il quorum è 2. Un cluster a **2 nodi non può avere quorum utile**: la partizione 1-1 blocca entrambi i lati. È una limitazione onesta da dichiarare nella relazione — è il teorema CAP, non un difetto implementativo: qui si sceglie la consistenza sulla disponibilità, che per un ledger di scommesse è la scelta giusta.

---

## Parte 3 — Le tre regole di riconciliazione

L'ack differito della §Parte 2 e il `round_cancelled` della Fase 5 si contraddicono se lasciati impliciti: il primo dice che le bet non ackate vengono **rigiocate** nel round successivo, il secondo che le bet `PENDING` del round caduto vengono **rimborsate**. Una bet che cade in entrambe le descrizioni verrebbe rimborsata *e* giocata: il denaro torna nel wallet e la scommessa gira lo stesso.

Al crash del leader in fase `betting`, ogni bet del round R sta in **uno solo** di tre insiemi:

| Insieme | Come si riconosce | Destino corretto |
| :--- | :--- | :--- |
| **Ackata** — accettata dal wheel morto, ack già inviato | assente dalle `inflight` dei worker, assente dal broker | **Rimborso**: nessuno la possiede più, è l'unico caso realmente perso |
| **Non ackata** — consumata ma senza `bet_result` | presente nelle `inflight` di un worker vivo | **Replay**: il broker la riconsegna, entra nel round R+1. Nessun rimborso |
| **Mai consumata** | ancora nella coda | **Replay**: idem |

`{collect_inflight, R}` serve esattamente a separare il primo insieme dagli altri due. Da qui le tre regole che vanno scritte nel piano una volta e rispettate ovunque:

> **R1 — Chi rimborsa.** Si rimborsa una bet solo se è assente da ogni ledger **e** assente da `exclude_bet_ids`. In dubbio non si rimborsa: la bet resta `PENDING` e sarà chiusa dal ledger del round in cui verrà rigiocata.
>
> **R2 — Chi è autoritativo.** `Bet.status` lato Java è autoritativo sui **movimenti di denaro**; il ledger Erlang è autoritativo sull'**esito di gioco**. Una bet già `REFUNDED` che ricompare in un ledger successivo (caso residuo: il worker che la teneva è morto insieme al leader, quindi non è finita in `exclude_bet_ids`) **non viene mai pagata**: `PayoutListener` interroga solo le `PENDING`, quindi la regola è già rispettata dalla query. `LedgerListener` deve però loggarla come `replay_after_refund` e chiuderla in stato terminale, perché è l'unico caso in cui l'utente vede sulla ruota una puntata che gli è stata restituita. Nessuna duplicazione di denaro, solo un'anomalia visiva da documentare.
>
> **R3 — L'insieme di dedup è l'unione dei ledger persistiti, non `bets` in memoria.** Una bet già presente in un ledger **non rientra mai in gioco**, in nessun round successivo: il wheel risponde `{bet_result, BetId, accepted}` senza rigiocarla, il worker acka e la scarta.

R3 è la regola che il resto della §Parte 2 dà per scontata senza enunciarla, ed è quella che rende davvero sicuro l'ack differito. Il motivo è che `bets` viene azzerato a ogni round ([wheel_process.erl](erlang-engine/game_engine/src/wheel_process.erl)): una dedup che guarda solo `bets` è idempotente **soltanto dentro la finestra del round**. Percorso concreto, che non richiede nemmeno un crash:

1. il wheel accetta `BetId = X` nel round R e casta `{bet_result, X, accepted}`;
2. X entra nel ledger di R, pubblicato e persistito su Mnesia;
3. il worker muore prima di ackare — **oppure** il suo `inflight_timeout` da 15 s scatta perché il wheel era bloccato nella `gen_server:call` del minigioco ([wheel_process.erl](erlang-engine/game_engine/src/wheel_process.erl)), e rimette in coda una bet già accettata e già a ledger;
4. il broker riconsegna X nel round R+1;
5. senza R3, `bets` di R+1 non contiene X → **accettata di nuovo, giocata due volte, pagata due volte**. `Σ wallet` cresce dal nulla, e `PayoutListener` filtrato per round paga sia in R sia in R+1 perché `LedgerListener` avrà nel frattempo riscritto `Bet.round`.

R2 non copre questo caso: parla di una bet `REFUNDED` che ricompare, non di una `PENDING` già a ledger che ricompare.

**Implementazione.** ✅ **Completa e verificata**, in entrambi i termini. Il wheel mantiene in `#state{}` un set `settled_bet_ids` con i `bet_id` degli ultimi 3 round, ripopolato dai `snapshot_record` all'avvio e a ogni elezione (il nuovo leader ne ha bisogno subito: è proprio dopo un crash che le riconsegne arrivano). Il fallback, se il set è vuoto o incerto, è una scansione all'indietro di `snapshot_record`, banale con `ordered_set` e chiave `{Round, Initiator}`.

Le tre regole rendono ogni percorso deterministico e sono ciò che i test «Kill del leader in fase `betting`», «Replay dopo rimborso» e «Riconsegna cross-round» della §Parte 7 verificano.

---

## Parte 4A — Correzioni al codice già scritto (Fasi 2-3) — ✅ APPLICATE

> 🔧 Questa sezione **sostituisce** quella che nella prima stesura descriveva modifiche testuali alle Fasi 2-3 di `implementation_plan.md`: quel codice ora esiste, quindi si tratta di modificarlo, non di riscrivere il piano.

### `cluster_manager.erl` — aggiunte

1. ✅ **FATTA — `configured_nodes/0`** — lista **statica** dei nodi, denominatore del quorum. Attenzione: `init/1` filtra il proprio nodo da `peer_nodes` ([cluster_manager.erl](erlang-engine/game_engine/src/cluster_manager.erl)), quindi `known_nodes` **non** contiene `node()` e va ri-aggiunto:
   ```erlang
   handle_call(configured_nodes, _From, State) ->
       {reply, lists:usort([State#state.self_node | State#state.known_nodes]), State};
   ```
   Non serve la nuova chiave `env` `{nodes, [...]}` proposta in origine: `peer_nodes` ([app.src:27](erlang-engine/game_engine/src/game_engine.app.src#L27)) contiene già tutti e tre i nodi.
2. ✅ **FATTA — `get_participants/0`** — lista **ordinata e stabile dei nodi vivi**, quella che lo snapshot congela all'avvio del taglio. Va **intersecata con `configured_nodes/0`**, per la stessa ragione del numeratore del quorum: `connected_nodes` viene popolata da `{nodeup, Node, _}` ([cluster_manager.erl](erlang-engine/game_engine/src/cluster_manager.erl)), che con `{node_type, all}` ([cluster_manager.erl](erlang-engine/game_engine/src/cluster_manager.erl)) include anche le shell diagnostiche. Una shell attaccata al cluster diventerebbe altrimenti un partecipante allo snapshot, e il taglio non si chiuderebbe mai (nessun `snapshot` gira su quella shell) fino al timeout di 5 s, marcando ogni snapshot come `degraded`.
3. ✅ **FATTA — Bootstrap Mnesia** — **dopo** la formazione del cluster, mai in `init/1`. Il punto d'aggancio naturale è un nuovo `handle_info(mnesia_bootstrap, ...)` schedulato insieme a `initial_election` ([cluster_manager.erl](erlang-engine/game_engine/src/cluster_manager.erl)), cioè dopo che `ping_peers` ([cluster_manager.erl](erlang-engine/game_engine/src/cluster_manager.erl)) ha avuto il tempo di connettere i peer.
   Non si può usare `mnesia:create_schema(AllNodes)`: quella forma esige Mnesia **arrestata su tutti i nodi elencati**, condizione che non si verifica mai con nodi che si avviano progressivamente. Serve il join dinamico, in due rami distinti:

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
   `MasterNode` = un nodo qualsiasi già nel cluster che possiede la tabella; discriminare i due rami interrogando `mnesia:table_info(snapshot_record, disc_copies)` via `rpc:call/4` sui peer raggiungibili. La `change_table_copy_type` dello schema è il passo che si dimentica più spesso: senza, il nodo tiene lo schema in RAM e **perde la propria copia a ogni riavvio**, vanificando `disc_copies`.
4. ✅ **FATTA — `mnesia:subscribe(system)`** + log rumoroso su `{inconsistent_database, _, _}`.
5. ✅ **FATTA — `leader_election:node_down/1`** al posto di `maybe_start_election_on_nodedown/1` ([cluster_manager.erl](erlang-engine/game_engine/src/cluster_manager.erl)): la decisione «rieleggere o retrocedere» ha bisogno del ruolo corrente e del quorum, e vive quindi nell'elezione. Il `try/catch error:undef` che proteggeva la Fase 2 dalla mancanza del modulo ([cluster_manager.erl](erlang-engine/game_engine/src/cluster_manager.erl)) non serve più: `leader_election` esiste ed è nel supervisore.

> [!NOTE]
> `mnesia` va aggiunta alle `applications` di [game_engine.app.src](erlang-engine/game_engine/src/game_engine.app.src#L7-L12). Con `rebar3 shell` la directory Mnesia di default è `Mnesia.<nodo>` nella cwd: avviando i tre nodi dalla stessa cartella si ottengono tre directory distinte, che è il comportamento voluto. Con `-sname` diverso e stessa cwd non c'è collisione.

> [!WARNING]
> Con `rest_for_one` ([game_engine_sup.erl:30](erlang-engine/game_engine/src/game_engine_sup.erl#L30)), `cluster_manager` è prima di `wheel_process`: un suo crash azzera il round in corso. Accettabile, ma non per la ragione più ovvia: il checkpoint Mnesia esiste **solo dopo il gong**, quindi copre il crash in fase `spinning`/`minigame`. Un crash in fase `betting` non ha checkpoint e ricade sullo stesso percorso del crash del leader — `round_cancelled` con `exclude_bet_ids` più ack differito (§Parte 3) — che è ciò che impedisce la perdita delle puntate. Le due protezioni sono complementari, nessuna delle due basta da sola.

### `leader_election.erl` — correzioni — ✅ 1-3 applicate

1. **`apply_role/1` non deve più toccare il `worker`** ([leader_election.erl](erlang-engine/game_engine/src/leader_election.erl)). Rimuovere `gen_server:cast(worker, activate)` ([leader_election.erl](erlang-engine/game_engine/src/leader_election.erl)) e `gen_server:cast(worker, deactivate)` ([leader_election.erl](erlang-engine/game_engine/src/leader_election.erl)): l'ingestione delle bet è replicata su tutti i nodi, solo `wheel_process` resta leader-only. I due cast verso `wheel_process` restano invariati.
2. **`{set_leader, LeaderNode}` a tutti i worker.** `declare_victory/1` ([leader_election.erl](erlang-engine/game_engine/src/leader_election.erl)) già itera su `nodes()` per il `{coordinator, _}`: nello stesso ciclo va aggiunto `gen_server:cast({worker, N}, {set_leader, MyNode})`, più il worker locale. Anche `handle_cast({coordinator, Leader}, ...)` ([leader_election.erl](erlang-engine/game_engine/src/leader_election.erl)) deve propagare `{set_leader, Leader}` al proprio worker: è da lì che gli standby apprendono l'identità del leader.
3. **Guardia di quorum in `declare_victory/1` e in `node_down/1`** (§Parte 2). Senza maggioranza il nodo non si dichiara leader e, se lo era già, si autoretrocede.
4. **Ripopolare `settled_bet_ids`** (regola R3): dopo `apply_role(leader)`, il nuovo leader chiede al wheel di ricaricare i `bet_id` degli ultimi round dai `snapshot_record`. È subito dopo un crash che le riconsegne arrivano, quindi un nuovo leader con il set vuoto è esattamente il caso in cui R3 serve di più.
5. **`{collect_inflight, R}`** ai worker superstiti, per il ramo di fallback della Fase 5 (§Parte 4B).

> [!CAUTION]
> Due difetti dell'elezione attuale, indipendenti dallo snapshot ma che il quorum rende visibili: `handle_cast({election, FromNode}, ...)` ([leader_election.erl](erlang-engine/game_engine/src/leader_election.erl)) rilancia **sempre** un'elezione, anche quando ne è già in corso una (`election_in_progress` è memorizzato ma mai controllato), e `cluster_manager` ne lancia una a ogni `nodeup`. Con 3 nodi che si avviano insieme si ottiene una raffica di elezioni innocua ma rumorosa. Vale la pena aggiungere la guardia su `election_in_progress` mentre si tocca il modulo.

### `worker.erl` — correzioni — ✅ applicate

- **Rimuovere il flag `active`** ([worker.erl](erlang-engine/game_engine/src/worker.erl)) e i due `handle_cast(activate|deactivate, ...)` ([worker.erl](erlang-engine/game_engine/src/worker.erl)); al loro posto `handle_cast({set_leader, Node}, ...)`.
- **Fondere le due clausole di delivery** ([worker.erl](erlang-engine/game_engine/src/worker.erl) e [worker.erl](erlang-engine/game_engine/src/worker.erl)) in una sola: il discriminante non è più il ruolo ma `leader =:= undefined`. Il ramo `reject(Tag, true)` sopravvive **solo** per quel caso. Così sparisce anche il loop caldo attuale, in cui uno standby rimbalza al broker ogni messaggio che gli capita, all'infinito, finché non lo prende il leader.
- Il dettaglio dei nuovi handler è nella §Parte 5.

---

## Parte 4B — Modifiche a `implementation_plan.md` — ✅ APPLICATE

> Le modifiche descritte da questa sezione sono state **riportate in `implementation_plan.md`**: quel documento descrive ora il progetto corretto. Qui resta la traccia di cosa è cambiato e dove, così che i due piani si leggano insieme.
> **Il codice non è stato toccato**: la realizzazione resta da fare, nell'ordine della §Parte 8, e la §Parte 4A (correzioni al codice già scritto delle Fasi 2-3) è il primo passo.

| Sezione di `implementation_plan.md` | Cosa è stato riportato |
|---|---|
| [Background & Goal](implementation_plan.md#L3-L25) | Tabella di stato delle 5 funzionalità distribuite, rimando a questo piano, avviso che le Fasi 2-3 vanno **corrette** e non solo estese |
| [FIX 0.1.14](implementation_plan.md#L162) | `bet_id` UUID presentato come **causa radice** di 0.1.3/0.1.4: colonna univoca sull'entity, Jackson al posto di `String.format`, `findByBetId`/`findByRoundAndStatus`, più la riga nella matrice di riepilogo |
| [Fase 1](implementation_plan.md#L895) | `reject/2` aggiunta all'API documentata di `rabbitmq_manager`, con la nota che serve all'ack differito |
| [Fase 2 — Estensioni](implementation_plan.md#L1200) | `configured_nodes/0` e `get_participants/0` (entrambe intersecate con la lista statica), bootstrap Mnesia nei due rami con l'avvertenza su `change_table_copy_type`, `mnesia:subscribe(system)`, delega a `leader_election:node_down/1`, `mnesia` fra le `applications` e `snapshot` fra i `registered` |
| [Fase 3](implementation_plan.md#L1285) | Marcata ✅ IMPLEMENTATA con la tabella delle **5 divergenze**; `apply_role/1` che non tocca più il worker; `broadcast_leader/1` con `{set_leader, N}`; `has_quorum/0` applicata nei due punti; worker senza flag `active`, con `leader` e `inflight`, e instradamento al leader di **tutti** i percorsi; `undo_bets` convertita a `cast` |
| [Fase 4](implementation_plan.md#L1484) | **Riscritta integralmente**: tabella dei 7 difetti della stesura precedente, architettura a competing consumers, `cl_recorder`, trigger del taglio **dopo** l'estrazione del vincitore, collector che non interroga mai i partecipanti, `snapshot_record` su Mnesia, `snapshot` come **ultimo** figlio del supervisore, limite noto del minigioco |
| [Fase 5](implementation_plan.md#L1726) | **Riscritta**: regole R1/R2/R3, recovery a due rami letto dal checkpoint, `exclude_bet_ids`, `apply_role(leader)` senza cast al worker, flag nel process dictionary sostituito, dispatch Java sul campo `type`, `LedgerListener`, handler `bet_rejected`, rimozione di `RefundListener` con il vincolo d'ordine sull'UNDO |
| [Fase 6](implementation_plan.md#L1932) | Checklist sostituita con i test **falsificabili** della §Parte 7 (via la riga sul log dello snapshot), precondizione `prefetch = 1`, comandi di verifica del cluster e avvertenza sulle shell distribuite |
| [Summary of All Files](implementation_plan.md#L1970) | Tabelle riallineate al codice: righe nuove per `cl_recorder.erl`, la tabella Mnesia, `LedgerListener.java` e `Bet.java`; `snapshot.erl` ridescritto come **collector**; corrette le righe che davano per da fare cose già fatte |
| [Execution Order](implementation_plan.md#L2018) | Grafo aggiornato con il nodo **RETROFIT** fra Fase 3 e Fase 4, e i tre vincoli d'ordine: retrofit prima di tutto, quorum prima della Fase 4, `bet_id` prima dell'ack differito |
| [Verification Plan](implementation_plan.md#L2044) | Stato per fase, con il test decisivo della Fase 4 dichiarato esplicitamente: se `in_flight_bets` resta 0, la riscrittura non ha raggiunto il suo scopo |

---

## Parte 5 — Modifiche al codice

### [NEW] `erlang-engine/game_engine/src/cl_recorder.erl` — ✅ FATTO (8 test eunit)
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

- `start(SnapId, LocalState, InChannels) -> NewCL` — ingresso dell'**iniziatore**, che non riceve mai un marker e quindi non può passare da `on_marker/5`: salva `local` e apre la registrazione su *tutti* gli `InChannels`. È la funzione che il wheel chiama al gong.
- `on_marker(SnapId, From, InChannels, LocalState, CL) -> {NewCL, first_marker | subsequent}` — al primo marker salva `local`, apre la registrazione su `InChannels -- [From]`; ai successivi chiude il canale `From`. Il chiamante è responsabile di inviare i propri marker uscenti (deve farlo *lui*, perché il mittente deve essere il processo applicativo).
- `on_app_msg(From, Msg, CL) -> NewCL` — accoda a `chan[From]` **solo se** sta registrando e `From ∈ in_open`.
- `close(From, CL) -> NewCL` — chiude un singolo canale entrante. Usata dal percorso di abort (`{cl_abort, SnapId}`) per chiudere in blocco ciò che resta aperto quando il collector non risponde.
- `is_complete(CL) -> boolean()` — `in_open == []`.

### `erlang-engine/game_engine/src/rabbitmq_manager.erl` — ✅ nessuna modifica necessaria
L'ack differito ha bisogno di rifiutare messaggi: `reject/2` **esiste già** ed è esportata ([:22](erlang-engine/game_engine/src/rabbitmq_manager.erl#L22), [:91-95](erlang-engine/game_engine/src/rabbitmq_manager.erl#L91-L95)), modellata su `ack/1` ([:84-88](erlang-engine/game_engine/src/rabbitmq_manager.erl#L84-L88)) — stesso accesso via ETS, quindi lock-free e senza passare dal gen_server.

Resta valida l'unica avvertenza: il worker usa `rabbitmq_manager:reject(Tag, true)`, mai `amqp_channel` direttamente, perché il pid del canale cambia a ogni riconnessione ([:227](erlang-engine/game_engine/src/rabbitmq_manager.erl#L227)) e solo il manager sa qual è quello valido.

L'unica modifica prevista su questo file arriva al passo 5 della §Parte 8: togliere `refunds_queue` da `?QUEUES` ([:30](erlang-engine/game_engine/src/rabbitmq_manager.erl#L30)).

### [MODIFY] `erlang-engine/game_engine/src/worker.erl`

> ✅ I punti **1-4** e **6-9** sono implementati (Blocchi A e B): campo `leader` al posto di `active`, `{set_leader, N}`, instradamento al leader di tutti i percorsi, canale asincrono con **ack differito**, `inflight_timeout`, `bet_id` nel parser, rimborso puntuale al posto di `refunds_queue`. Resta il punto **5**, gli handler dei marker dello snapshot.

1. ✅ **FATTO** — State record: `#state{leader = undefined, cl = cl_recorder:new(), inflight = #{}}`, dove `inflight :: #{BetId => {DeliveryTag, BetMap}}` — serve sia allo snapshot sia all'ack differito. 🔧 **Il campo `active` sparisce** ([worker.erl](erlang-engine/game_engine/src/worker.erl)).
2. ✅ **FATTO** — `handle_cast({set_leader, Node})` — memorizza il leader; **sostituisce** `handle_cast(activate|deactivate, ...)` ([worker.erl](erlang-engine/game_engine/src/worker.erl)).
3. ✅ **FATTO** — `process_message/3` non fa più `gen_server:call(wheel_process, ...)` ([worker.erl](erlang-engine/game_engine/src/worker.erl)): diventa `gen_server:cast({wheel_process, Leader}, {bet, BetMap})`.
   **L'ack AMQP non è più immediato**: oggi parte al ritorno di `process_message/1` ([worker.erl](erlang-engine/game_engine/src/worker.erl)), che essendo una `call` sincrona locale garantisce «il wheel ha deciso». Passando al `cast` quella garanzia si perderebbe: il `DeliveryTag` va quindi conservato in `inflight` e l'ack parte solo alla ricezione di `{bet_result, BetId, accepted | rejected}`. Motivazione completa in §Parte 2.
   Se `leader = undefined` (nessun leader eletto, o nodo in minoranza dopo una partizione) la bet non viene inoltrata: `rabbitmq_manager:reject(Tag, true)`, così resta nel broker e verrà consegnata a un nodo che può servirla. 🔧 Questo ramo **assorbe e sostituisce** la clausola di delivery in standby ([worker.erl](erlang-engine/game_engine/src/worker.erl)): il requeue avviene solo quando *nessuno* può servire il messaggio, non ogni volta che lo riceve un non-leader.
4. ✅ **FATTO** — Instradare al leader anche gli altri quattro percorsi di `process_message/3` ([worker.erl](erlang-engine/game_engine/src/worker.erl)). Il punto 3 converte il solo ramo bet, ma la stessa funzione instrada oggi **verso il `wheel_process` locale**: `type: "force_segment"` ([worker.erl](erlang-engine/game_engine/src/worker.erl)) e `FORCE_<Seg>` ([worker.erl](erlang-engine/game_engine/src/worker.erl)), `type: "minigame_choice"` ([worker.erl](erlang-engine/game_engine/src/worker.erl)), `segment: "UNDO_BETS"` ([worker.erl](erlang-engine/game_engine/src/worker.erl)).
   Appena il worker resta attivo su tutti i nodi, un `minigame_choice` consumato da uno standby chiama il `wheel_process` **dormiente di quel nodo** e la scelta del giocatore sparisce in silenzio; con 3 nodi succede circa 2 volte su 3, perché i worker sono competing consumer. Idem per il pannello dev dell'admin e per l'UNDO. Tutti e quattro vanno a `{wheel_process, Leader}`.

   ✅ Tutti e quattro i percorsi sono già instradati al leader e `undo_bets/1` è già stata convertita a `cast`, con il rimborso ora pubblicato dal wheel. Per memoria del perché: `force_segment/1` e `submit_choice/2` erano **già** `cast` ([wheel_process.erl](erlang-engine/game_engine/src/wheel_process.erl), [wheel_process.erl](erlang-engine/game_engine/src/wheel_process.erl)) e basta indirizzarli al nodo giusto. `undo_bets/1` no: è una `gen_server:call` ([wheel_process.erl](erlang-engine/game_engine/src/wheel_process.erl)) e va **convertita a `cast`**, perché come call cross-nodo ricadrebbe nel limite noto del punto 8 di §`wheel_process.erl` — il wheel è bloccato fino a 10 s dentro la `gen_server:call` del minigioco ([wheel_process.erl](erlang-engine/game_engine/src/wheel_process.erl)) — e un UNDO che capita in fase `minigame` andrebbe in timeout facendo crollare il worker. Con l'UNDO diventato `cast`, il rimborso non torna più come valore di ritorno: è il wheel a pubblicarlo, vedi il punto 8.
5. ✅ **FATTO** — **Nuovo handler marker** — sul canale entrante `wheel → worker`:
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
   Il worker ha un solo canale entrante, quindi termina immediatamente e riporta al collector: `gen_server:cast({snapshot, InitiatorNode}, {cl_part, SnapId, {worker, node()}, Local, ChanStates})`.
6. ✅ **FATTO** — `handle_cast({bet_result, BetId, Verdict})` è l'unico punto in cui si acka, poi `rabbitmq_manager:ack(Tag)` e rimuove da `inflight`. È l'unico punto in cui si acka.
7. ✅ **FATTO** — **Timeout dell'inflight**: `{inflight_timeout, BetId}` armato a 15 s al momento del cast. Se scade (leader morto prima di rispondere) → `rabbitmq_manager:reject(Tag, true)`: la bet torna nel broker e sarà riconsegnata a un worker vivo. La perdita silenziosa sparisce.
8. ✅ **FATTO** — **`publish_refund/1` riscritto, non rimosso** ([worker.erl](erlang-engine/game_engine/src/worker.erl)).
   Attenzione: ha **due** chiamanti, non uno. Oltre al caso `betting_closed` ([worker.erl](erlang-engine/game_engine/src/worker.erl)) c'è l'**UNDO** ([worker.erl](erlang-engine/game_engine/src/worker.erl)), che pubblica un totale **aggregato su più bet** con `segment = <<"REFUND">>`. Eliminando `refunds_queue` senza sostituire quel percorso, l'UNDO smette di accreditare il saldo: il giocatore annulla, le bet spariscono dal wheel, i soldi non tornano. È una regressione funzionale silenziosa, non un dettaglio.
   Rimedio, nella stessa direzione del resto del piano: `undo_bets/1` restituisce la **lista dei `bet_id` annullati** invece del totale ([wheel_process.erl](erlang-engine/game_engine/src/wheel_process.erl)), e il wheel emette un `bet_rejected` per ciascuno con `"reason":"undo"`. L'handler Java del punto 6 di §Java lo gestisce già senza modifiche.
   Quanto al caso `betting_closed`: delegare *tutti* i rimborsi al ledger lascerebbe scoperte le bet rifiutate **dopo** la pubblicazione del ledger: una puntata in ritardo che arriva durante `spinning` riceve `rejected`, il worker acka e la scarta, ma il ledger del round R è già stato pubblicato al gong e `PayoutListener` interroga per round — la bet resterebbe `PENDING` per sempre con il saldo scalato. Serve quindi un evento esplicito e puntuale, che è ciò che `publish_refund/1` già faceva; ciò che va eliminato è il **match per importo** lato Java, non l'evento. Nuova forma, pubblicata dal **wheel** (unico punto di decisione, e conosce il round autoritativo) su `results_queue`:
   ```json
   {"type":"bet_rejected","bet_id":"<uuid>","round":R,"reason":"betting_closed"}
   ```
   La coda `refunds_queue` non viene più usata da Erlang.
9. ✅ **FATTO** — `parse_bet_json/1` estrae anche `<<"bet_id">>` con `extract_string_field/2` ([worker.erl](erlang-engine/game_engine/src/worker.erl)). I comandi `force_segment` e `minigame_choice` non ne hanno uno: `bet_id = undefined` deve saltare sia la dedup di R3 sia la mappa `inflight`, e questi messaggi restano ad **ack immediato** — l'ack differito riguarda solo le bet.

### [MODIFY] `erlang-engine/game_engine/src/wheel_process.erl`
1. ✅ **FATTO** (tranne `settled_bet_ids`) — promuovere lo stato del round nel record ([wheel_process.erl](erlang-engine/game_engine/src/wheel_process.erl)); oggi segmento vincente, indice e dettagli del minigioco vivono solo dentro i messaggi `send_after` ([wheel_process.erl](erlang-engine/game_engine/src/wheel_process.erl), [wheel_process.erl](erlang-engine/game_engine/src/wheel_process.erl), [wheel_process.erl](erlang-engine/game_engine/src/wheel_process.erl)):
   ```erlang
   -record(state, {
       phase, time_left, round, bets, forced_segment, history, minigame_choices,
       active = false,               %% gia' presente (Fase 3)
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
   `settled_bet_ids` va ripopolato dai `snapshot_record` all'avvio **e a ogni elezione**: è subito dopo un crash che le riconsegne arrivano, quindi un nuovo leader con il set vuoto è esattamente il caso in cui R3 serve di più.
2. ✅ **FATTO** — `handle_cast({bet, BetMap, FromNode}, S)` sostituisce `handle_call({place_bet, ...})` ([wheel_process.erl](erlang-engine/game_engine/src/wheel_process.erl)).
   **Deduplicare per `bet_id` prima di tutto** (regola R3, §Parte 3): con l'ack differito una riconsegna del broker può ripresentare una bet già accettata (ack perso). Se `BetId` è già in `bets` **oppure in `settled_bet_ids`** → rispondere `{bet_result, BetId, accepted}` senza inserirla di nuovo. È il `bet_id` UUID a rendere l'operazione idempotente: senza di esso l'ack differito introdurrebbe duplicati.
   Il secondo termine del test non è pleonastico: `bets` è azzerato a ogni round ([wheel_process.erl](erlang-engine/game_engine/src/wheel_process.erl)), quindi da solo copre la riconsegna intra-round ma **non** quella che arriva nel round successivo, che è il caso frequente perché nasce da un crash. Vedi R3 per il percorso completo.
   Altrimenti:
   - `phase = betting, active = true` → accetta, `cast` di `{bet_result, BetId, accepted}` al worker mittente;
   - **in registrazione e canale aperto** → `cl_recorder:on_app_msg/3` e **basta**: la bet finisce nello stato del canale e verrà unita a `bets` alla chiusura del taglio. Non va aggiunta anche a `bets` qui, altrimenti si conta due volte;
   - `active = false` (non sono il leader) → `{bet_result, BetId, rejected}`. La clausola `not_leader` esistente ([wheel_process.erl](erlang-engine/game_engine/src/wheel_process.erl)) resta come rete di sicurezza, ma con l'instradamento del §`worker.erl` punto 4 non dovrebbe più scattare;
   - altrimenti (fase chiusa) → `{bet_result, BetId, rejected}`.
   Mantenere `handle_call({place_bet,...})` come alias deprecato non serve: nessun altro chiamante.
3. ✅ **FATTO** — **Trigger del taglio** in `handle_info(tick, #state{phase = betting, time_left = 1})` ([wheel_process.erl](erlang-engine/game_engine/src/wheel_process.erl)) — **prima** estrarre il vincitore ([wheel_process.erl](erlang-engine/game_engine/src/wheel_process.erl)), **poi** iniziare:
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
4. ✅ **FATTO** — `handle_cast({cl_marker, SnapId, {worker, N}}, S)` — chiude il canale `{worker,N}`; se `cl_recorder:is_complete/1`:
   - `Bets' = Bets ++ lists:append(maps:values(Chan))` — **le bet in transito entrano nel round** (sono state spedite prima che il worker apprendesse del taglio: per il taglio causale appartengono al round R);
   - `[gen_server:cast({worker,N}, {bet_result, BetId, accepted}) || ...]` per ciascuna;
   - report al collector: `gen_server:cast({snapshot, node()}, {cl_part, SnapId, {wheel, node()}, Local, Chan})`.
5. ✅ **FATTO** — `handle_info({cl_abort, SnapId}, S)` — chiude forzatamente la registrazione, riporta ciò che ha, logga `degraded`.
6. `build_result_json/7` ([wheel_process.erl](erlang-engine/game_engine/src/wheel_process.erl)) — aggiungere `bet_id` a ogni entry dell'array `payouts`; la sorgente è `compute_payouts/3` ([wheel_process.erl](erlang-engine/game_engine/src/wheel_process.erl)), che oggi propaga solo `username`/`bet`/`payout`.
7. ✅ **FATTO** — Ogni `{bet_result, BetId, rejected}` è accompagnato da un `bet_rejected` su `results_queue` (formato in §`worker.erl` punto 8). È il solo percorso che chiude le bet arrivate dopo la pubblicazione del ledger; senza, restano `PENDING` a saldo scalato. `publish_to_queue/2` ([wheel_process.erl](erlang-engine/game_engine/src/wheel_process.erl)) è lock-free (ETS + `amqp_channel:cast`), quindi non blocca il wheel.
8. **Non toccare** la `gen_server:call(Module, {play, BonusBets}, 10000)` verso i mini-game ([wheel_process.erl](erlang-engine/game_engine/src/wheel_process.erl)): fuori scope per la scelta fatta. Va però **documentato come limite noto**: durante il minigioco il wheel è bloccato fino a 10 s e non risponde ad alcuna `call`. Le conseguenze sono tre, e vanno scritte tutte: (a) il wheel non può partecipare a uno snapshot in quella finestra — innocuo oggi, perché l'unico trigger è al gong in fase `betting`, ma è ciò che impedirebbe di aggiungere in futuro il trigger «transizione di fase» di UC1; (b) è la ragione per cui `undo_bets/1` va convertita da `call` a `cast` (§`worker.erl` punto 4); (c) è la ragione per cui l'`inflight_timeout` del worker non può scendere sotto i ~12 s.
9. ✅ **FATTO** — Spostare `escape_json_string/1` qui da [worker.erl](erlang-engine/game_engine/src/worker.erl)**, dove resterebbe senza chiamanti dopo la riscrittura di `publish_refund/1`. Non è solo igiene: [wheel_process.erl](erlang-engine/game_engine/src/wheel_process.erl) costruisce oggi le entry di `payouts` con `~s` sull'username **senza escaping** — lo stesso difetto che il punto 2 di §Java corregge lato gateway. Poiché `build_result_json/7` va comunque toccata per aggiungere `bet_id`, costa una riga infilarcelo.

### [NEW] `erlang-engine/game_engine/src/snapshot.erl` — collector — ✅ FATTO
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

### [NEW] tabella Mnesia `snapshot_record` — ✅ CREATA (in `include/game_engine.hrl`)
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
Tabella di tipo `ordered_set`, così `mnesia:dirty_last/1` restituisce l'ultimo round e `mnesia:dirty_read({Round, Node})` il record di un round specifico. `local_states` + `channel_states` sono conservati integralmente: senza di essi il post-mortem non può dire cosa fosse in volo (UC3).

### [MODIFY] `game_engine_sup.erl` — ✅ FATTO
Albero attuale ([:34-65](erlang-engine/game_engine/src/game_engine_sup.erl#L34-L65)), con l'unica aggiunta di `snapshot` **in coda**:
```
rabbitmq_manager → cluster_manager → leader_election → wheel_process
  → minigames_sup → worker → snapshot      ← ULTIMO (NEW)
```
`rest_for_one` ([:30](erlang-engine/game_engine/src/game_engine_sup.erl#L30)) resta invariato: essendo ultimo, un crash del collector non riavvia nulla sopra di lui e quindi non azzera il round in corso.

### [MODIFY] Java — `bet_id` e riconciliazione per ledger
> Il gateway Java è **invariato** dal commit `05a5c25`: tutti i riferimenti qui sotto sono stati riverificati e sono validi.

1. ✅ **FATTO** — **`entity/Bet.java`** — nuovo campo `@Column(unique = true) String betId`. `ddl-auto=update` ([application.properties:13](java-gateway/src/main/resources/application.properties#L13)) crea la colonna senza migrazione manuale.
2. ✅ **FATTO** — **`WalletController`** — generare `UUID.randomUUID().toString()`, persisterlo sulla `Bet` e includerlo nel JSON. Sostituire la concatenazione di stringhe con Jackson (l'username oggi non è escapato).
3. ✅ **FATTO** — **`BetRepository.java`** — aggiungere `findByBetId(String)`, `findByRoundAndStatus(Integer, String)`.
4. ✅ **FATTO** — **`GameResultListener`** — **dispatch sul campo `type`** (restano i rami `round_ledger` e `round_cancelled`), oggi completamente ignorato: qualunque messaggio su `results_queue` viene trattato come risultato di round. Instradare `result` → `PayoutListener`, `round_ledger` → nuovo `LedgerListener`, `round_cancelled` → cancellazione per-round, `bet_rejected` → rimborso puntuale per `bet_id`.
5. ✅ **FATTO** — **[NEW] `rabbitmq/LedgerListener.java`** — `@Transactional`, implementa R1 e R2 della §Parte 3:
   - per ogni `bet_id` nel ledger con `Bet` `PENDING`: setta `Bet.round = R`, lascia `PENDING`;
   - per ogni `bet_id` nel ledger con `Bet` già `REFUNDED` (caso R2): **non** riaprire, **non** pagare; loggare `replay_after_refund` e chiudere in stato terminale;
   - per ogni `Bet` `PENDING` con `round = R` **assente** dal ledger: `REFUNDED` + accredito saldo, idempotente sul `betId`.
6. ✅ **FATTO** (`BetRejectionHandler.java`) — **handler `bet_rejected`** — `@Transactional`: `findByBetId(id)`, e **solo se** `PENDING` → `REFUNDED` + accredito. L'idempotenza è la guardia sullo stato, non serve altro: una riconsegna del messaggio trova la bet già `REFUNDED` e non fa nulla. È la sostituzione deterministica del match per importo di `RefundListener`.
7. **handler `round_cancelled`** — rimborsa `findByRoundAndStatus(R,"PENDING")` **meno** `exclude_bet_ids` (regola R1).
8. ✅ **FATTO** — **`PayoutListener`** — `findByRoundAndStatus(round, "PENDING")` al posto del `findByStatus("PENDING")` globale; match per `bet_id` invece che per `username` ([PayoutListener.java:64](java-gateway/src/main/java/com/crazytime/rabbitmq/PayoutListener.java#L64)); rimuovere `it.remove()`.
9. ✅ **FATTO** — **`RefundListener.java`** — classe e coda **rimosse**: la sua funzione è assorbita dall'handler `bet_rejected`, che identifica la bet per `bet_id` invece che per importo (`RefundListener`).
   Prerequisito: il percorso UNDO deve già passare da `bet_rejected` (§`worker.erl` punto 8), altrimenti la rimozione della coda rompe l'annullamento delle puntate.
   Togliere `refunds_queue` da `?QUEUES` ([rabbitmq_manager.erl:30](erlang-engine/game_engine/src/rabbitmq_manager.erl#L30)) e il bean `refundsQueue` da [GatewayApplication.java:26-29](java-gateway/src/main/java/com/crazytime/GatewayApplication.java#L26-L29).
10. ✅ **FATTO in `PayoutListener`** — rimuovere il `catch` che inghiotte le eccezioni in `PayoutListener:107-109` e `RefundListener:77-79`: essendo i metodi `@Transactional`, l'eccezione catturata non provoca rollback e i `save` parziali vengono committati.

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

**Avvio a 3 nodi** — le istruzioni sono già in [istruzioni.txt](istruzioni.txt) (sezione «CLUSTER ERLANG — 3 NODI»): `ERL_FLAGS=-sname game{1,2,3}@localhost -setcookie crazytime` + `rebar3 shell`, con RabbitMQ attivo e gateway Java su :8080. La verifica del cluster passa da `cluster_manager:get_connected_nodes/0`; dopo il retrofit va aggiunta `cluster_manager:get_participants/0` e `leader_election:get_leader/0`.

**Bootstrap Mnesia** — avviare i nodi **uno alla volta** e verificare che il secondo e il terzo passino dal ramo `add_table_copy`; poi riavviare un nodo secondario e controllare che conservi la propria copia su disco (`mnesia:table_info(snapshot_record, disc_copies)` deve elencarlo). Se non lo elenca, manca la `change_table_copy_type(schema, ...)`.

**Test unitari su `cl_recorder`** — ✅ **8 test, 0 fallimenti** (`rebar3 eunit`): primo marker apre i canali giusti; marker successivo chiude solo il proprio; `on_app_msg` accoda solo sui canali aperti e nell'ordine d'arrivo; `is_complete` scatta esattamente quando tutti i canali sono chiusi; un taglio nuovo sostituisce uno rimasto aperto.

**Precondizione già soddisfatta — `basic_qos`.** Il test «Canali non vuoti» richiede che le bet si distribuiscano davvero fra i worker. Il `prefetch_count` **è già impostato** dalla Fase 1: `amqp_channel:call(ConsCh, #'basic.qos'{prefetch_count = maps:get(prefetch, Cfg)})` ([rabbitmq_manager.erl:225-226](erlang-engine/game_engine/src/rabbitmq_manager.erl#L225-L226)), valore 10, configurabile per nodo da [game_engine.app.src:22](erlang-engine/game_engine/src/game_engine.app.src#L22). Non serve aggiungerlo. Va però **abbassato a 1 durante il test di distribuzione**: con 10 un burst breve può finire quasi tutto sul primo consumer con credito disponibile e far sembrare rotto un refactoring corretto. In esercizio si torna a 10.

| Test | Risultato atteso |
| :--- | :--- |
| **Retrofit — nessun requeue a vuoto** | Con i 3 nodi attivi e traffico di bet, i log degli standby **non** devono mostrare rifiuti/riconsegne continue. È la verifica che la clausola `reject` su standby ([worker.erl](erlang-engine/game_engine/src/worker.erl)) è stata sostituita dalla condizione `leader =:= undefined` |
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
| **Partizione di rete 2-1, isolando il LEADER in carica** | È il caso che `declare_victory` non copre: il vecchio leader deve autoretrocedersi da `leader_election:node_down/1` entro il tempo di rilevazione. Un solo ledger pubblicato. Se il vecchio leader continua a pubblicare, la guardia è nel posto sbagliato |
| Partizione di rete 2-1, isolando uno standby | Il lato di minoranza non elegge un leader; i suoi worker fanno `reject{requeue=true}` e le bet vengono servite dalla maggioranza |
| Riconnessione dopo la partizione | **Non** attendersi l'assenza di `inconsistent_database`: Mnesia emette `{inconsistent_database, running_partitioned_network, _}` alla rilevazione della **partizione**, non della divergenza dei dati, e tutti i nodi hanno una replica di `snapshot_record` quindi Mnesia attiva. L'evento è normale e va solo loggato. Ciò che si verifica è l'assenza di **divergenza**: esattamente una scrittura di `snapshot_record` per round, ed esattamente un `type:"round_ledger"` su `results_queue` per round. Due ledger per lo stesso round = split-brain non contenuto. Con `disc_copies` senza opzione `majority` la riparazione resta manuale se la divergenza si verifica davvero: il quorum la rende improbabile, non impossibile |
| Invariante di conservazione | Su ogni `snapshot_record`: `Σ wallet + Σ bet_bloccate + Σ payout_in_volo` costante fra snapshot consecutivi |

**Ispezione Mnesia** — da una shell su un nodo standby, `mnesia:dirty_last(snapshot_record)` deve mostrare lo stesso record scritto dal leader: prova diretta della replica del checkpoint.

> **Attenzione durante i test di partizione:** una shell distribuita attaccata al cluster compare in `nodes()`, e `cluster_manager` la registra come nodo connesso perché monitora con `{node_type, all}` ([cluster_manager.erl](erlang-engine/game_engine/src/cluster_manager.erl)). Se `has_quorum/0` e `get_participants/0` non intersecassero con `configured_nodes/0` (§Parte 2, §Parte 4A), la shell regalerebbe il quorum al lato di minoranza proprio nel test che deve dimostrare il contrario, e diventerebbe un partecipante fantasma dello snapshot. Usare `-hidden`, o restare fuori dal cluster e passare da `rpc:call/4`.

---

## Parte 8 — Ordine di esecuzione

0. ✅ **FATTO — Retrofit delle Fasi 2-3 già implementate** (§Parte 4A): `configured_nodes/0` e `get_participants/0` su `cluster_manager`; rimozione di `activate`/`deactivate` del worker da `apply_role/1`; `{set_leader, N}` da `declare_victory/1` e da `handle_cast({coordinator, _})`; flag `active` del worker rimosso e clausole di delivery fuse su `leader =:= undefined`; `leader_election:node_down/1` al posto di `maybe_start_election_on_nodedown/1`. È il passo che rimette il codice sulla traiettoria di questo piano; finché non è fatto, i passi successivi lavorano contro un'architettura che li contraddice.
1. ✅ **FATTO — Prerequisiti** (indipendenti dallo snapshot): stato del round nel record di `wheel_process`; `bet_id` UUID lato Java; `worker → wheel` da `call` a `cast` con `{bet_result, ...}` di ritorno, **ack differito** + dedup per `bet_id` (§Parte 2 e R3 di §Parte 3), ed evento `bet_rejected` con il relativo handler Java, **incluso il percorso UNDO** (`undo_bets/1` restituisce i `bet_id`, `reason:"undo"`). L'ordine interno conta: `bet_id` prima dell'ack differito (altrimenti si introducono duplicati); il percorso UNDO su `bet_rejected` prima della rimozione di `refunds_queue` al passo 5 (altrimenti l'annullamento smette di rimborsare). `rabbitmq_manager:reject/2` non è più un prerequisito: ✅ esiste già.
2. **Fase 2 estesa**: bootstrap Mnesia + `mnesia` in `applications` + `mnesia:subscribe(system)`.
3. ✅ **FATTO — Guardia di quorum** su `declare_victory/1` e `node_down/1`. Va prima della Fase 4: senza, gli `snapshot_record` concorrenti di due leader corrompono Mnesia al primo test di partizione.
4. ✅ **FATTO — Fase 4 riscritta**: `cl_recorder` → partecipanti → collector `snapshot.erl` → persistenza Mnesia → pubblicazione ledger. Con Mnesia in piedi si chiude anche **R3**: `settled_bet_ids` ripopolato dai `snapshot_record` all'avvio e a ogni elezione. Fino a qui la dedup copre solo l'intra-round; è l'unico punto del percorso in cui il sistema è temporaneamente esposto alla doppia giocata, ed è per questo che R3 non può slittare oltre.
5. ✅ **FATTO — UC2 lato Java**: dispatch su `type`, handler `bet_rejected`, `LedgerListener` con le regole R1/R2, `PayoutListener` per-round, rimozione di `RefundListener` e di `refunds_queue` (solo dopo il passo 1).
6. **Fase 5 corretta**: recovery dal checkpoint; fallback `round_cancelled` con `exclude_bet_ids`, che dipende dal `{collect_inflight, R}` introdotto ai passi 0-1.
7. **Fase 6**: test della Parte 7.

I passi 0-4 lasciano il sistema funzionante a ogni tappa. Il passo 5 è quello che rende lo snapshot **consumato**: fino ad allora resta un artefatto di log, cioè esattamente la critica dell'analisi.
