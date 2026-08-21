# Snapshot Implementation Plan — Chandy-Lamport in Distributed Crazy Time

## Context

`snapshot_analisi.md` conclude che lo snapshot previsto dalla Fase 4 di `implementation_plan.md` è ridondante: cattura uno stato già disponibile in locale sul leader, su canali vuoti per costruzione, e nessuno ne consuma il risultato. Ho verificato la tesi contro il codice: **è corretta**, ed è anzi più grave di come la descrive il documento.

Questo piano fa due cose:
1. corregge `snapshot_analisi.md` sui punti in cui sbaglia o omette;
2. definisce le modifiche a `implementation_plan.md` (Fasi 2-6) e al codice per rendere lo snapshot **load-bearing**: un algoritmo il cui risultato non è ottenibile in altro modo e che alimenta due decisioni reali del sistema (ledger del round verso Java, recovery al crash del dealer).

**Scelte già prese** (rispondendo alle domande poste):
- ingestione bet distribuita: tutti i nodi consumano da `bets_queue` come competing consumers e inoltrano **asincroni** al wheel del leader;
- `bet_id` UUID generato da Java + `round` autoritativo assegnato da Erlang;
- persistenza degli snapshot su **Mnesia replicata** (colma anche il gap col diagramma architetturale della specifica, dove Mnesia c'è ma non è mai stata usata).

---

## Parte 1 — Revisione di `snapshot_analisi.md`

### Confermato dal codice

| Affermazione | Verifica |
| :--- | :--- |
| Nessuno consuma `get_snapshot/0` | Confermato: `snapshot.erl` **non esiste ancora**; nel piano l'API è esportata ma mai invocata. |
| `channel_states` mai popolato | Confermato: nessun percorso di codice vi accoda messaggi ([implementation_plan.md:1370-1378](implementation_plan.md#L1370-L1378)). |
| Handler mancanti (`snapshot_complete`, `get_snapshot`, `snapshot_timeout`) | Confermato: il timeout è armato a [implementation_plan.md:1423](implementation_plan.md#L1423) ma nessuna clausola lo riceve. |
| Topologia non congelata | Confermato: `nodes()` ricalcolato a [1400](implementation_plan.md#L1400) e di nuovo a [1442](implementation_plan.md#L1442). |
| I marker non attraversano RabbitMQ | Confermato. |
| Accoppiamento fragile `snapshot` → `wheel_process:get_bets()` | Confermato e **peggiore del previsto**: `wheel_process` resta bloccato fino a 10 s dentro `gen_server:call(Module, {play, BonusBets}, 10000)` ([wheel_process.erl:198](erlang-engine/game_engine/src/wheel_process.erl#L198)); qualsiasi `call` sincrona verso il wheel in quella finestra va in timeout. |
| Ipotesi FIFO soddisfatta da Erlang | Corretto. Da aggiungere il caveat: l'ordinamento vale a coppie di processi e **decade se la connessione fra nodi cade e si riforma** — proprio lo scenario di crash che la Fase 5 vuole gestire. |
| Verdetto finale sulla ridondanza | Corretto. Va rafforzato: la barriera «nessuna puntata ulteriore processata» è già imposta dalla guardia `phase = betting` in `handle_call({place_bet,...})` ([wheel_process.erl:85-90](erlang-engine/game_engine/src/wheel_process.erl#L85-L90)). Lo snapshot non *impone* il taglio, semmai lo *certifica*. |

### Da correggere

**C1 — G1 non «ESISTE GIÀ» (riga 43, riga 122).** È l'errore più importante del documento. Il canale `wheel ↔ 4 mini-game` è oggi una `gen_server:call` **sincrona** ([wheel_process.erl:198](erlang-engine/game_engine/src/wheel_process.erl#L198)), e i 4 mini-game sono **stateless**: `init/1` ritorna `#{}` e lo stato resta `#{}` per sempre. Uno snapshot su G1 catturerebbe 4 stati locali vuoti e 4 canali vuoti — esattamente la vacuità che il documento rimprovera a G2. La priorità consigliata («A+C per primi perché il grafo esiste già») poggia quindi su una premessa falsa: A+C richiedono di rendere asincrono `wheel↔minigame`, cioè lo stesso ordine di lavoro di D.

**C2 — Lo stato del round non è nel record, quindi UC1 non è realizzabile come descritto.** `WinnerSegment`, `WinnerIndex`, `Details` e `BonusBets` vivono **solo dentro i messaggi schedulati con `send_after`** ([wheel_process.erl:142-183](erlang-engine/game_engine/src/wheel_process.erl#L142-L183), `:193-245`), e nessuna `TimerRef` è memorizzata. Uno snapshot di `#state{}` durante `spinning`/`minigame` è incompleto per costruzione: il nuovo leader non saprebbe su quale segmento è caduta la ruota. UC1 richiede quindi un prerequisito che il documento non nomina: **promuovere lo stato del round in corso dentro il record**.

**C3 — Errore di correttezza nell'algoritmo, non individuato dal documento.** Nel piano il marker viaggia `snapshot@A → snapshot@B`, mentre i messaggi applicativi viaggiano `worker@A → wheel@B`. Sono **canali diversi**: Chandy-Lamport richiede che il marker sia emesso *dallo stesso processo mittente sullo stesso canale* dei messaggi applicativi. Con il routing del piano il canale `worker→wheel` non viene mai marcato: una bet spedita prima della registrazione e ricevuta dopo non finisce in nessuno stato di canale e viene semplicemente persa dal taglio. L'algoritmo così com'è **non è Chandy-Lamport**, è solo una propagazione di marker.

**C4 — UC2 costa più di «Medio-basso» (riga 57).** Il documento dà per acquisiti gli «identificativi espliciti di bet e round». Non esistono su nessuno dei due lati: `bets_queue` trasporta solo `{username, amount, segment}` ([WalletController.java:161-164](java-gateway/src/main/java/com/crazytime/controller/WalletController.java#L161-L164)) e l'`id` di `Bet` è un identity H2 che non viaggia mai su AMQP. UC2 attraversa il confine di linguaggio e tocca lo schema DB.

**C5 — Il bug lato Java più grave non è quello citato.** Il documento cita il match per importo di `RefundListener` (corretto, [RefundListener.java:65-76](java-gateway/src/main/java/com/crazytime/rabbitmq/RefundListener.java#L65-L76)), ma il difetto strutturale è `betRepository.findByStatus("PENDING")` a [PayoutListener.java:53](java-gateway/src/main/java/com/crazytime/rabbitmq/PayoutListener.java#L53): raccoglie **tutte** le bet PENDING di **tutti gli utenti e tutti i round**, e le risolve col winner del round corrente. Qualunque bet rimasta orfana viene pagata o persa arbitrariamente in un round successivo. È esattamente ciò che un ledger per-round elimina, ed è l'argomento più forte a favore di UC2.

**C6 — L'opzione C non è un'opzione separata.** «Round terminato» è banalmente noto al leader dal campo `phase`; diventa informazione non locale **solo dopo il crash del leader**, cioè si riduce a un campo del checkpoint di UC1. Va assorbita in A, non elencata a parte.

**C7 — L'opzione D impatta la Fase 3, non solo la Fase 4.** Se tutti i nodi consumano da `bets_queue`, `apply_role(standby)` non può più disattivare il `worker` ([implementation_plan.md:1248-1253](implementation_plan.md#L1248-L1253)). La leadership diventa proprietà del solo wheel/RNG, non dell'ingestione.

**C8 — Refusi.** Riga 67: `$	ext{betting}` è LaTeX rotto (tab + `ext{`), va `\text{betting}`. Il documento usa math inline `$...$` che non tutti i renderer Markdown supportano.

---

## Parte 2 — Architettura risultante

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

**Costo in latenza: zero.** Lo snapshot parte al gong e ha un budget di 5 s; la risoluzione del round è già schedulata a 10,5 s per l'animazione della ruota ([wheel_process.erl:180](erlang-engine/game_engine/src/wheel_process.erl#L180)). Il taglio è chiuso molto prima che serva.

**Ordine nel tick del gong** (importante per il recovery): si estrae **prima** il segmento vincente, **poi** si inizia lo snapshot. Così il taglio cattura `{bets, winner_segment, winner_index}` insieme, e un nuovo leader eletto dopo il crash ha sia l'insieme autorevole delle puntate sia l'esito autorevole: può **completare** il round anziché annullarlo.

---

## Parte 3 — Modifiche a `implementation_plan.md`

### Fase 2 — Multi-Node Erlang Cluster
- Aggiungere il bootstrap **Mnesia**: `mnesia:create_schema/1` sui nodi del cluster (idempotente, ignorare `{error, {_, {already_exists, _}}}`), `mnesia:start()`, creazione della tabella `snapshot_record` con `{disc_copies, AllNodes}` e attesa `mnesia:wait_for_tables/2`. Va fatto **dopo** la formazione del cluster, non in `init/1`.
- `cluster_manager` espone `get_participants/0` che restituisce la lista **ordinata e stabile** dei nodi vivi; è la lista che lo snapshot congela.
- Nota da aggiungere: con `rest_for_one`, `cluster_manager` è prima di `wheel_process`; un suo crash azzera il round in corso. Accettabile solo perché il round è ora ricostruibile dal checkpoint Mnesia.

### Fase 3 — Bully Leader Election — **modifica sostanziale**
- **`apply_role(standby)` NON deve più disattivare il `worker`** ([implementation_plan.md:1248-1253](implementation_plan.md#L1248-L1253)). L'ingestione delle bet è replicata su tutti i nodi; solo `wheel_process` resta leader-only. Rimuovere `gen_server:cast(worker, deactivate)` da `apply_role(standby)` e `gen_server:cast(worker, activate)` da `apply_role(leader)`.
- Il `worker` riceve invece `{set_leader, LeaderNode}` da `leader_election` (broadcast a tutti i nodi in `declare_victory/1`) e instrada le bet a `{wheel_process, LeaderNode}`.
- La sezione «Modified Files» va aggiornata: il flag `active` del worker (riga 1320) non serve più; serve un campo `leader`.
- Il flag `active` di `wheel_process` resta invariato.

### Fase 4 — Chandy-Lamport Snapshot — **riscritta**
Sostituire integralmente [implementation_plan.md:1330-1538](implementation_plan.md#L1330-L1538). Il contenuto è la Parte 4 di questo piano. Punti che cambiano rispetto al testo attuale:
- i marker viaggiano sui canali applicativi, emessi da `worker` e `wheel_process` (correzione C3);
- `snapshot.erl` è un collector, non un partecipante; `initiate_snapshot` diventa un **cast**, mai una `call`;
- lista partecipanti congelata all'avvio;
- handler mancanti aggiunti, più un abort timer **locale a ciascun partecipante** (se il collector muore, i partecipanti non restano in registrazione per sempre);
- `snapshot` va messo come **ultimo** figlio del supervisore, non prima di `wheel_process` come dice [implementation_plan.md:1533](implementation_plan.md#L1533): con `rest_for_one`, un crash del collector non deve azzerare il round.

### Fase 5 — Fault Tolerance — **modifica sostanziale**
- Sostituire «annulla il round e rimborsa tutte le PENDING» con un percorso a due rami, letto dall'ultimo `snapshot_record` su Mnesia:
  - **checkpoint presente per il round R e risultato non ancora pubblicato** → il nuovo leader **completa** il round R: ricarica `bets` dal ledger, riusa `winner_segment`/`winner_index` del taglio, calcola i payout, pubblica su `results_queue`. Nessun rimborso.
  - **nessun checkpoint per R** (crash durante la fase betting) → `round_cancelled` **con l'elenco esplicito dei `bet_id` del round R**, non un refund globale.
- Rimuovere da [implementation_plan.md:1633](implementation_plan.md#L1633) il `findByStatus("PENDING")` globale: rimborsa bet di round estranei.
- `wheel_process:handle_cast(activate, ...)` non deve più resettare incondizionatamente a `bets = []` ([implementation_plan.md:1603-1617](implementation_plan.md#L1603-L1617)): prima consulta il checkpoint.

### Fase 6 — Integration Testing
Sostituire la riga *«Snapshot during "No more bets" → Snapshot log shows consistent state capture»* (non falsificabile: il log si stampa anche con canali vuoti) con i test della Parte 6.

### Fase 0 / Java — aggiunta
`bet_id` UUID sul messaggio AMQP e sull'entity `Bet`. È il prerequisito di UC2 e la correzione alla radice dei bug 0.1.3 / 0.1.4, che le patch attuali affrontano solo per sintomo.

---

## Parte 4 — Modifiche al codice

### [NEW] `erlang-engine/game_engine/src/cl_recorder.erl`
Modulo di **funzioni pure** (nessun processo) con la logica Chandy-Lamport lato partecipante, condivisa da `wheel_process` e `worker` per non duplicarla.

```erlang
-record(cl, {
    id      = undefined,   %% undefined = non sta registrando
    local   = undefined,   %% stato locale salvato al taglio
    in_open = [],          %% canali entranti ancora in registrazione, [{Role, Node}]
    chan    = #{}          %% #{{Role,Node} => [Msg]} messaggi in transito registrati
}).

-export([new/0, is_recording/1, on_marker/5, on_app_msg/3, close/2, is_complete/1]).
```

- `on_marker(SnapId, From, InChannels, LocalState, CL) -> {NewCL, first_marker | subsequent}` — al primo marker salva `local`, apre la registrazione su `InChannels -- [From]`; ai successivi chiude il canale `From`. Il chiamante è responsabile di inviare i propri marker uscenti (deve farlo *lui*, perché il mittente deve essere il processo applicativo).
- `on_app_msg(From, Msg, CL) -> NewCL` — accoda a `chan[From]` **solo se** sta registrando e `From ∈ in_open`.
- `is_complete(CL) -> boolean()` — `in_open == []`.

### [MODIFY] `erlang-engine/game_engine/src/worker.erl`
1. State record: `#state{leader = undefined, cl = cl_recorder:new(), inflight = #{}}`.
2. `handle_cast({set_leader, Node})` — memorizza il leader.
3. **`process_message/1` non fa più `gen_server:call(wheel_process, ...)`** ([worker.erl:104](erlang-engine/game_engine/src/worker.erl#L104)): diventa
   `gen_server:cast({wheel_process, Leader}, {bet, BetMap})`. L'ack AMQP resta immediato come oggi.
4. **Nuovo handler marker** — sul canale entrante `wheel → worker`:
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
5. `handle_cast({bet_result, BetId, accepted | rejected})` — rimuove da `inflight`; passa da `cl_recorder:on_app_msg/3` se in registrazione.
6. **Rimuovere `publish_refund/1`** ([worker.erl:184-195](erlang-engine/game_engine/src/worker.erl#L184-L195)): i rimborsi non sono più decisi dal worker per singola bet, ma derivati dal ledger lato Java. Elimina la race refund-vs-payout descritta a C5.
7. `parse_bet_json/1` — estrarre anche `<<"bet_id">>`.

### [MODIFY] `erlang-engine/game_engine/src/wheel_process.erl`
1. **Prerequisito (correzione C2)** — promuovere lo stato del round nel record:
   ```erlang
   -record(state, {
       phase, time_left, round, bets, forced_segment, history, minigame_choices,
       active = false,
       winner_segment = undefined,   %% NEW
       winner_index   = undefined,   %% NEW
       minigame_mod   = undefined,   %% NEW
       minigame_details = undefined, %% NEW
       timer_ref      = undefined,   %% NEW  (send_after cancellabile/ispezionabile)
       cl = cl_recorder:new()        %% NEW
   }).
   ```
   Ogni `erlang:send_after` di fase salva la ref in `timer_ref`. Senza questo, nessun recovery è possibile: oggi il segmento vincente esiste solo dentro un messaggio in volo.
2. `handle_cast({bet, BetMap}, S)` — sostituisce `handle_call({place_bet, ...})`:
   - `phase = betting, active = true` → accetta, `cast` di `{bet_result, BetId, accepted}` al worker mittente;
   - **in registrazione e canale aperto** → `cl_recorder:on_app_msg/3` e **basta**: la bet finisce nello stato del canale e verrà unita a `bets` alla chiusura del taglio. Non va aggiunta anche a `bets` qui, altrimenti si conta due volte;
   - altrimenti → `{bet_result, BetId, rejected}`.
   Mantenere `handle_call({place_bet,...})` come alias deprecato non serve: nessun altro chiamante.
3. **Trigger del taglio** in `handle_info(tick, #state{phase = betting, time_left = 1})` ([wheel_process.erl:142-183](erlang-engine/game_engine/src/wheel_process.erl#L142-L183)) — **prima** estrarre il vincitore, **poi** iniziare:
   ```erlang
   %% ... estrazione WinnerIndex / WinnerSeg come oggi ...
   Participants = cluster_manager:get_participants(),          %% CONGELATA qui, una volta sola
   SnapId = snapshot:begin_snapshot(Participants, S#state.round),  %% cast, non call
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
7. **Non toccare** la `gen_server:call(Module, {play, BonusBets}, 10000)` verso i mini-game: fuori scope per la scelta fatta. Va però **documentato come limite noto**: durante il minigioco il wheel è bloccato fino a 10 s e non può partecipare a uno snapshot. Poiché l'unico trigger è al gong (fase `betting`), la finestra non si sovrappone mai — ma questo vincolo va scritto, perché è ciò che impedisce di aggiungere in futuro il trigger «transizione di fase» di UC1.

### [NEW] `erlang-engine/game_engine/src/snapshot.erl` — collector
```erlang
-record(state, {
    next_id = 0,
    running = #{}   %% #{SnapId => #run{round, participants, expected, parts, timer, degraded}}
}).
-export([start_link/0, begin_snapshot/2, get_last/0, get_for_round/1]).
```
- `begin_snapshot(Participants, Round)` — **cast**; assegna `SnapId` monotono, registra `expected = [{wheel, Leader} | [{worker,N} || N <- Participants]]`, arma `send_after(5000, {snapshot_timeout, SnapId})`.
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
    id,              %% chiave, intero monotono
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
4. **`GameResultListener.java:38-44`** — **dispatchare sul campo `type`**, oggi completamente ignorato: qualunque messaggio su `results_queue` viene trattato come risultato di round. Instradare `result` → `PayoutListener`, `round_ledger` → nuovo `LedgerListener`, `round_cancelled` → cancellazione per-round.
5. **[NEW] `rabbitmq/LedgerListener.java`** — `@Transactional`:
   - per ogni `bet_id` nel ledger: setta `Bet.round = R`, lascia `PENDING`;
   - per ogni `Bet` `PENDING` con `round = R` **assente** dal ledger: `REFUNDED` + accredito saldo, idempotente sul `betId`.
6. **`PayoutListener.java:53`** — `findByRoundAndStatus(round, "PENDING")` al posto del `findByStatus("PENDING")` globale; match per `bet_id` invece che per `username` ([PayoutListener.java:64](java-gateway/src/main/java/com/crazytime/rabbitmq/PayoutListener.java#L64)); rimuovere `it.remove()`.
7. **`RefundListener.java:65-76`** — eliminare il match per importo. Con il ledger la coda `refunds_queue` non riceve più nulla dal worker; mantenere il listener solo per compatibilità o rimuoverlo.
8. **Rimuovere** il `catch` che inghiotte le eccezioni in `PayoutListener:107-109` e `RefundListener:77-79`: essendo i metodi `@Transactional`, l'eccezione catturata non provoca rollback e i `save` parziali vengono committati.

### [MODIFY] `app.js`
Gestire `type === 'round_cancelled'` sul WebSocket (notifica + `fetchBalance()`), come già previsto dalla Fase 5.

---

## Parte 5 — Come risponde alla specifica

| Requisito (`idea_distributed.md` §1.2) | Copertura |
| :--- | :--- |
| «Snapshot per catturare lo stato globale consistente delle bet accettate sui **worker nodes**» | Ora letterale: N worker su nodi distinti, bet realmente in transito catturate negli stati dei canali. |
| «Garantire che nessuna puntata ulteriore venga processata» | Imposto dalla guardia di fase; lo snapshot **certifica** il taglio e ne produce l'artefatto verificabile. Il limite va dichiarato apertamente nella relazione: CL dà consistenza causale, non una barriera temporale. |
| «Eleggere un nuovo Dealer e riprendere il gioco senza corruzione di stato» | Il checkpoint su Mnesia permette di **completare** il round anziché annullarlo. Il rimborso resta come fallback, esplicitamente permesso dalla specifica. |
| «Migliaia di richieste concorrenti» | Competing consumers su `bets_queue` distribuiti su N nodi. |
| Mnesia nel diagramma architetturale (§1.3.1) | Finalmente usata. |

---

## Parte 6 — Verifica

**Compilazione e unit test**
```bash
cd erlang-engine/game_engine && ../../rebar3 compile && ../../rebar3 eunit
cd java-gateway && mvn test
```

**Test unitari nuovi su `cl_recorder`** (funzioni pure, banali da testare): primo marker apre i canali giusti; marker successivo chiude solo il proprio; `on_app_msg` accoda solo sui canali aperti; `is_complete` scatta esattamente quando tutti i canali sono chiusi.

**End-to-end, 3 nodi.** Avvio con `-sname game{1,2,3}@localhost -setcookie crazytime`, RabbitMQ attivo, gateway Java su :8080.

| Test | Risultato atteso |
| :--- | :--- |
| Bet piazzate da 3 browser durante `betting` | Distribuite sui 3 worker (visibile nei log per-nodo), tutte accettate dal wheel del leader |
| **Canali non vuoti** — piazzare bet negli ultimi 200 ms della fase betting | Il log dello snapshot mostra `in_flight_bets > 0` su almeno un canale. **È il test che falsifica la vacuità denunciata dall'analisi**: se resta sempre 0, la rifattorizzazione non ha prodotto canali reali. Forzabile deterministicamente con un `timer:sleep/1` iniettato nel worker sotto flag di debug |
| Ledger su `results_queue` | `type:"round_ledger"` con `bet_id` di **tutte** le bet, incluse quelle in transito |
| Bet in transito | Presente nel ledger **e** pagata correttamente; nessun refund |
| Bet piazzata dopo la chiusura del taglio | `rejected`, assente dal ledger, `REFUNDED` da `LedgerListener`, **mai** `WON` |
| Doppio importo, segmenti diversi | Nessuno scambio di attribuzione: verifica diretta del bug C5 / riga 74 dell'analisi |
| **Kill del leader dopo il gong, prima del payout** | Nuovo leader eletto, legge `snapshot_record` da Mnesia, **completa** il round col vincitore del taglio. Nessun rimborso, nessun round perso |
| Kill del leader durante `betting` | Nessun checkpoint per R → `round_cancelled` con i soli `bet_id` del round R; bet di round precedenti **non** toccate |
| Kill di un worker standby durante lo snapshot | Snapshot completato in `degraded` entro 5 s, ledger comunque pubblicato e deterministico |
| Kill del collector `snapshot` durante il taglio | `{cl_abort, _}` scatta sui partecipanti entro 10 s; nessun processo resta in registrazione |
| Invariante di conservazione | Su ogni `snapshot_record`: `Σ wallet + Σ bet_bloccate + Σ payout_in_volo` costante fra snapshot consecutivi |

**Ispezione Mnesia** — da una shell su un nodo standby, `mnesia:dirty_last(snapshot_record)` deve mostrare lo stesso record scritto dal leader: prova diretta della replica del checkpoint.

---

## Parte 7 — Ordine di esecuzione

1. **Prerequisiti** (indipendenti, nessuno dipende dallo snapshot): stato del round nel record di `wheel_process` (C2); `bet_id` UUID lato Java; `worker → wheel` da `call` a `cast` con `{bet_result, ...}` di ritorno.
2. **Fase 2 estesa**: bootstrap Mnesia + `cluster_manager:get_participants/0`.
3. **Fase 3 corretta**: worker attivo su tutti i nodi, `{set_leader, N}`.
4. **Fase 4 riscritta**: `cl_recorder` → partecipanti → collector `snapshot.erl` → persistenza Mnesia → pubblicazione ledger.
5. **UC2 lato Java**: dispatch su `type`, `LedgerListener`, `PayoutListener` per-round.
6. **Fase 5 corretta**: recovery dal checkpoint, fallback `round_cancelled` per-round.
7. **Fase 6**: test della Parte 6.

I passi 1-4 lasciano il sistema funzionante a ogni tappa. Il passo 5 è quello che rende lo snapshot **consumato**: fino ad allora resta un artefatto di log, cioè esattamente la critica dell'analisi.

---

## Parte 8 — Aggiornamenti a `snapshot_analisi.md`

Il documento resta valido come analisi; va emendato su:
- riga 43 e 122: rimuovere «ESISTE GIÀ» da G1 e la priorità che ne discende (C1);
- §UC1: aggiungere il prerequisito dello stato del round nel record (C2);
- §1 «Problemi implementativi»: aggiungere il routing errato dei marker come **difetto di correttezza**, non di completezza (C3);
- tabella riga 57: UC2 da «Medio-basso» a «Medio», con la motivazione del confine Java/Erlang (C4);
- §UC2: sostituire il match per importo con `findByStatus("PENDING")` globale come bug capofila (C5);
- righe 58 e 68: assorbire l'opzione C dentro A (C6);
- riga 59: annotare che D impatta la Fase 3 (C7);
- riga 67: correggere `$	ext{betting}` (C8);
- riga 30: aggiungere il caveat FIFO sulla riconnessione fra nodi.
