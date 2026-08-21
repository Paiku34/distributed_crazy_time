# Snapshot Chandy-Lamport in Distributed Crazy Time
### Analisi dell'uso attuale previsto dall'Implementation Plan (Fase 4) e proposte di impiego
*DSMT, Università di Pisa*

---

## 1. Come viene usato lo snapshot allo stato attuale del piano

### Quando scatta
Un'unica volta per round, nella clausola `handle_info(tick, 1)` di `wheel_process`, cioè all'istante del «No more bets». Il leader chiama `snapshot:initiate_snapshot(State#state.bets)`, registra il proprio stato locale e diffonde i marker via `gen_server:cast({snapshot, N}, {marker, ...})` a tutti i nodi in `nodes()`.

### Che cosa cattura
La Fase 3 introduce il modello active/standby: `apply_role(standby)` disattiva sia `wheel_process` sia `worker`, e `place_bet` sui nodi non leader risponde `{error, not_leader}`. Di conseguenza, per costruzione:
- Solo il leader possiede puntate; gli standby hanno $\text{bets} = []$.
- Gli standby non consumano da `bets_queue`, quindi nessun messaggio applicativo transita tra i nodi Erlang: gli stati dei canali sono necessariamente vuoti.
- Il «taglio consistente» ottenuto vale $[\text{bets del leader}] \cup \emptyset$.

### Chi ne consuma il risultato
**Nessuno.** La funzione `get_snapshot/0` è esportata ma non viene invocata da alcun modulo. Il calcolo dei payout continua a usare `State#state.bets` locale. La Fase 5 (Fault Tolerance) non legge lo snapshot: al crash del dealer pubblica `round_cancelled`, rimborsa tutte le bet `PENDING` e riparte con $\text{bets} = []$. L'unico effetto osservabile dello snapshot è una riga di log, come conferma la checklist di test di Fase 6 (*«Snapshot log shows consistent state capture»*).

### Problemi implementativi nel codice proposto
- **Routing errato dei marker — difetto di correttezza, non di completezza**: nel piano i marker viaggiano `snapshot@A -> snapshot@B`, mentre i messaggi applicativi viaggiano `worker@A -> wheel_process@B`. Sono **canali diversi**, mentre Chandy-Lamport richiede che il marker sia emesso dallo stesso processo mittente e sullo stesso canale dei messaggi applicativi. Con questo instradamento il canale `worker -> wheel` non viene mai marcato: una bet spedita prima della registrazione e ricevuta dopo non finisce in alcuno stato di canale e viene semplicemente persa dal taglio. Ciò che il piano descrive non è Chandy-Lamport: è una pura propagazione di marker. Gli altri punti di questo elenco sono lacune di implementazione; questo invalida l'algoritmo anche se implementato per intero.
- **Manca la registrazione dei canali (`onBasicMsg`)**: il record ha il campo `channel_states` ma nessun percorso di codice vi accoda messaggi. `maps:size(channel_states)` stamperà sempre 0. È implementata solo la propagazione dei marker.
- **Handler assenti**: `handle_cast({snapshot_complete, ...})`, `handle_call(get_snapshot, ...)` e `handle_info({snapshot_timeout, Id}, ...)` non esistono, pur essendo il timeout armato con `send_after`. La raccolta delle porzioni locali non avviene.
- **Topologia non congelata**: `nodes()` viene ricalcolato in punti diversi; un `nodeup`/`nodedown` durante lo snapshot rende `recording_channels` incoerente. Chandy-Lamport assume topologia fissa e fortemente connessa.
- **I marker non attraversano RabbitMQ**: il piano indica come canale C1 il percorso `bets_queue -> worker -> wheel_process`, ma i marker viaggiano solo su canali Erlang. Le puntate in transito nel broker — l'unica cosa che si intendeva catturare — restano fuori dallo snapshot.
- **Accoppiamento fragile**: `snapshot` esegue `gen_server:call` verso `wheel_process:get_bets()` mentre `wheel_process` può trovarsi dentro una call verso `snapshot`.

> **Verdetto:** Allo stato attuale lo snapshot è ridondante: cattura uno stato già interamente disponibile in locale sul leader, su canali vuoti per costruzione, e il suo risultato non alimenta alcuna decisione del sistema. Inoltre non risponde alla motivazione dichiarata nella specifica (*«garantire che nessuna puntata ulteriore venga processata»*); un algoritmo di snapshot gira on-the-fly senza fermare il sistema, fornisce consistenza causale e non una barriera temporale, e per definizione include nel taglio i messaggi in transito anziché escluderli.

> **Nota a favore:** Erlang garantisce l'ordinamento dei messaggi tra una data coppia di processi: l'ipotesi FIFO, cruciale per la correttezza di Chandy-Lamport, è quindi soddisfatta all'interno del cluster senza artifici. Con un caveat da dichiarare: la garanzia vale **a coppie di processi** e **decade se la connessione fra due nodi cade e si riforma** — cioè proprio nello scenario di crash e riconnessione che la Fase 5 intende gestire. La base teorica regge; manca l'applicazione a un grafo di processi in cui ci sia davvero qualcosa da catturare.

---

## 2. Dove lo snapshot risolve un problema reale

**Criterio di selezione:** lo snapshot è utile solo dove lo stato è:
1. **Distribuito** su più processi;
2. **Collegato** da canali asincroni.

Nel sistema esistono tre grafi di questo tipo:

```
G1: [wheel_process] <── intra-nodo, canali mailbox Erlang ──> [4 mini-game]      [VUOTO oggi]

G2: [leader]        <── inter-nodo ─────────────────────────> [nodi standby]     [VUOTO per design]

G3: [gateway Java]  <── fuori dal dominio dei marker ───────> [Erlang / RabbitMQ] [NON copribile]
```

Il piano punta interamente su **G2**, vuoto per design. Va però detto subito che **anche G1 è vuoto allo stato attuale del codice**, e non solo per design: il canale `wheel <-> mini-game` è una `gen_server:call` **sincrona**, e i 4 mini-game sono **stateless** (`init/1` ritorna `#{}` e lo stato resta `#{}` per sempre). Uno snapshot su G1 così com'è catturerebbe 4 stati locali vuoti e 4 canali vuoti: esattamente la vacuità che si rimprovera a G2. G1 diventa un grafo con contenuto **solo dopo** aver reso asincrono `wheel <-> minigame` e aver promosso lo stato del round dentro il record di `wheel_process` — un lavoro dello stesso ordine di grandezza dell'opzione D, non un vantaggio già acquisito. Le opzioni seguenti sfruttano **G1** (una volta reso asincrono) e **G3**.

### Riepilogo delle opzioni di implementazione

| Opz. | Contenuto | Grafo | Trigger | Costo |
| :---: | :--- | :--- | :--- | :---: |
| **A** | Checkpoint consistente del round replicato sugli standby, comprensivo del predicato di terminazione | G1 reso asincrono + replica su G2 | Ogni transizione di fase | Medio |
| **B** | Ledger autoritativo delle puntate del round | `worker -> wheel`, export verso Java | «No more bets» | Medio |
| **D** | Raccolta distribuita delle puntate su più nodi | G2 reso non vuoto | «No more bets» | Alto |

Tre note sulla tabella:

- **Il costo di B è «Medio», non «Medio-basso».** Il ledger presuppone identificativi espliciti di bet e di round, che **non esistono su nessuno dei due lati**: `bets_queue` trasporta solo `{username, amount, segment}` e l'`id` di `Bet` è un identity H2 che non viaggia mai su AMQP. B attraversa quindi il confine di linguaggio Java/Erlang e tocca lo schema del DB.
- **Non compare un'opzione C autonoma.** «Round terminato» è banalmente noto al leader dal campo `phase`; diventa informazione non locale **solo dopo il crash del leader**, cioè si riduce a un campo del checkpoint di A. Va assorbita in A, non elencata a parte.
- **D impatta la Fase 3, non solo la Fase 4.** Se tutti i nodi consumano da `bets_queue`, `apply_role(standby)` non può più disattivare il `worker`: la leadership si riduce alla proprietà del solo wheel/RNG e non copre più l'ingestione.

---

### UC1 — Recovery senza perdere il round (Opzione A)

- **Problema:** La Fase 5 al crash del dealer scarta il round: pubblica `round_cancelled`, e `handle_cast(activate, ...)` riparte con $\text{bets} = []$ e $\text{minigame\_choices} = []$. Il rimborso lato Java, inoltre, usa `findByStatus("PENDING")`, cioè rimborsa tutte le puntate pendenti del sistema, non solo quelle del round interrotto.
- **Prerequisito non eliminabile:** Lo stato del round in corso **non è nel record**. `WinnerSegment`, `WinnerIndex`, `Details` e `BonusBets` vivono soltanto dentro i messaggi schedulati con `send_after`, e nessuna `TimerRef` viene memorizzata. Uno snapshot di `#state{}` preso durante `spinning` o `minigame` è quindi incompleto per costruzione: il nuovo leader non saprebbe nemmeno su quale segmento è caduta la ruota. Prima di UC1 vanno fatte due cose che non appartengono allo snapshot ma senza le quali esso non ha nulla da catturare: **promuovere lo stato del round dentro il record di `wheel_process`** e **rendere asincrono il canale `wheel <-> minigame`**.
- **Perché serve uno snapshot consistente:** Un checkpoint naif (interrogo `wheel_process`, poi `crazytime`) può fotografare un istante in cui il wheel ha già registrato la delega del bonus mentre il mini-game non ha ancora ricevuto il messaggio: il checkpoint conterrebbe un messaggio ricevuto ma mai spedito, cioè un taglio inconsistente. Al ripristino il nuovo leader pagherebbe il bonus due volte o non lo pagherebbe affatto. È esattamente la condizione $a \rightarrow b$ della prova di correttezza.
- **Soluzione (A):** Snapshot periodico sul grafo `wheel <-> 4 mini-game` a ogni transizione di fase ($\text{betting} \rightarrow \text{spinning} \rightarrow \text{minigame} \rightarrow \text{payout}$), con stati dei canali che catturano deleghe e scelte dei giocatori in volo. Il risultato viene replicato sugli standby; al `nodedown` il nuovo leader eletto riprende il round.
- **Predicato di terminazione (ex opzione C, assorbita in A):** Il nuovo leader deve sapere da dove ripartire: se nell'ultimo snapshot il predicato di terminazione era già vero, il round $N$ era chiuso e si passa al successivo; altrimenti si riprende dalla fase catturata. Senza questo campo non si distingue «crash dopo il payout» da «crash durante il mini-game», e si ricade nel `round_cancelled`. Non è però un'opzione a sé: finché il leader è vivo il predicato è leggibile in locale dal campo `phase`, e diventa informazione non locale solo dopo il crash — cioè è un campo del checkpoint di A, non un secondo algoritmo.

---

### UC2 — Consistenza tra Erlang e DB Java (Opzione B + D)

- **Problema:** I due sottosistemi hanno due idee diverse di quali puntate appartengano al round $N$. Java deduce il saldo e persiste la Bet come `PENDING`; Erlang decide autonomamente se accettarla o rimborsarla. I bug FIX 0.1.3 (bet rimborsata che resta `PENDING` e viene poi ripagata) e FIX 0.1.4 (payout aggregato riscosso più volte) sono sintomi della stessa causa: nessuno dei due lati possiede l'insieme autorevole delle puntate del round. Il difetto capofila è `betRepository.findByStatus("PENDING")` in `PayoutListener`: la query raccoglie **tutte** le bet pendenti di **tutti** gli utenti e di **tutti** i round, e le risolve con il segmento vincente del round corrente. Qualunque bet rimasta orfana — perché il suo round è stato annullato, o perché il refund non le è mai stato applicato — viene pagata o persa arbitrariamente in un round successivo. È esattamente ciò che un ledger per-round elimina, ed è l'argomento più forte a favore di UC2. Secondario ma reale: anche la patch proposta per il refund resta fragile, perché identifica la bet per importo (`bet.getAmount().compareTo(amount) == 0` seguito da `break`) e sbaglia bersaglio quando un giocatore ha due puntate di pari importo su segmenti diversi.
- **Soluzione (B):** Al «No more bets» lo snapshot produce l'insieme causalmente consistente delle puntate accettate — inclusi i messaggi in transito tra `worker` e `wheel_process` — e lo si pubblica su `results_queue` come ledger del round $N$, con identificativi espliciti di bet e round. Java riconcilia in modo deterministico: bet presente nel ledger $\rightarrow$ eleggibile al payout; bet assente $\rightarrow$ refund. Sparisce il matching per importo e sparisce la divergenza di stato.
- **Ruolo di D:** Con un solo nodo che raccoglie, Java potrebbe in linea di principio fidarsi della lista locale del leader. Se invece tutti i nodi consumano da `bets_queue` come competing consumers (opzione D), l'unica lista corretta è quella ricostruita dallo snapshot: B diventa necessario anziché comodo. D indirizza inoltre il requisito *«thousands of concurrent bet requests»* della specifica, che il modello active/standby non affronta. Va messo in conto che D **impatta la Fase 3**, non solo la Fase 4: con tutti i nodi consumatori, `apply_role(standby)` non può più disattivare il `worker` e la leadership resta proprietà del solo wheel/RNG.
- **Limite da dichiarare:** I marker non attraversano RabbitMQ: le puntate ferme nel broker restano fuori dal taglio. Va assunto esplicitamente che il broker sia il confine del sistema snapshottato, oppure va risolto con piggybacking alla Lai-Yang, propagando il `round_id` come header AMQP.

---

### UC3 — Logging distribuito, dispute sui pagamenti, post-mortem (A + B persistiti)

- **Problema:** In caso di contestazione di un pagamento o di crash, non esiste alcun artefatto che descriva lo stato del sistema in un istante ben definito: i log per-processo sono locali e non correlabili tra loro.
- **Soluzione:** Non è un'opzione nuova: sono A e B che, invece di restare strutture in memoria, diventano un artefatto durevole. Il delta implementativo è contenuto: `snapshot_id` monotono e `round_id` in ogni record; persistenza su Mnesia `disc_copies` replicata (Mnesia compare nel diagramma architetturale della specifica ma non è oggi utilizzata) oppure su una `audit_queue` AMQP dedicata con `delivery_mode = 2`; conservazione tanto degli stati locali quanto degli stati dei canali, senza i quali il post-mortem non può dire che cosa fosse in volo.
- **Perché serve uno snapshot e non un log per processo:** Un invariante globale è verificabile solo su un taglio consistente: su un taglio inconsistente il denaro in transito appare perso o duplicato, generando falsi allarmi. Su ogni snapshot si possono invece verificare invarianti reali:
  - **Conservazione del denaro:** $\sum \text{wallet} + \sum \text{bet\_bloccate} + \sum \text{payout\_in\_volo} = \text{costante}$;
  - **Nessuna bet contemporaneamente `PAID` e `REFUNDED`**;
  - **Nessuna bet esterna al ledger che riceva un payout**.
- **Dimostrazione di correttezza:** Per la correttezza di Chandy-Lamport la sequenza di snapshot costituisce una run valida del sistema; se gli invarianti valgono su ciascuno stato di quella run, si ha un'argomentazione formale che il sistema ha operato correttamente. Per il post-mortem, il confronto tra l'ultimo snapshot pre-crash e lo stato del DB Java dopo il failover isola esattamente ciò che è andato perso.
- **Limite da dichiarare:** Lo snapshot non prova la fairness temporale. Alla contestazione *«la mia puntata è arrivata prima del gong»* Chandy-Lamport non risponde: cattura un taglio causalmente consistente, uno stato che il sistema potrebbe non aver mai attraversato in alcun istante di tempo globale. Per le dispute temporali servono timestamp del gateway o clock di Lamport propagati come header AMQP. Ciò che lo snapshot dimostra è più debole ma sufficiente: il ledger del round è completo, non duplicato e riproducibile, e nessuna puntata è stata creata o persa attraverso il taglio.

---

## 3. Indicazione operativa

I tre use case si coprono con un solo modulo, purché progettato con due punti di innesco e tre consumatori anziché uno:

```
                  ┌─────────────────────────────────┐
                  │          snapshot.erl           │
                  └───────┬─────────────────┬───────┘
                          │                 │
             trigger 1:   │                 │ trigger 2:
    transizione di fase   │                 │ "No more bets"
                          ▼                 ▼
             ┌─────────────────┐       ┌─────────────────┐
             │       UC1       │       │       UC2       │
             │ (recovery round)│       │  (ledger Java)  │
             └────────┬────────┘       └────────┬────────┘
                      │                         │
                      └───────────┬─────────────┘
                                  ▼
                    ┌───────────────────────────┐
                    │            UC3            │
                    │   (audit e post-mortem)   │
                    │  ogni snapshot su Mnesia  │
                    │        disc_copies        │
                    └───────────────────────────┘
```

### Priorità consigliata:
1. **B per prima:** è l'unica opzione che produce stati di canale non vuoti senza dover prima riscrivere un grafo, e rimuove la causa comune dei bug di pagamento della Fase 0 (a partire da `findByStatus("PENDING")` globale). Il costo sta tutto nell'introdurre `bet_id` e `round_id` espliciti su entrambi i lati del confine Java/Erlang.
2. **A seguire A** (con il predicato di terminazione assorbito): richiede due prerequisiti non banali — rendere asincrono `wheel <-> minigame` e promuovere lo stato del round dentro il record — ma è ciò che fa finalmente **consumare** lo snapshot dal recovery della Fase 5.
3. **Infine D:** solo se resta margine di tempo, tenendo presente che tocca la Fase 3 e non solo la Fase 4.

> La priorità è invertita rispetto a una prima lettura che dava A+C per primi «perché il grafo `wheel <-> 4 mini-game` esiste già»: quella premessa è falsa (canale sincrono, mini-game stateless), e A richiede lo stesso ordine di lavoro preparatorio di D.

### Correzioni comunque necessarie rispetto alla Fase 4 attuale:
- **Spostamento dei marker sui canali applicativi reali** (`worker <-> wheel_process`, non `snapshot <-> snapshot`): senza questo l'algoritmo non è Chandy-Lamport e le altre correzioni non lo rendono corretto;
- Registrazione degli stati dei canali (`onBasicMsg`);
- Aggiunta degli handler mancanti (`snapshot_complete`, `get_snapshot`, `snapshot_timeout`);
- Congelamento della lista dei nodi all'avvio dello snapshot;
- Sostituzione della call sincrona verso `wheel_process` con il passaggio dello stato o una lettura da ETS.

---

*Documento di analisi interna Distributed Crazy Time, DSMT 2025/2026.*  
*Riferimenti: `implementation_plan.md` (Fasi 3-5), `Idea distributed.md` (§1.2), slide del corso su snapshot e algoritmi di Chandy-Lamport e Lai-Yang.*