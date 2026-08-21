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
- **Manca la registrazione dei canali (`onBasicMsg`)**: il record ha il campo `channel_states` ma nessun percorso di codice vi accoda messaggi. `maps:size(channel_states)` stamperà sempre 0. È implementata solo la propagazione dei marker.
- **Handler assenti**: `handle_cast({snapshot_complete, ...})`, `handle_call(get_snapshot, ...)` e `handle_info({snapshot_timeout, Id}, ...)` non esistono, pur essendo il timeout armato con `send_after`. La raccolta delle porzioni locali non avviene.
- **Topologia non congelata**: `nodes()` viene ricalcolato in punti diversi; un `nodeup`/`nodedown` durante lo snapshot rende `recording_channels` incoerente. Chandy-Lamport assume topologia fissa e fortemente connessa.
- **I marker non attraversano RabbitMQ**: il piano indica come canale C1 il percorso `bets_queue -> worker -> wheel_process`, ma i marker viaggiano solo su canali Erlang. Le puntate in transito nel broker — l'unica cosa che si intendeva catturare — restano fuori dallo snapshot.
- **Accoppiamento fragile**: `snapshot` esegue `gen_server:call` verso `wheel_process:get_bets()` mentre `wheel_process` può trovarsi dentro una call verso `snapshot`.

> **Verdetto:** Allo stato attuale lo snapshot è ridondante: cattura uno stato già interamente disponibile in locale sul leader, su canali vuoti per costruzione, e il suo risultato non alimenta alcuna decisione del sistema. Inoltre non risponde alla motivazione dichiarata nella specifica (*«garantire che nessuna puntata ulteriore venga processata»*); un algoritmo di snapshot gira on-the-fly senza fermare il sistema, fornisce consistenza causale e non una barriera temporale, e per definizione include nel taglio i messaggi in transito anziché escluderli.

> **Nota a favore:** Erlang garantisce l'ordinamento dei messaggi tra una data coppia di processi: l'ipotesi FIFO, cruciale per la correttezza di Chandy-Lamport, è quindi soddisfatta all'interno del cluster senza artifici. La base teorica regge; manca l'applicazione a un grafo di processi in cui ci sia davvero qualcosa da catturare.

---

## 2. Dove lo snapshot risolve un problema reale

**Criterio di selezione:** lo snapshot è utile solo dove lo stato è:
1. **Distribuito** su più processi;
2. **Collegato** da canali asincroni.

Nel sistema esistono tre grafi di questo tipo:

```
G1: [wheel_process] <── intra-nodo, canali mailbox Erlang ──> [4 mini-game]      [ESISTE GIÀ]

G2: [leader]        <── inter-nodo ─────────────────────────> [nodi standby]     [VUOTO per design]

G3: [gateway Java]  <── fuori dal dominio dei marker ───────> [Erlang / RabbitMQ] [NON copribile]
```

Il piano punta interamente su **G2**, l'unico privo di contenuto. Le opzioni seguenti sfruttano **G1** e **G3**.

### Riepilogo delle opzioni di implementazione

| Opz. | Contenuto | Grafo | Trigger | Costo |
| :---: | :--- | :--- | :--- | :---: |
| **A** | Checkpoint consistente del round replicato sugli standby | G1 + replica su G2 | Ogni transizione di fase | Medio |
| **B** | Ledger autoritativo delle puntate del round | `worker -> wheel`, export verso Java | «No more bets» | Medio-basso |
| **C** | Rilevazione della terminazione del round (predicato stabile) | G1 | Fine fase payout | Basso |
| **D** | Raccolta distribuita delle puntate su più nodi | G2 reso non vuoto | «No more bets» | Alto |

---

### UC1 — Recovery senza perdere il round (Opzione A + C)

- **Problema:** La Fase 5 al crash del dealer scarta il round: pubblica `round_cancelled`, e `handle_cast(activate, ...)` riparte con $\text{bets} = []$ e $\text{minigame\_choices} = []$. Il rimborso lato Java, inoltre, usa `findByStatus("PENDING")`, cioè rimborsa tutte le puntate pendenti del sistema, non solo quelle del round interrotto.
- **Perché serve uno snapshot consistente:** Un checkpoint naif (interrogo `wheel_process`, poi `crazytime`) può fotografare un istante in cui il wheel ha già registrato la delega del bonus mentre il mini-game non ha ancora ricevuto il messaggio: il checkpoint conterrebbe un messaggio ricevuto ma mai spedito, cioè un taglio inconsistente. Al ripristino il nuovo leader pagherebbe il bonus due volte o non lo pagherebbe affatto. È esattamente la condizione $a \rightarrow b$ della prova di correttezza.
- **Soluzione (A):** Snapshot periodico sul grafo `wheel <-> 4 mini-game` a ogni transizione di fase ($	ext{betting} \rightarrow \text{spinning} \rightarrow \text{minigame} \rightarrow \text{payout}$), con stati dei canali che catturano deleghe e scelte dei giocatori in volo. Il risultato viene replicato sugli standby; al `nodedown` il nuovo leader eletto riprende il round.
- **Ruolo di C:** Il nuovo leader deve sapere da dove ripartire: se nell'ultimo snapshot il predicato di terminazione era già vero, il round $N$ era chiuso e si passa al successivo; altrimenti si riprende dalla fase catturata. Senza C non si distingue «crash dopo il payout» da «crash durante il mini-game», e si ricade nel `round_cancelled`.

---

### UC2 — Consistenza tra Erlang e DB Java (Opzione B + D)

- **Problema:** I due sottosistemi hanno due idee diverse di quali puntate appartengano al round $N$. Java deduce il saldo e persiste la Bet come `PENDING`; Erlang decide autonomamente se accettarla o rimborsarla. I bug FIX 0.1.3 (bet rimborsata che resta `PENDING` e viene poi ripagata) e FIX 0.1.4 (payout aggregato riscosso più volte) sono sintomi della stessa causa: nessuno dei due lati possiede l'insieme autorevole delle puntate del round. La patch proposta per il refund resta fragile, perché identifica la bet per importo (`bet.getAmount().compareTo(amount) == 0` seguito da `break`) e sbaglia bersaglio quando un giocatore ha due puntate di pari importo su segmenti diversi.
- **Soluzione (B):** Al «No more bets» lo snapshot produce l'insieme causalmente consistente delle puntate accettate — inclusi i messaggi in transito tra `worker` e `wheel_process` — e lo si pubblica su `results_queue` come ledger del round $N$, con identificativi espliciti di bet e round. Java riconcilia in modo deterministico: bet presente nel ledger $\rightarrow$ eleggibile al payout; bet assente $\rightarrow$ refund. Sparisce il matching per importo e sparisce la divergenza di stato.
- **Ruolo di D:** Con un solo nodo che raccoglie, Java potrebbe in linea di principio fidarsi della lista locale del leader. Se invece tutti i nodi consumano da `bets_queue` come competing consumers (opzione D), l'unica lista corretta è quella ricostruita dallo snapshot: B diventa necessario anziché comodo. D indirizza inoltre il requisito *«thousands of concurrent bet requests»* della specifica, che il modello active/standby non affronta.
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
1. **$A + C$ per primi:** non richiedono di modificare la Fase 3, perché il grafo `wheel <-> 4 mini-game` esiste già e fornisce subito stati di canale non vuoti; soprattutto, il risultato dello snapshot viene finalmente consumato dal recovery della Fase 5.
2. **A seguire B:** rimuove la causa comune di due bug della Fase 0.
3. **Infine D:** solo se resta margine di tempo.

### Correzioni comunque necessarie rispetto alla Fase 4 attuale:
- Registrazione degli stati dei canali (`onBasicMsg`);
- Aggiunta degli handler mancanti (`snapshot_complete`, `get_snapshot`, `snapshot_timeout`);
- Congelamento della lista dei nodi all'avvio dello snapshot;
- Sostituzione della call sincrona verso `wheel_process` con il passaggio dello stato o una lettura da ETS.

---

*Documento di analisi interna Distributed Crazy Time, DSMT 2025/2026.*  
*Riferimenti: `implementation_plan.md` (Fasi 3-5), `Idea distributed.md` (§1.2), slide del corso su snapshot e algoritmi di Chandy-Lamport e Lai-Yang.*