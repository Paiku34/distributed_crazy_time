# Riassunto — Implementazione dello Snapshot Chandy-Lamport

Sintesi ad alto livello delle modifiche descritte in [snapshot_implementation_plan.md](snapshot_implementation_plan.md), organizzate nelle fasi in cui vanno eseguite.

---

## Stato del codice — da dove si parte

> **Aggiornato al commit `6ef3b73`.** La prima stesura di questo riassunto era scritta sul codice della sola **Fase 1** (AMQP nativo). Nel frattempo sono state implementate anche la **Fase 2** (cluster manager) e la **Fase 3** (elezione del leader con algoritmo Bully), ma **seguendo il piano originale**, cioè senza applicare prima le correzioni strutturali che questo piano richiedeva. Parte di quel lavoro va quindi corretta, non aggiunta.

Tre categorie:

- **Già fatto e utilizzabile così com'è**: la connessione AMQP con pubblicazione lock-free, l'ack manuale e — prerequisito importante — la **possibilità di rifiutare un messaggio** rimettendolo in coda, che nella prima stesura figurava fra le cose da aggiungere. Inoltre **ogni nodo è già sottoscritto alla coda delle scommesse**: la topologia a *competing consumers* esiste già a livello di broker, quindi il lavoro da fare è più piccolo del previsto.
- ✅ **Corretto** (era la Fase 0 qui sotto, ora eseguita): l'elezione non tocca più il worker, che resta attivo su tutti i nodi e instrada al leader; il rifiuto con rimessa in coda avviene solo quando nessun leader è noto; ed è entrata in funzione la guardia di quorum. Verificato su un cluster a 3 nodi.
- **Ancora da scrivere**: tutto il resto — identificativo univoco di bet, ack differito, persistenza su Mnesia, quorum, snapshot vero e proprio, riconciliazione lato Java.

Le conclusioni di questo piano sono state **riportate in `implementation_plan.md`**, e il lavoro sul codice è iniziato: sono completate la **Fase 0 (retrofit)**, la **Fase 1 (prerequisiti)** e la parte di **Fase 3** relativa al quorum, oltre a metà della **Fase 6** (lato Java). Restano Mnesia, lo snapshot vero e proprio, la regola R3 e il recovery.

Un chiarimento che cambia un'argomentazione della prima stesura: oggi il worker inoltra la scommessa al processo della ruota con una **chiamata sincrona locale** e conferma al broker solo dopo la risposta. La finestra di perdita descritta più avanti quindi **non esiste ancora**: verrebbe *introdotta* dal passaggio a comunicazione asincrona, ed è l'ack differito a chiuderla prima che si apra.

---

## Obiettivo e idea di fondo

Lo snapshot previsto originariamente dalla Fase 4 era **ridondante**: catturava uno stato già interamente disponibile in locale sul leader, su canali vuoti per costruzione, e nessuno ne consumava il risultato.

Il rifacimento nasce da una sola idea: **spostare i marker sui canali applicativi reali e rendere quei canali asincroni**, così che ci sia davvero qualcosa da catturare. L'obiettivo è rendere lo snapshot *load-bearing*, cioè un algoritmo il cui risultato non è ottenibile in altro modo e che alimenta due decisioni reali del sistema:

1. il **ledger autorevole del round** pubblicato verso il gateway Java;
2. il **recovery al crash del dealer**, che può completare il round anziché annullarlo.

### Architettura risultante

- L'ingestione delle bet diventa **distribuita**: tutti i nodi consumano dalla coda delle scommesse come *competing consumers* e inoltrano le puntate in modo **asincrono** al wheel del leader.
- Ne risulta un grafo **fortemente connesso** (stella bidirezionale) con canali asincroni e FIFO: il canale `worker → wheel` contiene le bet in transito al momento del gong — l'unica cosa che la specifica chiede di catturare e l'unica non disponibile in locale da nessuna parte.
- **Partecipanti allo snapshot** sono il processo wheel e gli N worker. Il modulo `snapshot` diventa un puro **collector**: assegna l'id, congela la lista dei partecipanti, arma il timeout, raccoglie le porzioni, persiste e pubblica il ledger. Non interroga mai i partecipanti, sono loro a fare push.
- Gli snapshot vengono persistiti su **Mnesia replicata**, colmando anche il divario con il diagramma architetturale della specifica, dove Mnesia compare ma non era mai stata usata.
- **Costo in latenza nullo**: lo snapshot parte al gong con un budget di 5 secondi, mentre la risoluzione del round è già schedulata più tardi per l'animazione della ruota.
- **Ordine importante nel tick del gong**: prima si estrae il segmento vincente, poi si avvia lo snapshot. Così il taglio cattura insieme puntate ed esito, e un nuovo leader eletto dopo un crash ha entrambi.

---

## Fase 0 — Retrofit di ciò che le Fasi 2-3 hanno già scritto — ✅ FATTA

Il passo che rimette il codice sulla traiettoria di questo piano. È stato eseguito e verificato: 6 scommesse pubblicate sulla coda si distribuiscono fra i tre worker e arrivano **tutte** al wheel del solo leader; l'annullamento puntate rimborsa correttamente; alla caduta del leader se ne elegge un altro, e un nodo rimasto solo su tre si rifiuta di autoeleggersi.

- **L'elezione non deve più attivare e disattivare il worker.** L'ingestione delle bet è replicata su tutti i nodi; solo il processo della ruota resta esclusivo del leader. I due comandi verso il worker vanno rimossi e sostituiti dalla **comunicazione dell'identità del leader corrente**, propagata sia da chi vince l'elezione sia da chi riceve l'annuncio del nuovo coordinatore.
- **Il flag «attivo/passivo» del worker sparisce**, e con esso il rifiuto sistematico dei messaggi sui nodi passivi. Il discriminante non è più il ruolo del nodo ma la presenza di un leader noto: si rimette un messaggio in coda **solo quando nessuno può servirlo** (nessun leader eletto, o nodo finito nella minoranza di una partizione). Sparisce così anche il rimbalzo continuo fra broker e standby introdotto dalla Fase 3.
- **Il cluster manager espone due nuove liste** (vedi Fase 2) e **delega all'elezione la decisione sulla caduta di un nodo**: oggi lancia un'elezione a ogni evento di topologia, incondizionatamente, mentre con il quorum quella rielezione non deve avvenire quando ci si trova nella minoranza. Il controllo va dove risiedono il ruolo corrente e il conteggio del quorum, cioè nel modulo di elezione.

> Nota a margine, indipendente dallo snapshot ma resa visibile dal quorum: l'elezione registra se ne ha già una in corso ma non lo controlla mai, e il cluster manager ne lancia una a ogni nodo che si connette. Con tre nodi che partono insieme si ottiene una raffica di elezioni innocua ma rumorosa: vale la pena aggiungere la guardia mentre si tocca il modulo.

---

## Fase 1 — Prerequisiti — ✅ FATTA

Modifiche indipendenti dallo snapshot, da fare per prime perché tutto il resto vi si appoggia. **L'ordine interno conta**, ed è stato rispettato: identificativo di bet prima dell'ack differito, percorso di annullamento spostato sul nuovo evento prima di rimuovere la vecchia coda dei rimborsi.

- **Identificatore univoco di bet**: un `bet_id` UUID generato da Java, presente sia sul messaggio AMQP sia sull'entità persistita, mentre il numero di round resta assegnato in modo autoritativo da Erlang. È il prerequisito di tutto il resto e la correzione alla radice di bug finora affrontati solo per sintomo.
- **Stato del round promosso nello stato del processo wheel**: oggi segmento vincente, indice e dettagli del minigioco vivono solo dentro messaggi temporizzati in volo. Senza questo, nessun recovery è possibile.
- ✅ **Possibilità di rifiutare messaggi AMQP** (oltre al solo ack): **già disponibile** nel gestore della connessione. Resta l'unica avvertenza di sempre — il worker deve passare dal gestore e mai dal canale AMQP diretto, perché il canale cambia a ogni riconnessione e solo il gestore sa qual è quello valido.
- **Comunicazione worker → wheel da sincrona ad asincrona**, con l'esito che torna indietro come messaggio dedicato.
- **Ack differito** (vedi sotto) e **deduplica per `bet_id`**.
- **Nuovo evento di rifiuto puntuale di una singola bet**, con il relativo handler lato Java, **incluso il percorso di annullamento puntate (UNDO)**, che deve passare da questo evento *prima* che venga rimossa la vecchia coda dei rimborsi.

### Durabilità: l'ack differito

Il canale è passato ad asincrono **insieme** all'ack differito, quindi la finestra di perdita non si è mai aperta. Il ragionamento, per memoria: con la vecchia chiamata sincrona la conferma al broker significava già «il wheel ha deciso»; passando alla comunicazione asincrona cross-nodo quella garanzia si sarebbe persa e fra la conferma e l'arrivo del messaggio la bet **non esisterebbe in nessuno stato replicato** — è uscita dal broker, il wallet è già stato addebitato, e se il leader crasha in fase di puntata la bet è persa in silenzio: il giocatore ha pagato, il sistema non sa che esiste.

**Rimedio**: l'ack viene differito fino alla risposta del wheel, conservando anche dopo il passaggio ad asincrono il significato che ha oggi. Se il leader muore prima di rispondere, il broker riconsegna automaticamente i messaggi non ackati a un worker vivo e la bet entra nel round successivo — strettamente meglio di un rimborso. Il broker torna a essere il buffer durevole che è. Il rischio introdotto (riconsegna di una bet già accettata il cui ack si è perso) è coperto dalla deduplica per `bet_id`: senza l'UUID questo rimedio non sarebbe praticabile.

Il worker acquisisce inoltre un **timeout sulle bet in attesa di risposta**: allo scadere rimette la puntata in coda invece di perderla.

---

## Fase 2 — Infrastruttura di cluster e persistenza

Il cluster manager esiste già: qui si tratta di estenderlo.

- ❌ **Bootstrap di Mnesia** (unico punto ancora aperto di questa fase) con join dinamico in due rami distinti (primo nodo che crea lo schema; nodo che si aggiunge a un cluster dove la tabella esiste già), sempre dopo la formazione del cluster — il punto d'aggancio naturale è lo stesso ritardo che oggi precede la prima elezione, mai l'avvio del processo. Il passo che si dimentica più spesso è la conversione dello schema su disco: senza, un nodo perde la propria copia a ogni riavvio, vanificando la persistenza. Va inoltre dichiarata la dipendenza da Mnesia nella configurazione dell'applicazione, oggi assente.
- ✅ Il gestore del cluster espone due liste distinte:
  - la lista **ordinata e stabile dei nodi vivi**, che è quella che lo snapshot congela all'avvio del taglio;
  - la lista **statica dei nodi configurati**, che è il denominatore del quorum. Usare la lista dei nodi correntemente connessi renderebbe la guardia inutile, perché in partizione si riduce da sola.
  Entrambe vanno **intersecate con l'elenco dei nodi configurati**: il monitoraggio include anche i nodi nascosti e le shell diagnostiche, che altrimenti regalerebbero quorum al lato sbagliato e diventerebbero partecipanti fantasma dello snapshot. La lista statica **non richiede nuova configurazione**: l'elenco completo dei tre nodi è già presente fra i parametri dell'applicazione, va solo riesposto includendo il nodo locale, che oggi viene filtrato all'avvio.
- **Sottoscrizione agli eventi di sistema di Mnesia** con log rumoroso in caso di database inconsistente.
- Va documentato che il checkpoint esiste **solo dopo il gong**: copre quindi il crash nelle fasi successive, mentre un crash in fase di puntata ricade sul percorso di riconciliazione (ack differito + annullamento selettivo). Le due protezioni sono complementari, nessuna basta da sola.

---

## Fase 3 — Elezione del leader e quorum — ✅ FATTA

Modifica sostanziale rispetto al piano originale, e in parte correzione di codice già scritto (vedi Fase 0). Tutto ciò che segue è ora nel codice.

- **Il worker resta attivo su tutti i nodi**, non solo sul leader: l'ingestione delle bet è replicata, solo il processo wheel resta esclusivo del leader.
- Il worker riceve dall'elezione l'identità del leader corrente e **instrada verso il leader tutti i percorsi dei messaggi in ingresso**, non solo le bet. È la conseguenza meno ovvia dell'ingestione distribuita e la più facile da dimenticare: comandi come la scelta del minigioco, l'annullamento delle puntate e i comandi del pannello dev, se consumati da uno standby, finirebbero al wheel dormiente di quel nodo e sparirebbero in silenzio — con 3 nodi, circa due volte su tre.
- Due comandi finora sincroni vanno **convertiti in asincroni**, perché come chiamate sincrone verso un altro nodo andrebbero in timeout quando il wheel è bloccato nel minigioco, facendo crollare il worker.

### Guardia di quorum contro lo split-brain

L'elezione scatta alla caduta di un nodo senza distinguere un **crash** da una **partizione di rete**. In una partizione 2-1 entrambi i lati eleggono un leader. Finché il danno era il solo estrattore casuale duplicato era contenuto; con questa architettura diventa grave: due lati pubblicano ledger concorrenti e divergenti per lo stesso round, entrambi scrivono su Mnesia provocando un'inconsistenza che richiede riparazione manuale, e l'audit trail perde ogni valore probatorio.

**Rimedio**: una guardia di maggioranza applicata in **due punti, non uno**:

1. quando un nodo *sta per diventare* leader;
2. quando un leader **già in carica** rileva la caduta di un nodo — questo è il punto che conta davvero, perché il leader isolato nella minoranza non ripassa mai dall'elezione e resterebbe attivo a produrre il ledger divergente.

Una precisazione rispetto alla prima stesura: **la caduta dei nodi è rilevata dal cluster manager**, non dal modulo di elezione. Anziché duplicare la sottoscrizione agli eventi di rete, è il cluster manager a notificare l'elezione, che decide se rieleggere o autoretrocedersi.

Il nodo che perde il quorum si autoretrocede a standby; i suoi worker, non avendo più un leader, rimettono le bet nel broker, che le farà servire dalla maggioranza. Nessuna bet persa, nessun ledger divergente, e con al più un leader nessuna scrittura concorrente su Mnesia.

Due accorgimenti a corredo: la chiave dei record di snapshot include l'iniziatore, così due leader concorrenti produrrebbero record **distinti e diagnosticabili** invece di sovrascriversi in silenzio; e le inconsistenze di Mnesia vanno loggate esplicitamente.

> Con 3 nodi il quorum è 2. Un cluster a **2 nodi non può avere quorum utile**: la partizione 1-1 blocca entrambi i lati. È una limitazione onesta da dichiarare nella relazione — è il teorema CAP, non un difetto implementativo: qui si sceglie la consistenza sulla disponibilità, che per un ledger di scommesse è la scelta giusta.

---

## Fase 4 — Lo snapshot Chandy-Lamport riscritto

Ha **sostituito integralmente** la Fase 4 di `implementation_plan.md`, che è stato aggiornato di conseguenza: quel documento descrive ora il progetto qui riassunto. Cosa cambia rispetto alla stesura precedente:

- **I marker viaggiano sui canali applicativi reali**, emessi dai worker e dal wheel, non fra istanze del modulo snapshot: è la correzione che rende l'algoritmo effettivamente Chandy-Lamport. Il marker deve partire dal processo applicativo stesso, così da condividere mailbox e ordine FIFO con i messaggi che deve delimitare.
- Il modulo snapshot è un **collector**, non un partecipante, e viene avviato in modo asincrono. Non interroga mai i partecipanti: sono loro a inviargli la propria porzione. Ne consegue che la funzione di lettura delle bet che il piano originale prevedeva di aggiungere al wheel **non va aggiunta** — sarebbe una chiamata sincrona verso un processo che durante il minigioco resta bloccato fino a 10 secondi, e farebbe cadere il collector per timeout.
- La **lista dei partecipanti è congelata** all'avvio del taglio, una volta sola, e presa dal cluster manager anziché dall'elenco dei nodi connessi: quest'ultimo può cambiare fra l'invio dei marker e la verifica di completamento.
- Viene finalmente **popolato lo stato dei canali**: nel piano originale il campo esisteva ma nessun handler vi accodava messaggi, il che è la dimostrazione formale della vacuità denunciata dall'analisi.
- Sono aggiunti gli handler mancanti e un **timer di abort locale a ciascun partecipante**: se il collector muore, nessuno resta in registrazione per sempre.
- Il collector diventa l'**ultimo figlio del supervisore**: con la strategia di ripartenza adottata, un suo crash non deve azzerare il round in corso. È l'unica modifica all'albero di supervisione, che per il resto resta quello attuale.

### Componenti

- **Un modulo di funzioni pure** con la logica Chandy-Lamport lato partecipante (apertura/chiusura dei canali entranti, accodamento dei messaggi in transito, completamento del taglio), condivisa da wheel e worker per non duplicarla. Essendo puro, è anche l'unica parte banalmente testabile in isolamento.
- **Il wheel** avvia il taglio al gong, dopo aver estratto il vincitore, salva il proprio stato locale (round, bet, esito) ed emette i marker verso tutti i worker. Alla chiusura del taglio, **le bet in transito entrano nel round**: sono state spedite prima che il worker apprendesse del taglio, quindi per il taglio causale appartengono al round corrente.
- **Ogni worker**, al primo marker, salva le proprie bet non ancora ackate, emette il proprio marker verso il wheel e — avendo un solo canale entrante — riporta immediatamente al collector.
- **Il collector** raccoglie le porzioni, oppure conclude in modalità **degradata** allo scadere del timeout indicando i partecipanti mancanti; il ledger resta comunque deterministico. Poi persiste il record su Mnesia (replicato automaticamente su tutti i nodi) e, solo sul leader, pubblica il ledger del round.
- **Il record persistito** conserva round, istante, iniziatore, flag di degradazione, fase, esito, stati locali, stati dei canali e il ledger autorevole. Stati locali e stati dei canali sono conservati integralmente: senza di essi il post-mortem non potrebbe dire cosa fosse in volo.

---

## Fase 5 — Riconciliazione e regole di correttezza

L'ack differito e l'annullamento del round si contraddicono se lasciati impliciti: il primo dice che le bet non ackate vengono **rigiocate**, il secondo che le bet pendenti del round caduto vengono **rimborsate**. Una bet che cade in entrambe le descrizioni verrebbe rimborsata *e* giocata.

Al crash del leader in fase di puntata, ogni bet del round sta in **uno solo** di tre insiemi:

| Insieme | Come si riconosce | Destino corretto |
| :--- | :--- | :--- |
| **Ackata** — accettata dal wheel morto | assente dai worker e dal broker | **Rimborso**: è l'unico caso realmente perso |
| **Non ackata** — consumata ma senza esito | ancora tracciata da un worker vivo | **Replay**: il broker la riconsegna, entra nel round successivo |
| **Mai consumata** | ancora nella coda | **Replay**: idem |

Il nuovo leader raccoglie dai worker superstiti le bet non ackate proprio per separare il primo insieme dagli altri due. Da qui tre regole da rispettare ovunque:

- **R1 — Chi rimborsa.** Si rimborsa solo una bet assente da ogni ledger *e* dall'insieme delle bet escluse. In dubbio non si rimborsa: resterà pendente e sarà chiusa dal ledger del round in cui verrà rigiocata.
- **R2 — Chi è autoritativo.** Lo stato lato Java è autoritativo sui **movimenti di denaro**, il ledger Erlang sull'**esito di gioco**. Una bet già rimborsata che ricompare in un ledger successivo non viene mai pagata; va però loggata e chiusa in stato terminale, perché è l'unico caso in cui l'utente vede sulla ruota una puntata che gli è stata restituita — un'anomalia visiva da documentare, non una duplicazione di denaro.
- **R3 — L'insieme di deduplica è l'unione dei ledger persistiti**, non la lista delle bet in memoria. Quest'ultima viene azzerata a ogni round, quindi da sola garantisce l'idempotenza soltanto *dentro la finestra del round*: una riconsegna che arriva nel round successivo verrebbe accettata di nuovo, giocata due volte e pagata due volte. È la regola che rende davvero sicuro l'ack differito. Il wheel mantiene perciò un insieme delle bet già liquidate negli ultimi round, ripopolato dai record persistiti **all'avvio e a ogni elezione** — è proprio dopo un crash che le riconsegne arrivano.

### Recovery a due rami

L'approccio «annulla il round e rimborsa tutto» viene sostituito da un percorso a due rami, deciso leggendo l'ultimo checkpoint su Mnesia:

- **Checkpoint presente e risultato non ancora pubblicato** → il nuovo leader **completa** il round: ricarica le bet dal ledger, riusa l'esito catturato nel taglio, calcola i payout e pubblica. Nessun rimborso.
- **Nessun checkpoint** (crash durante la fase di puntata) → annullamento del **solo** round interessato, ma **non** di tutte le sue bet: il messaggio di annullamento porta con sé l'elenco delle bet da escludere (quelle che il broker riconsegnerà), e Java rimborsa solo le pendenti di quel round che non vi compaiono.

Due dettagli del piano originale vanno corretti mentre si tocca questa fase: il frammento di recovery **reintroduce la disattivazione del worker** proprio nel momento in cui il nuovo leader si attiva, vanificando l'ingestione distribuita; e il flag «il leader precedente è crashato», tenuto nel dizionario di processo dell'elezione, va **sostituito** dalla lettura del checkpoint — un flag in memoria non sopravvive al riavvio del processo e soprattutto non dice *quale* round è stato interrotto, informazione indispensabile per annullare un solo round anziché tutti.

Viene inoltre eliminato il rimborso globale su tutte le bet pendenti, che colpiva anche round estranei. Da notare che il reset incondizionato delle bet alla riattivazione del wheel, previsto dalla Fase 5 originale, **non è mai stato implementato**: la riattivazione conserva le bet. Non va quindi introdotto nella forma descritta là, ma direttamente in quella corretta — consultare prima il checkpoint.

---

## Fase 6 — Allineamento del gateway Java e del frontend — 🔧 A METÀ

✅ Già fatti: identificativo univoco sulla bet con serializzazione vera, ricerche per identificativo e per round, dispatch sul tipo di messaggio, handler del rifiuto puntuale (idempotente), rimozione della vecchia coda dei rimborsi e del suo listener.
❌ Restano: handler del ledger con le regole R1/R2, payout per round, rimozione dei blocchi che inghiottono le eccezioni, notifica di round annullato sul frontend.

- **Entità e API**: nuovo campo identificativo univoco sulla bet, generato all'accettazione e incluso nel messaggio; ricerche per identificativo e per round + stato. Contestualmente, la costruzione del JSON passa a una serializzazione vera invece della concatenazione di stringhe (oggi l'username non viene mai escapato).
- **Dispatch sul tipo di messaggio**, oggi completamente ignorato: qualunque messaggio in arrivo viene trattato come risultato di round. I quattro tipi (risultato, ledger, annullamento round, rifiuto puntuale) vanno instradati ai rispettivi handler.
- **Nuovo handler del ledger**, transazionale, che implementa R1 e R2: conferma il round autoritativo sulle bet presenti, non riapre né paga quelle già rimborsate, e rimborsa in modo idempotente le pendenti del round assenti dal ledger.
- **Nuovo handler del rifiuto puntuale**: rimborsa una singola bet identificata dal suo UUID, e solo se ancora pendente. L'idempotenza è la guardia sullo stato stesso. È la sostituzione deterministica del vecchio meccanismo che riconciliava i rimborsi **per importo** — fragile per costruzione, perché due puntate di pari importo su segmenti diversi sono indistinguibili.
- **Payout per round e per identificativo di bet**, al posto della scansione globale delle pendenti e del match per username.
- **Rimozione della vecchia coda dei rimborsi** e del listener relativo, ma **solo dopo** che il percorso di annullamento puntate è passato al nuovo evento: altrimenti l'utente annulla, le bet spariscono dalla ruota e i soldi non tornano — una regressione funzionale silenziosa.
- **Rimozione dei blocchi che inghiottono le eccezioni** nei listener transazionali: l'eccezione catturata non provoca rollback e i salvataggi parziali finiscono committati.
- **Frontend**: gestione della notifica di round annullato con aggiornamento del saldo.

---

## Copertura della specifica

| Requisito | Copertura |
| :--- | :--- |
| Snapshot dello stato globale consistente delle bet sui **worker nodes** | Ora letterale: N worker su nodi distinti, bet realmente in transito catturate negli stati dei canali |
| Garantire che nessuna puntata ulteriore venga processata | Imposto dalla guardia di fase; lo snapshot **certifica** il taglio e ne produce l'artefatto verificabile. Da dichiarare apertamente: Chandy-Lamport dà consistenza causale, non una barriera temporale |
| Eleggere un nuovo dealer e riprendere senza corruzione di stato | Il checkpoint permette di **completare** il round anziché annullarlo; il rimborso resta come fallback |
| Migliaia di richieste concorrenti | Competing consumers distribuiti su N nodi |
| Mnesia nel diagramma architetturale | Finalmente usata |

---

## Limite noto da documentare

Durante il minigioco il wheel resta bloccato fino a 10 secondi in una chiamata sincrona e non risponde ad altre chiamate. Le conseguenze sono tre: non può partecipare a uno snapshot in quella finestra (innocuo oggi, perché l'unico trigger è al gong, ma è ciò che impedirebbe di aggiungere in futuro un trigger sulla transizione di fase); è la ragione per cui l'annullamento puntate va convertito in messaggio asincrono; ed è la ragione per cui il timeout delle bet in attesa di esito non può scendere sotto una certa soglia.
