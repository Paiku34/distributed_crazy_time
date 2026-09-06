# Limiti noti e voci aperte

Raccolta unica di ciò che il sistema **non** garantisce, o garantisce a certe condizioni, per non doverlo ricostruire dai documenti di piano al momento di scrivere la relazione.

Ogni voce è **classificata**:

| Etichetta | Significato |
|---|---|
| **Scelta progettuale** | Conseguenza voluta di una decisione dichiarata. Va spiegata, non corretta. |
| **Limite accettato** | Comportamento della piattaforma (Mnesia, RabbitMQ, Erlang) che conosciamo e su cui abbiamo scelto di non intervenire. |
| **Difetto** | Qualcosa che, potendo rifarlo, faremmo diversamente. Dichiarato per onestà, con il perimetro entro cui è contenuto. |

Le voci sono ordinate per **gravità decrescente**: dalle prime, che limitano ciò che il sistema può promettere, alle ultime, che sono rumore o debito tecnico.

---

## Indice

**Alta — limitano ciò che il sistema può promettere**

| | Voce | Classificazione |
|---|---|---|
| [A1](#a1) | Gateway, broker e database non sono replicati | Scelta progettuale (perimetro) |
| [A2](#a2) | Quorum: consistenza sopra disponibilità | Scelta progettuale |
| [A3](#a3) | I messaggi pubblicati da Erlang non sono persistenti | Difetto |
| [A4](#a4) | Dopo una partizione, quale replica Mnesia sopravvive non lo decidiamo noi | Limite accettato |

**Media — contenute a valle, ma da conoscere**

| | Voce | Classificazione |
|---|---|---|
| [B1](#b1) | La disattivazione non ferma i timer di fase né azzera il round | Difetto (latente) |
| [B2](#b2) | Un round finito su minigioco non è completabile: si annulla | Scelta progettuale |
| [B3](#b3) | Ledger degradato: in dubbio non si rimborsa | Scelta progettuale |
| [B4](#b4) | Un nodo riavviato da solo non sempre carica la propria copia Mnesia | Limite accettato |
| [B5](#b5) | Il wheel resta bloccato fino a 10 secondi nel minigioco | Scelta progettuale |

**Bassa — rumore, debito tecnico, chiarimenti**

| | Voce | Classificazione |
|---|---|---|
| [C1](#c1) | Il consumer AMQP non viene cancellato alla demozione | Scelta progettuale |
| [C2](#c2) | Puntata rimborsata che ricompare sulla ruota | Scelta progettuale |
| [C3](#c3) | Chandy-Lamport dà consistenza causale, non una barriera temporale | Chiarimento |
| [C4](#c4) | JSON scritto e letto a mano | Debito tecnico |
| [C5](#c5) | Nessuna politica di ritenzione | Debito tecnico |
| [C6](#c6) | Elezioni a raffica all'avvio | Rumore |
| [C7](#c7) | `bet_forward_delay`: scaffolding di test lasciato nel codice | Scelta progettuale |
| [C8](#c8) | Nessun modello di sicurezza | Scelta progettuale (perimetro) |

[Cosa non è stato misurato](#cosa-non-è-stato-misurato)

---

# Alta

<a id="a1"></a>
## A1. Gateway, broker e database non sono replicati

**Classificazione: scelta progettuale (perimetro).**

**Cosa succede.** La tolleranza ai guasti riguarda **il solo engine Erlang**: tre nodi, elezione, quorum, checkpoint replicati. Tutto il resto gira in copia singola sulla stessa macchina — il gateway Spring Boot, il broker RabbitMQ e il database H2 su file. Se cade VM1, cade il sistema, per quanto l'engine sia replicato.

**Perché.** L'oggetto del progetto è l'engine distribuito: elezione, snapshot consistente, recovery. Replicare anche il livello di accesso avrebbe richiesto un cluster RabbitMQ, un database condiviso e un bilanciatore — cioè un progetto diverso, che non aggiunge nulla sui temi del corso.

**Da dichiarare.** È il vero limite di disponibilità del sistema, e va detto prima che lo dica qualcun altro: quello che dimostriamo è che *l'engine* sopravvive alla caduta di un nodo e a una partizione, non che il servizio nel suo insieme sia altamente disponibile.

**Attenzione a non assorbire [A3](#a3) qui dentro.** Questa voce riguarda la **disponibilità**: se VM1 non c'è, non c'è servizio. A3 riguarda la **durabilità attraverso un riavvio**, che è un'altra cosa e resta un problema anche replicando il broker.

---

<a id="a2"></a>
## A2. Quorum: consistenza sopra disponibilità

**Classificazione: scelta progettuale.**

**Cosa succede.** Il cluster elegge un leader solo se vede la **maggioranza** dei nodi configurati. Un lato di minoranza non gioca: i suoi worker rimettono le puntate nel broker.

**Perché.** Senza questa guardia una partizione 2-1 produrrebbe due leader, due ledger divergenti per lo stesso round e scritture concorrenti su Mnesia. Per un registro di scommesse la consistenza vale più della disponibilità: è il teorema CAP, non un difetto implementativo.

**Conseguenze.**
- Con 3 nodi il quorum è 2. **Un cluster a 2 nodi non ha quorum utile**: la partizione 1-1 blocca entrambi i lati.
- **Uso a nodo singolo**: `peer_nodes` elenca tutti e tre i nodi, quindi un solo nodo avviato con `-sname` vede quorum 1/3 e resta standby — il gioco non parte. Per sviluppare su un nodo solo: avviarlo **senza** `-sname`, oppure sovrascrivere la lista con `-game_engine peer_nodes "['game1@localhost']"`.

*Verificato*: con 1 nodo su 3 il superstite logga `Quorum 1/3 non raggiunto` e non si autoelegge; in partizione 2-1 il leader isolato si autoretrocede e la maggioranza elegge.

---

<a id="a3"></a>
## A3. I messaggi pubblicati da Erlang non sono persistenti

**Classificazione: difetto.**

**Cosa succede.** Le tre code sono dichiarate `durable` da entrambi i lati, ma l'engine pubblica senza impostare il `delivery_mode`: i suoi messaggi sono **transienti**. Un riavvio del broker con messaggi ancora in coda perde risultati di round, ledger e rimborsi. Il gateway Java, che usa `RabbitTemplate`, pubblica invece persistente per impostazione predefinita: l'asimmetria è involontaria.

**Perché conta.** Tutta la storia del recovery si appoggia sull'idea che il broker sia il punto in cui una puntata non ancora liquidata è al sicuro. Vale per le puntate in ingresso (pubblicate da Java, persistenti), ma non per gli esiti in uscita: una `results_queue` persa in un riavvio del broker lascia scommesse `PENDING` con il saldo già scalato.

**Perché non è una conseguenza di [A1](#a1).** L'obiezione naturale è che il broker sia comunque in copia singola, quindi una sua caduta sia già fatale. Non regge, per tre motivi:

1. **Classe di guasto diversa.** A1 è disponibilità: VM1 giù, servizio fermo, nulla di corrotto. A3 è durabilità: il broker **torna su**, il servizio riprende, e restano scommesse `PENDING` con il saldo già scalato. Il primo è un'interruzione, il secondo è un'incoerenza contabile che sopravvive al ripristino.
2. **Il trigger è ordinario.** Non serve che VM1 muoia: basta un riavvio del servizio, un reboot della macchina, un aggiornamento di pacchetto. Sono eventi che il sistema è progettato per superare — è esattamente il motivo per cui le code sono dichiarate `durable`.
3. **A1 sta mascherando A3, non assorbendolo.** Clusterizzando il broker, A3 non si risolve: un messaggio transiente non viene replicato, quindi un failover lo perde ugualmente. Anzi peggiora — oggi la perdita è accompagnata da un blackout evidente, con il broker in cluster diventerebbe silenziosa.

C'è poi un argomento interno alla nostra stessa architettura: le code sono dichiarate `durable` da entrambi i lati, e tutto il recovery si appoggia all'idea che il broker sia il posto in cui una puntata non liquidata è al sicuro (`exclude_bet_ids`, "rientrano dal broker"). È una promessa di durabilità che il progetto fa a sé stesso, e che il lato Erlang mantiene solo a metà. Vale anche con un broker solo.

**Cosa fare.** Una riga per pubblicazione (`#'P_basic'{delivery_mode = 2}`) in `rabbitmq_manager:publish/2`. Non è stato fatto: obbliga a rieseguire i test di round e rimborso, e il riavvio del broker non è uno scenario che il progetto dimostra.

---

<a id="a4"></a>
## A4. Dopo una partizione, quale replica Mnesia sopravvive non lo decidiamo noi

**Classificazione: limite accettato.**

**Cosa succede.** Dopo una partizione 2-1, riavviando i nodi, i checkpoint scritti dalla **maggioranza** durante la partizione non erano più leggibili: è rimasta la copia del nodo isolato.

**Cosa è osservato e cosa è inferito.** La perdita è un dato misurato: chiavi mancanti su due nodi e file `snapshot_record.DCD` identici e sovrascritti. Il **meccanismo** — quale replica Mnesia considera autoritativa al caricamento — è un'inferenza, e la sequenza del test (riavvio del nodo isolato per primo) può averla influenzata.

**Cosa resta vero in ogni caso.** Il quorum garantisce **un solo scrittore**, quindi la divergenza non viene *prodotta*; ma non decide quale replica vince al riavvio.

**Mitigazioni non implementate**: `{majority, true}` sulla tabella e `mnesia:set_master_nodes/2` per dichiarare la copia autoritativa prima di far ripartire il cluster. Cambierebbero la semantica delle scritture e imporrebbero di rifare tutti i test su Mnesia.

---

# Media

<a id="b1"></a>
## B1. La disattivazione non ferma i timer di fase né azzera il round

**Classificazione: difetto latente. Non raggiungibile nella topologia attuale.**

**Cosa succede.** `deactivate` sul `wheel_process` si limita a spegnere il flag `active`. Restano in piedi due cose:

1. i **timer di fase** già armati (l'attesa di 10,5 s per l'animazione, la risoluzione del minigioco). Se un nodo viene demosso in fase di spin, quel timer scatta comunque e il nodo **pubblica l'esito del round** pur essendo standby;
2. le **puntate del round in corso**, che restano nello stato. Se quel nodo torna leader più tardi, entrano nel ledger di un round successivo.

**Perché non produce danni.** L'invariante «solo il leader paga» viene comunque tenuta, ma **a valle**, dal gateway: i pagamenti si applicano solo alle scommesse ancora `PENDING`, quindi un esito duplicato o una puntata vecchia trovano righe già in stato terminale e non muovono denaro. Restano un ledger sporco e un messaggio di risultato di troppo.

**Perché non è raggiungibile oggi.** Serve che un nodo perda il quorum *mentre* resta capace di parlare col broker. Nella topologia di deploy (due nodi e il broker su VM1, uno su VM2) il lato che perde il quorum è sempre quello che perde anche il broker. La condizione si presenta solo nella partizione **logica** di test, che isola il solo livello di distribuzione Erlang.

**Cosa fare.** Cancellare `timer_ref` e azzerare `bets` in `deactivate` — il campo `timer_ref` esiste già proprio per questo, ma non viene mai usato.

---

<a id="b2"></a>
## B2. Un round finito su minigioco non è completabile: si annulla

**Classificazione: scelta progettuale.**

**Cosa succede.** Se il leader muore dopo il gong e il segmento estratto era un **minigioco**, il nuovo leader **annulla** il round invece di completarlo.

**Perché.** Il taglio cattura il segmento vincente, non l'esito del bonus: quello viene calcolato dopo, e se il leader è morto prima non esiste da nessuna parte. Un round senza esito non si può chiudere, quindi si rimborsa.

Il ramo «completa il round» vale quindi per i **moltiplicatori diretti**, che sono la maggioranza dei segmenti (21×1, 13×2, 7×5, 4×10 su 54).

---

<a id="b3"></a>
## B3. Ledger degradato: in dubbio non si rimborsa

**Classificazione: scelta progettuale.**

**Cosa succede.** Se il taglio si chiude per timeout senza tutti i partecipanti, il ledger viene marcato `degraded`. In quel caso il gateway **non** rimborsa le puntate pendenti del round che non compaiono nel ledger: le lascia `PENDING` e logga l'anomalia.

**Perché.** Regola R1: un ledger incompleto non è prova che una puntata sia stata esclusa. Rimborsare in dubbio significherebbe restituire i soldi di una scommessa che potrebbe essere stata giocata davvero.

**Conseguenza.** Dopo un taglio degradato può restare qualche bet `PENDING` da chiudere a mano. È il prezzo della prudenza, ed è la scelta giusta per un registro di scommesse.

---

<a id="b4"></a>
## B4. Un nodo riavviato da solo non sempre carica la propria copia Mnesia

**Classificazione: limite accettato.**

**Cosa succede.** Un nodo che riparte da solo carica subito la sua copia di `snapshot_record` **solo se era l'ultimo a essersi spento**. Altrimenti attende i nodi che possiedono le altre repliche, e i checkpoint non sono leggibili finché non tornano.

**Perché.** È il comportamento corretto di Mnesia: la copia locale potrebbe non essere la più recente, e caricarla significherebbe resuscitare dati vecchi.

**Cosa fare.** Il bootstrap lo dichiara nel log, nominando i nodi attesi, e **il gioco continua a funzionare** — manca solo il recovery dai checkpoint (e con esso la numerazione dei round ripresa dallo storico, che riparte dal contatore locale). Se quei nodi non torneranno più, `cluster_manager:force_load_snapshots/0` carica la copia locale, accettando di perdere ciò che gli altri avessero scritto nel frattempo. Da usare consapevolmente, non come prassi.

*Verificato*: entrambi i rami, incluso il recupero via force-load.

---

<a id="b5"></a>
## B5. Il wheel resta bloccato fino a 10 secondi nel minigioco

**Classificazione: scelta progettuale.**

**Cosa succede.** `wheel_process` chiama il modulo del minigioco con una `call` sincrona da 10 secondi di timeout. In quella finestra non risponde ad altre richieste.

**Tre conseguenze, tutte già gestite ma da conoscere.**
1. Il wheel **non può partecipare a uno snapshot** in quella finestra. Innocuo oggi, perché l'unico trigger è al gong in fase di puntata; è però ciò che impedirebbe di aggiungere un trigger «alla transizione di fase».
2. È la ragione per cui l'annullamento puntate è un **cast** e non una call: come chiamata cross-nodo andrebbe in timeout e farebbe crollare il worker.
3. È la ragione per cui il timeout delle bet in attesa di esito non può scendere sotto i ~12 secondi.

---

# Bassa

<a id="c1"></a>
## C1. Il consumer AMQP non viene cancellato alla demozione

**Classificazione: scelta progettuale.**

**Cosa succede.** Quando un nodo perde il quorum, la demozione spegne il `wheel_process` ma **non** cancella la sottoscrizione del worker a `bets_queue`. Il nodo isolato resta competing consumer: continua a prelevare puntate e a rimetterle subito in coda.

**Perché va bene.** Insieme alla demozione il worker perde il riferimento al leader, e in quello stato ogni messaggio viene **rifiutato e rimesso in coda immediatamente**, senza entrare nel meccanismo di attesa dell'esito. Non c'è quindi né ritardo né rischio di perdita: il messaggio rimbalza e viene servito da un nodo in maggioranza. Il costo è rumore nei log e qualche giro in più sul broker.

**Perché non abbiamo fatto il `basic.cancel`.** Cancellare e ri-sottoscrivere a ogni cambio di ruolo aggiunge una macchina a stati da riarmare correttamente in tutte le rielezioni, e introduce una finestra in cui quel nodo non consuma nulla. Il rimbalzo immediato ottiene lo stesso risultato senza stato aggiuntivo.

**Nota sull'unica finestra in cui si perde tempo davvero.** Fra la partizione fisica e il momento in cui Erlang la rileva, il worker ha ancora un riferimento al vecchio leader e gli inoltra puntate che non arriveranno mai: quelle attendono l'esito fino al timeout prima di tornare in coda. È una finestra di detection, non di demozione — e un `basic.cancel` legato al quorum non la coprirebbe, perché scatterebbe alla sua fine.

---

<a id="c2"></a>
## C2. Puntata rimborsata che ricompare sulla ruota

**Classificazione: scelta progettuale (regola R2).**

**Cosa succede.** Se il leader muore insieme al worker che teneva una puntata, quella puntata viene rimborsata; se poi il broker la riconsegna e finisce in un ledger successivo, l'utente **vede la puntata sulla ruota** pur avendo già riavuto i soldi.

**Cosa NON succede.** Non viene pagata: il gateway la trova in stato terminale, logga `replay_after_refund` e non la riapre. Nessuna duplicazione di denaro — solo un'incoerenza fra ciò che l'utente vede e ciò che ha in tasca.

---

<a id="c3"></a>
## C3. Chandy-Lamport dà consistenza causale, non una barriera temporale

**Classificazione: chiarimento concettuale, da dichiarare nella relazione.**

Il requisito «garantire che nessuna puntata ulteriore venga processata» è imposto dalla **guardia di fase** nel wheel, non dallo snapshot. Lo snapshot **certifica** il taglio e ne produce l'artefatto verificabile — il ledger autorevole del round — ma un algoritmo di snapshot consistente non è, e non può essere, un orologio globale.

---

<a id="c4"></a>
## C4. JSON scritto e letto a mano

**Classificazione: debito tecnico.**

**Cosa succede.** L'engine non usa una libreria JSON: costruisce i payload per concatenazione e li rilegge con espressioni regolari, con un escaping che copre solo virgolette e backslash. Va bene per i campi che produciamo noi, ma un **username con caratteri fuori dall'ordinario** (virgolette, a capo, caratteri di controllo) può produrre un payload che il gateway non riesce a rileggere, o farsi leggere male in ingresso.

**Perché non è esploso.** La registrazione non filtra il campo, ma nessun test lo ha stressato: con username normali il percorso è corretto end-to-end.

**Cosa fare.** Validare l'username lato gateway è la mitigazione da un minuto; usare `thoas` (già fra le dipendenze) è quella giusta.

---

<a id="c5"></a>
## C5. Nessuna politica di ritenzione

**Classificazione: debito tecnico.**

Né la tabella dei checkpoint su Mnesia né l'insieme dei `bet_id` già liquidati tenuto in memoria dal leader vengono mai potati: crescono con il numero di round giocati. Su una sessione di dimostrazione è irrilevante (ordine dei kilobyte); su un esercizio reale servirebbe una finestra di ritenzione.

---

<a id="c6"></a>
## C6. Elezioni a raffica all'avvio

**Classificazione: rumore.**

`cluster_manager` lancia un'elezione a ogni nodo che entra nel cluster: con tre nodi avviati insieme si vedono più elezioni di fila nei log. L'assegnazione del ruolo è idempotente e il recovery parte **solo** alla transizione standby → leader, quindi le rielezioni non hanno effetti collaterali.

---

<a id="c7"></a>
## C7. `bet_forward_delay`: scaffolding di test lasciato nel codice

**Classificazione: scelta progettuale.**

**Cos'è.** Un flag di configurazione nel worker che ritarda l'inoltro della scommessa al wheel. **Default 0, cioè disattivato.**

**A cosa serve.** La finestra in cui una puntata è davvero *in transito* al momento del taglio dura pochi millisecondi: senza questo flag il test «canali non vuoti» dipende dalla fortuna (in una prova, 0 catture su 40 puntate). Con `-game_engine bet_forward_delay 600` il caso diventa deterministico e si misura `in_flight_bets = 3`.

È volutamente rimasto: è ciò che rende **riproducibile** la dimostrazione che lo snapshot cattura qualcosa. Da citare come strumento di verifica, non come comportamento di esercizio.

---

<a id="c8"></a>
## C8. Nessun modello di sicurezza

**Classificazione: scelta progettuale (perimetro).**

Utenza `guest/guest` sul broker, cookie Erlang condiviso in chiaro, nessun TLS su AMQP né su HTTP, e chiunque possa pubblicare su `bets_queue` può forzare il segmento vincente (il comando esiste per i test; l'endpoint HTTP corrispondente è invece riservato all'admin). Sono impostazioni da laboratorio: il progetto non ha un modello di minaccia, e va detto invece di lasciarlo intendere.

---

## Cosa non è stato misurato

- **Partizione di rete con regole firewall.** Quella eseguita è una partizione **logica** (`net_kernel:allow/1` più disconnessione forzata): isola il livello di distribuzione Erlang, non lo stack di rete, quindi il nodo isolato continua a vedere il broker. Riproduce fedelmente il comportamento del cluster ed è il presupposto delle voci [B1](#b1) e [C1](#c1); una prova con `pfctl` o namespace richiede privilegi che i container non concedono.
- **Carico.** Il requisito «migliaia di richieste concorrenti» è coperto dall'architettura (competing consumers su N nodi) ma non è mai stato misurato. Va anche detto che l'ingestione è distribuita mentre **il wheel è unico**: il collo di bottiglia di progetto è lui, non i worker.
- **Riavvio del broker.** Nessun test tocca RabbitMQ: i quattro guasti iniettati (carico, partizione logica, `pkill` del dealer, ritardo di forward) colpiscono tutti l'engine, e il broker è trattato come dipendenza sempre viva. È il motivo per cui [A3](#a3) non poteva emergere dai test — un messaggio transiente si comporta come uno persistente finché il broker non muore, e il broker non protesta: pubblicare transiente su una coda durable è AMQP legale. Lo scenario che lo mostrerebbe: puntate piazzate, gateway fermo, riavvio del broker con `results_queue` non vuota.
- **Interfaccia.** Le verifiche sono state fatte via API e coda, non dal browser. Le animazioni dei minigiochi e il banner di round annullato sono stati letti nel codice, non visti a schermo.
