# Limiti noti e voci aperte

Raccolta unica di ciò che il sistema **non** garantisce, o garantisce a certe condizioni, per non doverlo ricostruire dai quattro documenti di piano al momento di scrivere la relazione.

Tutti i punti qui sotto sono **consapevoli**: o sono conseguenze di scelte progettuali dichiarate, o sono comportamenti di Mnesia e di Erlang che abbiamo osservato durante i test. Nessuno è un difetto lasciato indietro per fretta.

Ogni voce dice **dove si manifesta**, **perché**, e **cosa fare** se dà fastidio.

---

## 1. Consistenza scelta sopra disponibilità: il quorum

**Cosa succede.** Il cluster elegge un leader solo se vede la **maggioranza** dei nodi configurati. Un lato di minoranza non gioca: i suoi worker rimettono le puntate nel broker.

**Perché.** Senza questa guardia, una partizione 2-1 produrrebbe due leader, due ledger divergenti per lo stesso round e scritture concorrenti su Mnesia. Per un registro di scommesse la consistenza vale più della disponibilità — è il teorema CAP, non un difetto implementativo.

**Conseguenze da dichiarare.**
- Con 3 nodi il quorum è 2. **Un cluster a 2 nodi non ha quorum utile**: la partizione 1-1 blocca entrambi i lati.
- **Uso a nodo singolo**: `peer_nodes` elenca tutti e tre i nodi, quindi un solo nodo avviato con `-sname` vede quorum 1/3 e resta standby — il gioco non parte. Per sviluppare su un nodo solo: avviarlo **senza** `-sname` (un nodo non distribuito non può essere in partizione, la guardia non si applica) oppure sovrascrivere la lista con `-game_engine peer_nodes "['game1@localhost']"`.

*Verificato*: con 1 nodo su 3 il superstite logga `Quorum 1/3 non raggiunto` e non si autoelegge; in partizione 2-1 il leader isolato si autoretrocede e la maggioranza elegge.

---

## 2. Mnesia: un nodo riavviato da solo non sempre carica la propria copia

**Cosa succede.** Un nodo che riparte da solo carica subito la sua copia di `snapshot_record` **solo se era l'ultimo a essersi spento**. Altrimenti attende i nodi che possiedono le altre repliche, e i checkpoint non sono leggibili finché non tornano.

**Perché.** È il comportamento corretto di Mnesia: la copia locale potrebbe non essere la più recente, e caricarla significherebbe resuscitare dati vecchi.

**Cosa fare.** Il bootstrap lo dichiara esplicitamente nel log, nominando i nodi attesi, e **il gioco continua a funzionare** (manca solo il recovery dai checkpoint). Se quei nodi non torneranno più, `cluster_manager:force_load_snapshots/0` carica la copia locale — accettando di perdere ciò che gli altri avessero scritto nel frattempo. Da usare consapevolmente, non come prassi.

*Verificato*: entrambi i rami, incluso il recupero via force-load.

---

## 3. Mnesia: se un cluster partizionato viene riavviato, quale copia sopravvive lo decide Mnesia

**Cosa succede.** Dopo una partizione 2-1, riavviando i nodi, i checkpoint scritti dalla **maggioranza** durante la partizione non erano più leggibili: è rimasta la copia del nodo isolato.

**Cosa è osservato e cosa è inferito.** La perdita è un dato misurato: chiavi mancanti su due nodi e file `snapshot_record.DCD` identici e sovrascritti. Il **meccanismo** che l'ha causata è un'inferenza — la scelta che Mnesia fa al caricamento su quale replica sia autoritativa — e la sequenza del test (riavvio del nodo isolato prima degli altri) può averla influenzata.

**Cosa resta vero in ogni caso.** Il quorum garantisce **un solo scrittore**, quindi la divergenza non viene *prodotta*; ma non decide quale replica vince al riavvio.

**Mitigazioni**, se il punto vi interessa: l'opzione `{majority, true}` sulla tabella e `mnesia:set_master_nodes/2` per dichiarare la copia autoritativa prima di far ripartire il cluster. Nessuna delle due è implementata: cambierebbero la semantica delle scritture e imporrebbero di rifare i test su Mnesia.

---

## 4. Il wheel resta bloccato fino a 10 secondi durante il minigioco

**Cosa succede.** `wheel_process` chiama il modulo del minigioco con `gen_server:call(Module, {play, BonusBets}, 10000)`. In quella finestra non risponde ad altre chiamate.

**Tre conseguenze, tutte già gestite ma da conoscere.**
1. Il wheel **non può partecipare a uno snapshot** in quella finestra. Innocuo oggi, perché l'unico trigger è al gong in fase di puntata; è però ciò che impedirebbe di aggiungere in futuro un trigger «alla transizione di fase».
2. È la ragione per cui l'annullamento puntate (`undo_bets`) è un **cast** e non una call: come chiamata cross-nodo andrebbe in timeout e farebbe crollare il worker.
3. È la ragione per cui il timeout delle bet in attesa di esito (`inflight_timeout`) non può scendere sotto i ~12 secondi.

---

## 5. Chandy-Lamport dà consistenza causale, non una barriera temporale

**Da dichiarare apertamente nella relazione.** Il requisito «garantire che nessuna puntata ulteriore venga processata» è imposto dalla **guardia di fase** nel wheel, non dallo snapshot. Lo snapshot **certifica** il taglio e ne produce l'artefatto verificabile — il ledger autorevole del round — ma un algoritmo di snapshot consistente non è, e non può essere, un orologio globale.

---

## 6. Recovery: un round finito su un minigioco non è completabile

**Cosa succede.** Se il leader muore dopo il gong e il segmento estratto era un **minigioco**, il nuovo leader **annulla** il round invece di completarlo.

**Perché.** Il taglio cattura il segmento vincente, non l'esito del bonus: quello viene calcolato dopo, e se il leader è morto prima non esiste da nessuna parte. Un round senza esito non si può chiudere, quindi si rimborsa.

Il ramo «completa il round» vale quindi per i **moltiplicatori diretti**, che sono la maggioranza dei segmenti (21×1, 13×2, 7×5, 4×10 su 54).

---

## 7. Ledger degradato: in dubbio non si rimborsa

**Cosa succede.** Se il taglio si chiude per timeout senza tutti i partecipanti, il ledger viene marcato `degraded`. In quel caso il gateway **non** rimborsa le puntate pendenti del round che non compaiono nel ledger: le lascia `PENDING` e logga l'anomalia.

**Perché.** Regola R1: un ledger incompleto non è prova che una puntata sia stata esclusa. Rimborsare in dubbio significherebbe restituire i soldi di una scommessa che potrebbe essere stata giocata davvero.

**Conseguenza.** Dopo un taglio degradato può restare qualche bet `PENDING` da chiudere a mano. È il prezzo della prudenza, ed è la scelta giusta per un registro di scommesse.

---

## 8. Anomalia visiva: puntata rimborsata che ricompare sulla ruota (regola R2)

**Cosa succede.** Se il leader muore insieme al worker che teneva una puntata, quella puntata viene rimborsata; se poi il broker la riconsegna e finisce in un ledger successivo, l'utente **vede la puntata sulla ruota** pur avendo già riavuto i soldi.

**Cosa NON succede.** Non viene pagata: il gateway la trova in stato terminale, logga `replay_after_refund` e non la riapre. Nessuna duplicazione di denaro — solo un'incoerenza fra ciò che l'utente vede e ciò che ha in tasca.

---

## 9. Elezioni a raffica all'avvio

**Cosa succede.** `cluster_manager` lancia un'elezione a ogni nodo che entra nel cluster. Con tre nodi avviati insieme si vedono più elezioni di fila nei log.

**Perché non è un problema.** L'assegnazione del ruolo è idempotente e il recovery parte **solo** alla transizione standby → leader, quindi le rielezioni non hanno effetti collaterali. È rumore nei log, non un difetto di correttezza.

---

## 10. `bet_forward_delay`: scaffolding di test lasciato nel codice

**Cos'è.** Un flag di configurazione nel worker che ritarda l'inoltro della scommessa al wheel. **Default 0, cioè disattivato.**

**A cosa serve.** La finestra in cui una puntata è davvero *in transito* al momento del taglio dura pochi millisecondi: senza questo flag il test «canali non vuoti» dipende dalla fortuna (in una prova, 0 catture su 40 puntate). Con `-game_engine bet_forward_delay 600` il caso diventa deterministico e si misura `in_flight_bets = 3`.

È volutamente rimasto: è ciò che rende **riproducibile** la dimostrazione che lo snapshot cattura qualcosa. Da citare come strumento di verifica, non come comportamento di esercizio.

---

## 11. Test non eseguiti

- **Partizione di rete con regole firewall**. Quella eseguita è una partizione **logica** (`net_kernel:allow/1` più disconnessione forzata), che riproduce fedelmente il comportamento del cluster ma non tocca lo stack di rete. Una prova con `pfctl` o namespace richiede privilegi di amministratore.
- **Carico**: il requisito «migliaia di richieste concorrenti» è coperto dall'architettura (competing consumers su N nodi) ma non è mai stato misurato con un test di carico.
- **Interfaccia**: le verifiche sono state fatte via API e coda, non dal browser. Le animazioni dei minigiochi e il banner di round annullato sono stati letti nel codice, non visti a schermo.
