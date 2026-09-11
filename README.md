# Distributed Crazy Time

Progetto per il corso di *Distributed Systems and Middleware Technologies* (Università di Pisa, A.A. 2025–2026) — Gabriele Caioli, Klaudio Ciacia, Lorenzo Vanni.

## Idea del progetto

Una web app di scommesse in tempo reale ispirata al game show *Crazy Time*: una ruota viene fatta girare a ogni round, i giocatori puntano su uno o più segmenti entro una finestra di betting e, se la ruota si ferma su un segmento bonus, il payout viene deciso da un mini-gioco (Pachinko, Coin Flip, Cash Hunt, Crazy Time).

L'interesse non è il gioco in sé ma i problemi distribuiti che porta con sé, affrontati con un'architettura ibrida **Java / Erlang / RabbitMQ**:

- **Leader election (Bully)** — l'esito di ogni round deve essere generato da un solo nodo autorevole (il *dealer*). Se cade, il cluster elegge un nuovo leader e la partita riprende. Una guardia di quorum sui nodi configurati impedisce due leader (e due ledger divergenti) in caso di partizione di rete.
- **Snapshot distribuito (Chandy-Lamport)** — allo scadere del timer ("no more bets") il sistema cattura un taglio globale consistente delle scommesse accettate sui nodi, comprese quelle *in transito*, prima di risolvere i payout. Il record del taglio è persistito su Mnesia replicato.
- **Sincronizzazione dello stato** — countdown, fase del round ed esiti sono propagati a tutti i client via WebSocket, partendo da un unico stato autorevole pubblicato dal leader.
- **Concorrenza distribuita** — l'ingestione delle scommesse è fatta da worker *competing consumers* sulla stessa coda AMQP, su tutti i nodi; solo la ruota è leader-only.

Il flusso end-to-end: browser → REST/WebSocket → gateway Spring Boot → `bets_queue` → worker Erlang → wheel process del leader → `results_queue` / `state_queue` → gateway → browser.

## Composizione delle cartelle

| Cartella | Contenuto |
|---|---|
| `java-gateway/` | API Gateway Spring Boot: autenticazione, wallet e storico su H2/JPA, endpoint REST, WebSocket STOMP verso i browser e listener AMQP verso l'engine. Il frontend statico (HTML/CSS/JS) sta in `src/main/resources/static/`. |
| `erlang-engine/` | Il game engine distribuito (progetto rebar3 `game_engine`), cuore del sistema. |
| `erlang-engine-2/`, `erlang-engine-3/` | Copie locali dell'engine usate per far girare un cluster a 3 nodi su una sola macchina durante i test (non versionate). |
| `docs/` | Relazione del progetto in LaTeX (`Distributed_Crazy_Time.tex`), PDF compilato e immagini/diagrammi. |
| `util_docs/` | Materiale di lavoro interno: proposta iniziale, piano di implementazione, guide di deploy sulle VM, walkthrough dei test, limiti noti (non versionato). |
| `test_scripts/` | Script Python di test end-to-end contro il gateway: `stress_test.py` (50 utenti concorrenti), `crash_test.py` (crash del dealer durante il betting), `inflight_test.py` (bet in transito al momento del taglio). |
| `logs/` | Log raccolti dalle esecuzioni locali e sulle VM (non versionati). |

### Dentro `erlang-engine/game_engine/src/`

- `game_engine_sup.erl` — supervisore principale, strategia `rest_for_one`: `rabbitmq_manager` → `cluster_manager` → `wheel_process` → `minigames_sup` → `worker` → `snapshot`.
- `cluster_manager.erl` — discovery e monitoraggio dei nodi peer, bootstrap di Mnesia, liste su cui si appoggiano elezione e snapshot.
- `leader_election.erl` — algoritmo Bully con guardia di quorum.
- `wheel_process.erl` — ciclo di vita del round (betting → spinning → mini-gioco → risultato → cooldown); gira solo sul leader.
- `worker.erl` — consumer AMQP di `bets_queue` su ogni nodo, inoltra al wheel del leader.
- `snapshot.erl` + `cl_recorder.erl` — collector dello snapshot e logica Chandy-Lamport lato partecipante (funzioni pure, usate da wheel e worker).
- `minigames_sup.erl` + `pachinko.erl`, `coinflip.erl`, `cashhunt.erl`, `crazytime.erl` — i quattro mini-giochi, supervisionati `one_for_one`.
- `rabbitmq_manager.erl` — connessione AMQP persistente, dichiarazione code e registrazione dei consumer.

## Componenti esterni

- **RabbitMQ** come middleware fra gateway ed engine, con tre code durable: `bets_queue`, `results_queue`, `state_queue`.
- **Mnesia** replicato sui nodi Erlang per i record di snapshot.
- **H2** su file per utenti, wallet e scommesse lato gateway.

La configurazione di default (`game_engine.app.src`, `application.properties`) punta a `localhost`; `config/vm.config` sovrascrive host del broker e nodi peer per il deploy sulle VM del DSMT.
