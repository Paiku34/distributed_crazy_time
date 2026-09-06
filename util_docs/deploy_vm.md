# Deployment e test sulle VM del DSMT

Container assegnati:

| | IP | Ruolo |
|---|---|---|
| **VM1** | `10.2.1.15` | RabbitMQ + Java Gateway + `game1` + `game2` |
| **VM2** | `10.2.1.16` | `game3` |

Credenziali: `root` / `root`. Ubuntu 24.04, Java 25, Erlang/OTP 28, Maven 3.9.11.

**Perché questa ripartizione.** Il quorum è la maggioranza dei nodi in `peer_nodes`,
cioè 2 su 3 (vedi [limiti_noti.md](limiti_noti.md#a2)).
Mettendo due nodi su VM1 e uno su VM2, una partizione fra le due macchine lascia la
maggioranza dallo stesso lato del gateway: il gioco continua e il nodo isolato si
autoretrocede. È esattamente lo scenario da mostrare, e su due macchine distinte
il nodo isolato è isolato **davvero**: la sua rete, la sua beam, il suo disco
Mnesia (vedi "Cosa non è stato misurato" nei [limiti noti](limiti_noti.md#cosa-non-è-stato-misurato)).

L'isolamento si provoca dalla shell Erlang di `game3`
([sezione 6.2](#62-partizione-fra-game3-e-la-maggioranza)): i container non
permettono di manipolare il firewall.

**Quanti terminali servono.** Le sezioni 1–4 sono preparazione: comandi che partono,
finiscono e restituiscono il prompt, quindi vanno bene tutti nello stesso terminale,
in sequenza (con `exit` fra una sessione SSH e l'altra). L'unica eccezione è la VPN
della sezione 0, che resta in foreground: quello è un terminale dedicato, da lasciare
aperto per tutta la durata del lavoro.

La sezione 5 richiede invece **4 sessioni SSH** (tre verso VM1: gateway, `game1`,
`game2`; una verso VM2: `game3`), perché sono quattro processi long-running che
devono girare insieme e stampano i log che ti servono durante i test. Il "terminale
5" è solo il browser sul tuo Mac, non una sessione remota.

Con `tmux` sulle VM le sessioni SSH scendono a due (una per macchina) e i processi
sopravvivono a un calo della VPN — vedi la sezione 5.

---

## 0. VPN e accesso SSH

Sul tuo Mac, con il file `.ovpn` ricevuto per email:

```bash
brew install openvpn                       # una volta sola
sudo /usr/local/sbin/openvpn --config ~/Downloads/dsmt/VMs/studenti2026_2.ovpn
```

Il **percorso assoluto** non è pignoleria: Homebrew installa i demoni di rete in
`sbin`, e `/usr/local/sbin` non è nel PATH di default su macOS — `openvpn` da solo
risponde `command not found` anche se il pacchetto c'è. Il percorso assoluto aggira
anche l'eventuale `secure_path` di `sudo`. (Su Mac Apple Silicon il prefisso è
`/opt/homebrew/sbin/openvpn`.)

Lascia il terminale aperto (la VPN resta attiva finché gira).

Se OpenVPN 2.7 rifiuta il file con `Unrecognized option` — capita con i `.ovpn`
scritti per versioni più vecchie, che usano direttive legacy come `cipher` o
`comp-lzo` — usa [Tunnelblick](https://tunnelblick.net/), che include più versioni
del client e permette di scegliere quella compatibile.

Verifica in un **altro** terminale:

```bash
ping -c 2 10.2.1.15 && ping -c 2 10.2.1.16
ssh root@10.2.1.15     # password: root
```

**Consiglio:** ti serviranno 5 sessioni SSH. Copia la chiave per non digitare la
password ogni volta:

```bash
ssh-keygen -t ed25519 -N '' -f ~/.ssh/id_dsmt      # se non ne hai già una
ssh-copy-id -i ~/.ssh/id_dsmt root@10.2.1.15
ssh-copy-id -i ~/.ssh/id_dsmt root@10.2.1.16
```

---

## 1. Build in locale (non sulle VM)

Costruire sul Mac ed esportare gli artefatti evita di dipendere dalla rete dei
container: niente download Maven Central, niente fetch da Hex.

```bash
cd /Users/gabrielecaioli/Downloads/Uni/DistributedSystemsMT/ProgettoDistributedSMT/distributed_crazy_time

# Jar eseguibile dello Spring Boot gateway
cd java-gateway && mvn -DskipTests package && cd ..
ls -lh java-gateway/target/java-gateway-0.0.1-SNAPSHOT.jar
unzip -p java-gateway/target/java-gateway-0.0.1-SNAPSHOT.jar META-INF/MANIFEST.MF | grep Main-Class

# Dipendenze Erlang (amqp_client & co.) compilate dentro _build/
cd erlang-engine/game_engine && ../rebar3 compile && cd ../..
```

Il jar deve pesare **~55 MB** e il manifest deve contenere
`Main-Class: org.springframework.boot.loader.launch.JarLauncher`: è il jar
*repackaged*, con le dipendenze incluse. Se ne trovi uno da ~8 MB e `java -jar`
risponde `no main manifest attribute`, significa che il goal `repackage` non è
girato — serve `spring-boot-maven-plugin` dichiarato nel `<build>` del
[pom.xml](../java-gateway/pom.xml). Ereditare da `spring-boot-starter-parent` non
basta: il parent ne fornisce solo la configurazione, non lo attiva.

Il tuo OTP locale è 28, lo stesso delle VM: i `.beam` in `_build/` sono
direttamente utilizzabili là.

---

## 2. Copia sulle VM

Un solo comando per macchina, via `tar` su SSH (non richiede `rsync` sul
container). Esclude git, il DB H2 locale e le directory Mnesia di sviluppo, ma
**include** `_build/` e il jar:

```bash
cd /Users/gabrielecaioli/Downloads/Uni/DistributedSystemsMT/ProgettoDistributedSMT/distributed_crazy_time

for IP in 10.2.1.15 10.2.1.16; do
  COPYFILE_DISABLE=1 tar czf - \
      --exclude='.git' \
      --exclude='.DS_Store' \
      --exclude='java-gateway/data' \
      --exclude='Mnesia.*' \
      erlang-engine java-gateway/target/java-gateway-0.0.1-SNAPSHOT.jar test_scripts docs \
  | ssh root@$IP 'mkdir -p /root/dct && tar xzf - -C /root/dct'
done
```

`COPYFILE_DISABLE=1` non è opzionale: senza, il `tar` di macOS affianca a ogni file
un gemello **AppleDouble** con prefisso `._` per i metadati estesi. Sulla VM quei
gemelli diventano file veri, e `rebar3` si ferma prima ancora di compilare:

```
===> Multiple app files found in one app dir:
     .../src/._game_engine.app.src and .../src/game_engine.app.src
```

Se ti è già successo, ripulisci le macchine con
`ssh root@$IP "find /root/dct -name '._*' -delete"`.

Su **VM1** servono due copie dell'engine, una per nodo: due `rebar3 shell` nella
stessa directory si contendono `_build/` e la directory Mnesia va tenuta separata.

```bash
ssh root@10.2.1.15 'cp -r /root/dct/erlang-engine /root/dct/erlang-engine-2'
```

Quindi: `game1` gira in `/root/dct/erlang-engine`, `game2` in `/root/dct/erlang-engine-2`.

> **Attenzione (una volta sola):** `cp -r` cambia comportamento a seconda che la
> destinazione esista: la prima volta crea la copia, la seconda copia *dentro* e
> ti lascia `/root/dct/erlang-engine-2/erlang-engine/`. Se ti succede:
> `rm -rf /root/dct/erlang-engine-2/erlang-engine` (il percorso annidato, non
> quello di primo livello). Per i riallineamenti successivi usa la forma della
> [sezione 8](#ho-toccato-lengine-erlang), che copia solo `src/` e `config/`.

---

## 3. RabbitMQ su VM1

Il broker non è preinstallato. Gira **solo su VM1**, ed entrambe le VM ci si
collegano.

```bash
ssh root@10.2.1.15
apt-get update && apt-get install -y rabbitmq-server
```

### 3.1 Conflitto di versioni Erlang (da fare **una volta sola**)

`apt` si porta dietro come dipendenza il **proprio** Erlang — `erlang-base` 25.3
in `/usr/lib/erlang` — perché l'OTP 28 dell'immagine è installato da sorgente in
`/usr/local/lib/erlang` e quindi `dpkg` non sa che esiste. Servono entrambi:
`rabbitmq-server` 3.12 di Ubuntu è compilato per OTP 25, l'engine gira su OTP 28.

Il problema è l'ordine del `PATH`: `/usr/local/bin` precede `/usr/bin`, quindi
senza intervento anche RabbitMQ parte su OTP 28 e muore prima di aprire la porta,
con un errore che parla di Elixir e non di RabbitMQ:

```
beam/beam_load.c(594): Error loading function 'Elixir.Kernel':alias_defmodule/3:
  please re-compile this module with an Erlang/OTP 28 compiler
```

Si fissa il `PATH` del **servizio** con un drop-in systemd, in cui
`/usr/local/bin` è tolto del tutto e non solo retrocesso:

```bash
mkdir -p /etc/systemd/system/rabbitmq-server.service.d
cat > /etc/systemd/system/rabbitmq-server.service.d/otp25.conf <<'EOF'
[Service]
Environment=PATH=/usr/lib/erlang/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin
EOF
systemctl daemon-reload
```

**È un passaggio una tantum**: il drop-in è un file su disco, sopravvive a
riavvii del servizio e della macchina, e non compare fra i passaggi da ripetere
della [sezione 8](#8-ho-modificato-il-codice-cosa-rifaccio). Lo rifai solo se
reinstalli la VM da zero.

Questo sistema il **servizio**. I comandi CLI non passano da systemd e hanno un
problema in più, che vale la pena capire perché il sintomo è ingannevole:
`rabbitmqctl` e `rabbitmq-plugins` sono entrambi symlink a
`rabbitmq-script-wrapper`, che si comporta in due modi diversi:

```sh
elif [ `id -u` = `id -u rabbitmq` -o "$SCRIPT" = "rabbitmq-plugins" ] ; then
    /usr/lib/rabbitmq/bin/${SCRIPT} "$@"                      # diretto → PATH preservato
elif [ `id -u` = 0 ] ; then
    su rabbitmq -s /bin/sh -c "/usr/lib/rabbitmq/bin/${SCRIPT} ${CMDLINE}"   # su → ambiente azzerato
```

`rabbitmq-plugins` è nominato nel primo ramo, quindi da root gira diretto e un
eventuale prefisso `PATH=...` sopravvive. `rabbitmqctl` da root finisce nel ramo
`su`, e `su` **ricostruisce l'ambiente da zero**: il tuo `PATH` viene buttato via
e tornano i default di sistema, con `/usr/local/bin` (OTP 28) in testa. Risultato:
prefissare `rabbitmqctl` non serve a niente, l'errore `beam_load.c` resta identico.

La soluzione è presentarsi **già come utente `rabbitmq`**, così `id -u` coincide,
si prende il primo ramo e non c'è nessun `su` di mezzo. Anche questo è da fare una
volta sola:

```bash
cat > /usr/local/bin/rmq <<'EOF'
#!/bin/sh
exec sudo -u rabbitmq env PATH=/usr/lib/erlang/bin:/usr/sbin:/usr/bin:/sbin:/bin "$@"
EOF
chmod +x /usr/local/bin/rmq
```

Da qui in poi ogni comando CLI si lancia con `rmq` davanti — `rmq rabbitmqctl status`,
`rmq rabbitmqctl purge_queue bets_queue`. Non serve passare `HOME`: nel primo ramo
il wrapper fa già `cd /var/lib/rabbitmq` e imposta `HOME=.` per trovare il cookie.
La conferma che funziona è la riga `Erlang/OTP 25 [erts-13.2.2.5]` nell'output di
`rmq rabbitmqctl status`.

> **Attenzione:** Non mettere mai `/usr/lib/erlang/bin` nel `PATH` della tua shell con `export`
> o nel `.bashrc`: in quella sessione `../rebar3 shell` girerebbe su OTP 25 e i
> `.beam` compilati sul Mac con OTP 28 non si caricherebbero — stesso errore,
> ribaltato sull'engine. Il senso di `rmq` è proprio confinare l'OTP 25 al singolo
> comando.

> `sudo` stampa `unable to resolve host Distributed2025-1516`: innocuo, è
> l'hostname assente da `/etc/hosts`. Si zittisce con
> `echo "127.0.1.1 $(hostname)" >> /etc/hosts`.

Perché la convivenza è legittima: RabbitMQ e l'engine parlano **AMQP su TCP**, non
condividono la VM Erlang. L'unico gruppo che deve avere versioni compatibili fra
loro è `game1`/`game2`/`game3`, che usano distribuzione Erlang e Mnesia.

### 3.2 Configurazione

**Passaggio obbligatorio.** Di default RabbitMQ accetta l'utente `guest` solo da
`localhost`: senza questo, `game3` su VM2 verrebbe rifiutato con
`ACCESS_REFUSED`.

```bash
# forma rilanciabile: cancella e riscrive, invece di accodare a ogni esecuzione
sed -i '/^loopback_users/d' /etc/rabbitmq/rabbitmq.conf
echo 'loopback_users = none' >> /etc/rabbitmq/rabbitmq.conf

systemctl restart rabbitmq-server
rmq rabbitmq-plugins enable rabbitmq_management    # dashboard su :15672, opzionale
```

Il solo `echo >>` **accoda**: rilanciando il blocco ti ritrovi la riga ripetuta.
RabbitMQ la tollera (vince l'ultima) ma il file diventa illeggibile, e non capisci
più quale valore è attivo.

Verifica: le tre porte in ascolto e la CLI che risponde sull'OTP giusto.

```bash
ss -lntp | grep -E '5672|15672'    # 5672 AMQP, 15672 dashboard, 25672 clustering
rmq rabbitmqctl status | grep -E 'RabbitMQ version|Erlang configuration'
```

> **Solo se `systemctl` non funziona** — alternativa ai due comandi qui sopra, non
> un passaggio in più. Se `systemctl status rabbitmq-server` risponde con un unit
> `loaded`, salta questo riquadro: lanciarlo con il servizio già attivo avvia un
> secondo broker che collide su porte e nome di nodo.
>
> Senza systemd il drop-in non serve, ma il `PATH` va passato lo stesso, e il
> broker resta in foreground occupando il terminale (aprine un altro per
> proseguire, o lancialo dentro `tmux`):
>
> ```bash
> rmq /usr/lib/rabbitmq/bin/rabbitmq-server
> ```

Verifica da **VM2** che il broker sia raggiungibile:

```bash
ssh root@10.2.1.16 'timeout 3 bash -c "</dev/tcp/10.2.1.15/5672" && echo RAGGIUNGIBILE'
```

---

## 4. Configurazione del cluster

Non serve modificare `game_engine.app.src`: i default (`localhost`) restano quelli
di sviluppo, e sulle VM si applica il sys.config
[`config/vm.config`](../erlang-engine/game_engine/config/vm.config), già presente
nel repo, che sovrascrive:

- `rabbitmq.host` → `"10.2.1.15"`
- `peer_nodes` → `['game1@10.2.1.15', 'game2@10.2.1.15', 'game3@10.2.1.16']`
- porte fisse per la distribuzione Erlang (9100–9155): senza, la beam ne sceglie
  una a caso a ogni avvio e non sapresti cosa aprire se un giorno ci fosse un
  firewall di mezzo

Lo **stesso file** va bene su entrambe le VM: `cluster_manager` filtra da
`peer_nodes` il nome del nodo locale.

Il gateway non richiede modifiche: gira su VM1 insieme al broker, e
`spring.rabbitmq.host=localhost` è già corretto.

### Porte usate

I container non hanno filtri attivi, quindi non c'è nulla da configurare. L'elenco
serve a sapere cosa passa fra le due macchine:

| Porta | Protocollo | Dove | A cosa serve |
|---|---|---|---|
| 8080 | TCP | VM1 | GUI e REST del gateway |
| 5672 | TCP | VM1 | AMQP, il broker |
| 15672 | TCP | VM1 | dashboard RabbitMQ |
| 4369 | TCP | entrambe | `epmd`, il name server della distribuzione Erlang |
| 9100–9155 | TCP | entrambe | distribuzione Erlang fra i nodi |

---

## 5. Avvio del sistema

Quattro processi long-running, quindi 4 sessioni SSH: tre verso VM1 (gateway,
`game1`, `game2`) e una verso VM2 (`game3`). La password è `root` a ogni
connessione, a meno di aver fatto `ssh-copy-id` nella sezione 0.

Meglio ancora: **una sola sessione SSH per macchina** e dentro `tmux`
(`apt-get install -y tmux`), che dimezza le connessioni e mantiene vivi i processi
se cade la VPN — con `ssh` diretti un calo di rete ti ammazza il cluster a metà
test.

```bash
tmux new -s dct        # crea la sessione
#  Ctrl-b c            nuova finestra
#  Ctrl-b n / p        finestra successiva / precedente
#  Ctrl-b d            esci lasciando tutto in esecuzione
tmux attach -t dct     # rientra
```

**L'ordine conta:** broker, poi gateway, poi i nodi Erlang **uno alla volta** — il
primo crea lo schema Mnesia, gli altri due se ne prendono una copia.

### Terminale 1 — Gateway (VM1)

```bash
ssh root@10.2.1.15
cd /root/dct/java-gateway
java -jar target/java-gateway-0.0.1-SNAPSHOT.jar
```

Il DB H2 viene creato in `./data/` rispetto alla directory da cui lanci il jar:
lancialo sempre dalla stessa, o ti ritrovi utenti diversi a ogni avvio.

### Terminale 2 — `game1` (VM1)

```bash
ssh root@10.2.1.15
cd /root/dct/erlang-engine/game_engine
../rebar3 shell --name game1@10.2.1.15 --setcookie crazytime --config config/vm.config
```

Aspetta che compaia `Tabella snapshot_record pronta` prima di procedere.

### Terminale 3 — `game2` (VM1, seconda copia)

```bash
ssh root@10.2.1.15
cd /root/dct/erlang-engine-2/game_engine
../rebar3 shell --name game2@10.2.1.15 --setcookie crazytime --config config/vm.config
```

### Terminale 4 — `game3` (VM2)

```bash
ssh root@10.2.1.16
cd /root/dct/erlang-engine/game_engine
../rebar3 shell --name game3@10.2.1.16 --setcookie crazytime --config config/vm.config
```

`game3` ha il nome più alto: vince l'elezione e diventa leader (il dealer).

### Terminale 5 — client (nessun SSH)

Dal tuo Mac, con la VPN attiva: **http://10.2.1.15:8080**

### Verifica del cluster

Da una qualsiasi shell Erlang:

```erlang
nodes().                                %% deve elencare gli altri due
cluster_manager:get_connected_nodes().
leader_election:get_leader().           %% {ok,'game3@10.2.1.16'}
leader_election:is_leader().
rabbitmq_manager:is_connected().        %% true su tutti e tre
```

---

## 6. Test da eseguire sulle VM

### 6.1 Carico concorrente

Copre il punto "carico mai misurato" dei limiti noti. Dal Mac, con la VPN su:

```bash
python3 test_scripts/stress_test.py http://10.2.1.15:8080
```

(`pip3 install requests` se manca). Con 50 utenti guarda i log dei tre nodi: le
puntate si distribuiscono fra i worker dei nodi (competing consumers sulla
`bets_queue`), mentre il wheel gira solo sul leader.

### 6.2 Partizione fra `game3` e la maggioranza

Questo è il test che in locale non si poteva fare: colma il punto 11 dei limiti
noti. Si esegue **interamente dalla shell Erlang di `game3`**.

**Il test.** Nella finestra di `game3` (prompt `(game3@10.2.1.16)1>`), tre
comandi, uno alla volta, ciascuno chiuso dal punto:

```erlang
net_kernel:allow(['game3@10.2.1.16']).
erlang:disconnect_node('game1@10.2.1.15').
erlang:disconnect_node('game2@10.2.1.15').
```

L'ordine conta. `disconnect_node/1` da solo non basta: la distribuzione Erlang
riconnette al primo messaggio e la partizione si richiuderebbe in un istante. È
`net_kernel:allow/1`, chiamata **prima**, a reggere l'isolamento rifiutando le
riconnessioni. Restringe i nodi ammessi a se stesso, quindi `game1` e `game2`
vengono respinti in fase di handshake.

Il nodo resta **isolato ma vivo**: la sua shell risponde ancora, e lì dentro puoi
verificare l'effetto.

```erlang
nodes().     %% deve rispondere []
```

Cosa aspettarsi:

- `game3` (leader, ora 1/3) logga `[ELECTION] Quorum 1/3 non raggiunto` e
  `[ROLE] Questo nodo ora e' in STANDBY`, con `[WHEEL] DISATTIVATO`: nessun round
  parte da quel lato;
- `game1` e `game2` (2/3) eleggono `game2` leader e il gioco prosegue sulla GUI.

**Ripristino: riavviare `game3`.** `Ctrl-C` due volte nella sua finestra, poi lo
stesso comando di avvio della sezione 5. Non è un ripiego: una volta chiamata
`allow/1`, la lista dei nodi ammessi non si può più svuotare — `allow([])` non
annulla nulla — e l'unico modo di togliere la restrizione è ricreare `net_kernel`,
cioè far ripartire il nodo. Rientrando, `game3` riprende il ruolo di leader alla
rielezione, che è l'ultima cosa che il test deve mostrare.

> **Differenza rispetto a una partizione di rete vera:** qui il broker resta
> raggiungibile da `game3`, quindi non vedrai `Broker non raggiungibile`. Se ti
> serve anche quell'aspetto, chiudi la connessione AMQP di VM2 dalla dashboard
> RabbitMQ (*Connections* → la connessione da `10.2.1.16` → *Force Close*): il
> nodo ritenta, e nei log compaiono i tentativi.

> **Variante: nodo congelato.** Da una shell su VM2, `pkill -STOP -f game3@10.2.1.16`
> sospende la beam e `pkill -CONT -f game3@10.2.1.16` la risveglia. È l'unica
> forma davvero reversibile senza riavvio, ma mostra solo il lato maggioranza:
> `game3` è fermo e non logga, quindi l'autoretrocessione non si vede.

### 6.3 Crash del dealer

Con `game3` leader, chiudi la sua shell (`Ctrl-C` due volte) o, più brutalmente,
da VM2: `pkill -9 -f game3@10.2.1.16`. Il round in corso viene recuperato da
`game2`: completato se il segmento era un moltiplicatore, annullato e rimborsato
se era un minigioco (limite noto 6).

### 6.4 Snapshot con canali non vuoti

Il flag è letto a ogni inoltro, quindi basta abilitarlo a caldo dalla shell di
**ogni** nodo (senza riavviare nulla):

```erlang
application:set_env(game_engine, bet_forward_delay, 600).
```

Poi piazza qualche puntata e fai scattare il gong: il ledger del round riporta
`in_flight_bets` diverso da zero. Rimettilo a `0` per tornare al comportamento di
esercizio — è scaffolding di test, non un parametro di produzione (limite noto 10).

---

## 7. Ripartire da zero

A processi spenti, su entrambe le VM:

```bash
rm -rf /root/dct/erlang-engine*/game_engine/Mnesia.*   # checkpoint snapshot
rm -rf /root/dct/java-gateway/data                     # utenti e scommesse H2
```

E sul broker (VM1):

```bash
rmq rabbitmqctl purge_queue bets_queue
rmq rabbitmqctl purge_queue state_queue
rmq rabbitmqctl purge_queue results_queue
```

---

## 8. Ho modificato il codice: cosa rifaccio?

Le sezioni **0, 3 e 4 non si ripetono**: la VPN resta connessa, RabbitMQ resta
installato col suo drop-in systemd, `vm.config` è già sulle macchine (a meno che
non sia lui ad essere cambiato). Se invece hai chiuso il terminale della VPN,
quella sì va rifatta — è l'unica cosa che non vive su disco.

### Ho toccato il gateway Java

```bash
# sul Mac
cd java-gateway && mvn -DskipTests package && cd ..
scp java-gateway/target/java-gateway-0.0.1-SNAPSHOT.jar \
    root@10.2.1.15:/root/dct/java-gateway/target/
```

Riavvii **solo** il terminale 1. I nodi Erlang non se ne accorgono: si
riconnettono da soli al broker, che non è mai caduto.

### Ho toccato l'engine Erlang

```bash
# sul Mac, dalla radice del progetto
cd erlang-engine/game_engine && ../rebar3 compile && cd ../..

for IP in 10.2.1.15 10.2.1.16; do
  COPYFILE_DISABLE=1 tar czf - --exclude='Mnesia.*' erlang-engine \
  | ssh root@$IP 'tar xzf - -C /root/dct'
done

# VM1: allinea la seconda copia, quella di game2.
# Copia solo src/ e config/: _build e Mnesia.* restano, rebar3 ricompila da solo.
ssh root@10.2.1.15 'cp -r /root/dct/erlang-engine/game_engine/src \
                          /root/dct/erlang-engine/game_engine/config \
                          /root/dct/erlang-engine-2/game_engine/'
```

**Il passaggio su `erlang-engine-2` è obbligatorio** e non ha rete di protezione:
se lo salti, `game1` e `game3` girano col codice nuovo e `game2` col vecchio. Il
cluster si forma lo stesso e il bug sembra intermittente, perché dipende da quale
nodo è leader.

Poi riavvii i nodi. Il quorum è 2 su 3, quindi puoi farlo **uno alla volta** senza
fermare il gioco: il cluster regge con due nodi vivi, e se cade il leader la
rielezione lo sostituisce. Cambia solo l'ordine se vuoi che a fine giro il leader
sia di nuovo `game3`: riavvia quello per ultimo.

### Iterazione rapida senza riavviare

Per modifiche piccole, dentro la shell di un nodo già avviato:

```erlang
r3:compile().        %% ricompila il progetto e ricarica i moduli cambiati
```

Non applica modifiche a `vm.config` né allo stato dei `gen_server` già avviati:
per quelle serve il riavvio. Utile per aggiustare una funzione di calcolo, non per
cambiare la struttura del supervisore.

### Ho toccato `vm.config`

Va ricopiato su entrambe le VM ed **entrambe le copie** di VM1, e i nodi vanno
riavviati: il sys.config si legge solo al boot.

---

## 9. Se qualcosa non parte

| Sintomo | Causa |
|---|---|
| `ACCESS_REFUSED` da `game3` | manca `loopback_users = none` in `/etc/rabbitmq/rabbitmq.conf` |
| `beam_load.c ... Elixir.Kernel` da `rabbitmqctl` | l'hai lanciato da root: il wrapper fa `su` e azzera il `PATH`. Usa `rmq rabbitmqctl ...` ([3.1](#31-conflitto-di-versioni-erlang-da-fare-una-volta-sola)) |
| RabbitMQ non parte, stesso errore `beam_load.c` | manca il drop-in systemd della [sezione 3.1](#31-conflitto-di-versioni-erlang-da-fare-una-volta-sola) |
| `nodes()` vuoto | cookie diverso fra i nodi, oppure `--name` con hostname invece dell'IP |
| Tutti standby, il gioco non parte | meno di 2 nodi su 3 attivi: è il quorum, non un bug |
| Mnesia non carica i checkpoint dopo un riavvio | comportamento atteso (limite noto 2): `cluster_manager:force_load_snapshots().` |
| Il gateway esplode all'avvio su Java 25 | Spring Boot 3.2 non è certificato su 25: `sdk install java 21.0.5-open && sdk use java 21.0.5-open` |
| `epmd: node name already occupied` | resta una beam viva: `epmd -names`, poi `pkill -f gameN@` |

### Il trasferimento della sezione 2 si blocca a metà

SSH si autentica, poi la copia si ferma senza errore e senza avanzare. Non è
lentezza: è un **buco nero di MTU**. Il tunnel OpenVPN si configura a 1500, ma se
sei sotto hotspot il collegamento fisico ne regge meno, e i pacchetti pieni
vengono scartati in silenzio — quelli piccoli (ping, handshake SSH) passano, il
trasferimento di volume no.

Misura l'MTU effettivo del percorso, con il bit "don't fragment":

```bash
for s in 1200 1300 1350 1400 1450; do
  printf "pkt %s: " $((s+28))
  ping -D -c 2 -W 1500 -s $s 10.2.1.15 >/dev/null 2>&1 && echo PASSA || echo scartato
done
```

Poi abbassa l'MTU del tunnel sotto il valore più alto che passa (il nome
dell'interfaccia cambia a ogni riconnessione, verificalo con `ifconfig | grep utun`):

```bash
sudo ifconfig utun7 mtu 1380
```

Oppure, in modo stabile, riconnetti la VPN aggiungendo `--tun-mtu 1380`. Sistema
entrambe le direzioni: l'MSS annunciato nell'handshake TCP costringe anche i
container a rispondere con segmenti sotto soglia.
