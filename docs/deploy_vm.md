# Deployment e test sulle VM del DSMT

Container assegnati:

| | IP | Ruolo |
|---|---|---|
| **VM1** | `10.2.1.17` | RabbitMQ + Java Gateway + `game1` + `game2` |
| **VM2** | `10.2.1.18` | `game3` |

Credenziali: `root` / `root`. Ubuntu 24.04, Java 25, Erlang/OTP 28, Maven 3.9.11.

**Perché questa ripartizione.** Il quorum è la maggioranza dei nodi in `peer_nodes`,
cioè 2 su 3 (vedi [limiti_noti.md](limiti_noti.md#1-consistenza-scelta-sopra-disponibilità-il-quorum)).
Mettendo due nodi su VM1 e uno su VM2, una partizione fra le due macchine lascia la
maggioranza dallo stesso lato del gateway: il gioco continua e il nodo isolato si
autoretrocede. È esattamente lo scenario da mostrare — e con due macchine reali si può
finalmente fare con regole di firewall invece che con una partizione logica
(punto 11 dei limiti noti, "test non eseguiti").

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
ping -c 2 10.2.1.17 && ping -c 2 10.2.1.18
ssh root@10.2.1.17     # password: root
```

**Consiglio:** ti serviranno 5 sessioni SSH. Copia la chiave per non digitare la
password ogni volta:

```bash
ssh-keygen -t ed25519 -N '' -f ~/.ssh/id_dsmt      # se non ne hai già una
ssh-copy-id -i ~/.ssh/id_dsmt root@10.2.1.17
ssh-copy-id -i ~/.ssh/id_dsmt root@10.2.1.18
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

# Dipendenze Erlang (amqp_client & co.) compilate dentro _build/
cd erlang-engine/game_engine && ../rebar3 compile && cd ../..
```

Il tuo OTP locale è 28, lo stesso delle VM: i `.beam` in `_build/` sono
direttamente utilizzabili là.

---

## 2. Copia sulle VM

Un solo comando per macchina, via `tar` su SSH (non richiede `rsync` sul
container). Esclude git, il DB H2 locale e le directory Mnesia di sviluppo, ma
**include** `_build/` e il jar:

```bash
cd /Users/gabrielecaioli/Downloads/Uni/DistributedSystemsMT/ProgettoDistributedSMT/distributed_crazy_time

for IP in 10.2.1.17 10.2.1.18; do
  tar czf - \
      --exclude='.git' \
      --exclude='.DS_Store' \
      --exclude='java-gateway/data' \
      --exclude='Mnesia.*' \
      erlang-engine java-gateway/target/java-gateway-0.0.1-SNAPSHOT.jar stress_test.py docs \
  | ssh root@$IP 'mkdir -p /root/dct && tar xzf - -C /root/dct'
done
```

Su **VM1** servono due copie dell'engine, una per nodo: due `rebar3 shell` nella
stessa directory si contendono `_build/` e la directory Mnesia va tenuta separata.

```bash
ssh root@10.2.1.17 'cp -r /root/dct/erlang-engine /root/dct/erlang-engine-2'
```

Quindi: `game1` gira in `/root/dct/erlang-engine`, `game2` in `/root/dct/erlang-engine-2`.

---

## 3. RabbitMQ su VM1

Il broker non è preinstallato. Gira **solo su VM1**, ed entrambe le VM ci si
collegano.

```bash
ssh root@10.2.1.17
apt-get update && apt-get install -y rabbitmq-server
```

> Il pacchetto Ubuntu si porta dietro il proprio Erlang, indipendente dall'OTP 28
> usato dall'engine: le due installazioni non si disturbano.

**Passaggio obbligatorio.** Di default RabbitMQ accetta l'utente `guest` solo da
`localhost`: senza questo, `game3` su VM2 verrebbe rifiutato con
`ACCESS_REFUSED`.

```bash
echo 'loopback_users = none' >> /etc/rabbitmq/rabbitmq.conf
rabbitmq-plugins enable rabbitmq_management     # dashboard su :15672, opzionale
systemctl restart rabbitmq-server
systemctl status rabbitmq-server --no-pager
```

Se il container non ha systemd attivo (in questo caso il broker resta in foreground
e occupa il terminale: aprine un altro per proseguire, o lancialo dentro `tmux`):

```bash
sudo -u rabbitmq RABBITMQ_CONFIG_FILE=/etc/rabbitmq/rabbitmq /usr/lib/rabbitmq/bin/rabbitmq-server
```

Verifica da **VM2** che il broker sia raggiungibile:

```bash
ssh root@10.2.1.18 'timeout 3 bash -c "</dev/tcp/10.2.1.17/5672" && echo RAGGIUNGIBILE'
```

---

## 4. Configurazione del cluster

Non serve modificare `game_engine.app.src`: i default (`localhost`) restano quelli
di sviluppo, e sulle VM si applica il sys.config
[`config/vm.config`](../erlang-engine/game_engine/config/vm.config), già presente
nel repo, che sovrascrive:

- `rabbitmq.host` → `"10.2.1.17"`
- `peer_nodes` → `['game1@10.2.1.17', 'game2@10.2.1.17', 'game3@10.2.1.18']`
- porte fisse per la distribuzione Erlang (9100–9155), così sai cosa aprire e puoi
  scrivere regole iptables mirate

Lo **stesso file** va bene su entrambe le VM: `cluster_manager` filtra da
`peer_nodes` il nome del nodo locale.

Il gateway non richiede modifiche: gira su VM1 insieme al broker, e
`spring.rabbitmq.host=localhost` è già corretto.

### Firewall

Di norma i container non hanno filtri attivi. In caso contrario, su entrambe le VM:

```bash
ufw status                       # se "inactive", non serve altro
# se attivo:
ufw allow 8080/tcp               # GUI + REST (VM1)
ufw allow 5672/tcp               # AMQP (VM1)
ufw allow 4369/tcp               # epmd
ufw allow 9100:9155/tcp          # distribuzione Erlang
```

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
ssh root@10.2.1.17
cd /root/dct/java-gateway
java -jar target/java-gateway-0.0.1-SNAPSHOT.jar
```

Il DB H2 viene creato in `./data/` rispetto alla directory da cui lanci il jar:
lancialo sempre dalla stessa, o ti ritrovi utenti diversi a ogni avvio.

### Terminale 2 — `game1` (VM1)

```bash
ssh root@10.2.1.17
cd /root/dct/erlang-engine/game_engine
../rebar3 shell --name game1@10.2.1.17 --setcookie crazytime --config config/vm.config
```

Aspetta che compaia `Tabella snapshot_record pronta` prima di procedere.

### Terminale 3 — `game2` (VM1, seconda copia)

```bash
ssh root@10.2.1.17
cd /root/dct/erlang-engine-2/game_engine
../rebar3 shell --name game2@10.2.1.17 --setcookie crazytime --config config/vm.config
```

### Terminale 4 — `game3` (VM2)

```bash
ssh root@10.2.1.18
cd /root/dct/erlang-engine/game_engine
../rebar3 shell --name game3@10.2.1.18 --setcookie crazytime --config config/vm.config
```

`game3` ha il nome più alto: vince l'elezione e diventa leader (il dealer).

### Terminale 5 — client (nessun SSH)

Dal tuo Mac, con la VPN attiva: **http://10.2.1.17:8080**

### Verifica del cluster

Da una qualsiasi shell Erlang:

```erlang
nodes().                                %% deve elencare gli altri due
cluster_manager:get_connected_nodes().
leader_election:get_leader().           %% {ok,'game3@10.2.1.18'}
leader_election:is_leader().
rabbitmq_manager:is_connected().        %% true su tutti e tre
```

---

## 6. Test da eseguire sulle VM

### 6.1 Carico concorrente

Copre il punto "carico mai misurato" dei limiti noti. Dal Mac, con la VPN su:

```bash
python3 stress_test.py http://10.2.1.17:8080
```

(`pip3 install requests` se manca). Con 50 utenti guarda i log dei tre nodi: le
puntate si distribuiscono fra i worker dei nodi (competing consumers sulla
`bets_queue`), mentre il wheel gira solo sul leader.

### 6.2 Partizione di rete **reale** fra le due VM

Questo è il test che in locale non si poteva fare: colma il punto 11 dei limiti
noti. Su **VM2**, isola `game3` dalla maggioranza:

```bash
ssh root@10.2.1.18
iptables -A INPUT  -s 10.2.1.17 -j DROP
iptables -A OUTPUT -d 10.2.1.17 -j DROP
```

> Blocca **solo** l'IP del peer. Un `DROP` generico ti farebbe cadere anche la
> sessione SSH, che arriva dall'indirizzo VPN del tuo Mac, non da 10.2.1.17.

Cosa aspettarsi:

- `game3` (leader, ora 1/3) logga `Quorum ... non raggiunto` e si autoretrocede a
  standby: nessun round parte da quel lato;
- `game1` e `game2` (2/3) eleggono `game2` leader e il gioco prosegue sulla GUI;
- `game3` perde anche il broker e logga `Broker non raggiungibile`, ritentando.

Ripristino:

```bash
iptables -D INPUT  -s 10.2.1.17 -j DROP
iptables -D OUTPUT -d 10.2.1.17 -j DROP
```

`game3` rientra, e alla rielezione riprende il ruolo di leader.

> Se il container è unprivileged, `iptables` fallisce con `Permission denied`:
> ripiega sulla partizione logica già usata in locale (`erlang:disconnect_node/1`
> più `net_kernel:allow/1`), oppure sospendi il processo della beam con
> `kill -STOP` per simulare un nodo congelato.

### 6.3 Crash del dealer

Con `game3` leader, chiudi la sua shell (`Ctrl-C` due volte) o, più brutalmente,
da VM2: `pkill -9 -f game3@10.2.1.18`. Il round in corso viene recuperato da
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
rabbitmqctl purge_queue bets_queue
rabbitmqctl purge_queue state_queue
rabbitmqctl purge_queue results_queue
```

---

## 8. Ho modificato il codice: cosa rifaccio?

Le sezioni **0, 3 e 4 non si ripetono**: la VPN resta connessa, RabbitMQ resta
installato, `vm.config` è già sulle macchine (a meno che non sia lui ad essere
cambiato).

### Ho toccato il gateway Java

```bash
# sul Mac
cd java-gateway && mvn -DskipTests package && cd ..
scp java-gateway/target/java-gateway-0.0.1-SNAPSHOT.jar \
    root@10.2.1.17:/root/dct/java-gateway/target/
```

Riavvii **solo** il terminale 1. I nodi Erlang non se ne accorgono: si
riconnettono da soli al broker, che non è mai caduto.

### Ho toccato l'engine Erlang

```bash
# sul Mac, dalla radice del progetto
cd erlang-engine/game_engine && ../rebar3 compile && cd ../..

for IP in 10.2.1.17 10.2.1.18; do
  tar czf - --exclude='Mnesia.*' erlang-engine \
  | ssh root@$IP 'tar xzf - -C /root/dct'
done

# VM1: allinea la seconda copia, quella di game2.
# Copia solo src/ e config/: _build e Mnesia.* restano, rebar3 ricompila da solo.
ssh root@10.2.1.17 'cp -r /root/dct/erlang-engine/game_engine/src \
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
  ping -D -c 2 -W 1500 -s $s 10.2.1.17 >/dev/null 2>&1 && echo PASSA || echo scartato
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
