package com.crazytime.rabbitmq;

import com.crazytime.entity.Bet;
import com.crazytime.entity.Player;
import com.crazytime.repository.BetRepository;
import com.crazytime.repository.PlayerRepository;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Consuma il ledger del round prodotto dallo snapshot Chandy-Lamport.
 *
 * E' cio' che rende lo snapshot LOAD-BEARING: l'insieme autorevole delle
 * scommesse di un round non e' ottenibile in altro modo, perche' comprende
 * anche le puntate che al momento del gong erano ancora in volo fra un
 * worker e il wheel.
 *
 * Implementa due delle tre regole di riconciliazione:
 *
 *   R1 — si rimborsa solo una bet assente dal ledger. In dubbio non si
 *        rimborsa: resta PENDING e sara' chiusa dal ledger del round in cui
 *        verra' effettivamente giocata.
 *
 *   R2 — lo stato lato Java e' autoritativo sui MOVIMENTI DI DENARO, il
 *        ledger Erlang sull'ESITO DI GIOCO. Una bet gia' REFUNDED che
 *        ricompare in un ledger non viene mai ripagata: si logga e si lascia
 *        in stato terminale. E' l'unico caso in cui l'utente vede sulla ruota
 *        una puntata che gli e' stata restituita — anomalia visiva, non
 *        duplicazione di denaro.
 */
@Component
public class LedgerListener {

    private static final Logger log = LoggerFactory.getLogger(LedgerListener.class);

    @Autowired
    private PlayerRepository playerRepository;

    @Autowired
    private BetRepository betRepository;

    @Transactional
    public void processLedger(JsonNode root) {
        int round = root.path("round").asInt(-1);
        boolean degraded = root.path("degraded").asBoolean(false);
        if (round < 0) {
            log.warn("round_ledger senza round: {}", root);
            return;
        }

        Set<String> ledgerIds = new HashSet<>();
        JsonNode ids = root.path("bet_ids");
        if (ids.isArray()) {
            ids.forEach(n -> ledgerIds.add(n.asText()));
        }
        log.info("Ledger del round {} ({} bet, degraded={})", round, ledgerIds.size(), degraded);

        // 1. Le bet nel ledger appartengono a questo round in modo autoritativo:
        //    il round che il gateway aveva assegnato al piazzamento e' solo una
        //    stima locale, soggetta a sfasatura al confine fra due round.
        for (String betId : ledgerIds) {
            Optional<Bet> optional = betRepository.findByBetId(betId);
            if (optional.isEmpty()) {
                log.warn("Ledger: bet {} sconosciuta al gateway", betId);
                continue;
            }
            Bet bet = optional.get();
            if ("PENDING".equals(bet.getStatus())) {
                if (bet.getRound() == null || bet.getRound() != round) {
                    bet.setRound(round);
                    betRepository.save(bet);
                }
            } else if ("REFUNDED".equals(bet.getStatus())) {
                // R2: gia' rimborsata e ricomparsa nel ledger. Non si riapre e
                // non si paga; va solo documentata.
                log.warn("replay_after_refund: la bet {} e' stata rimborsata ma compare "
                         + "nel ledger del round {}. Non verra' pagata.", betId, round);
            }
        }

        // 2. Le pendenti di questo round ASSENTI dal ledger non sono mai entrate
        //    nel taglio: vanno rimborsate (regola R1).
        List<Bet> pending = betRepository.findByRoundAndStatus(round, "PENDING");
        for (Bet bet : pending) {
            if (bet.getBetId() != null && ledgerIds.contains(bet.getBetId())) {
                continue;
            }
            if (degraded) {
                // Taglio chiuso senza tutti i partecipanti: il ledger potrebbe
                // essere incompleto, quindi in dubbio NON si rimborsa (R1).
                log.warn("Ledger degradato del round {}: la bet {} resta PENDING "
                         + "invece di essere rimborsata", round, bet.getBetId());
                continue;
            }
            refund(bet, round);
        }
    }

    /**
     * Annullamento di un round interrotto dal crash del dealer.
     *
     * Rimborsa le pendenti di quel round TRANNE quelle elencate in
     * exclude_bet_ids: sono state consumate da un worker ma mai confermate,
     * quindi il broker le riconsegnera' e verranno giocate nel round
     * successivo. Rimborsarle significherebbe restituire i soldi E farle
     * girare lo stesso (regola R1).
     */
    @Transactional
    public void cancelRound(JsonNode root) {
        int round = root.path("round").asInt(-1);
        if (round < 0) {
            log.warn("round_cancelled senza round: {}", root);
            return;
        }

        Set<String> excluded = new HashSet<>();
        JsonNode ids = root.path("exclude_bet_ids");
        if (ids.isArray()) {
            ids.forEach(n -> excluded.add(n.asText()));
        }

        List<Bet> pending = betRepository.findByRoundAndStatus(round, "PENDING");
        int refunded = 0;
        for (Bet bet : pending) {
            if (bet.getBetId() != null && excluded.contains(bet.getBetId())) {
                log.info("Round {} annullato: la bet {} rientra dal broker, nessun rimborso",
                         round, bet.getBetId());
                continue;
            }
            refund(bet, round);
            refunded++;
        }
        log.info("Round {} annullato: {} bet rimborsate, {} escluse perche' verranno rigiocate",
                 round, refunded, excluded.size());
    }

    private void refund(Bet bet, int round) {
        Player player = playerRepository.findByUsernameForUpdate(bet.getUsername()).orElse(null);
        if (player == null) {
            log.warn("Giocatore {} non trovato per il rimborso della bet {}",
                     bet.getUsername(), bet.getBetId());
            return;
        }
        player.setBalance(player.getBalance().add(bet.getAmount()));
        playerRepository.save(player);

        bet.setStatus("REFUNDED");
        bet.setPayout(bet.getAmount());
        betRepository.save(bet);

        log.info("Bet {} assente dal ledger del round {}: rimborsati ${} a {}",
                 bet.getBetId(), round, bet.getAmount(), bet.getUsername());
    }
}
