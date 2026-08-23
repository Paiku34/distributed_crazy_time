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

import java.util.Optional;

/**
 * Rimborso puntuale di una singola scommessa, identificata dal suo bet_id.
 *
 * Sostituisce il vecchio RefundListener, che riconciliava i rimborsi
 * confrontando gli IMPORTI: due puntate di pari importo su segmenti diversi
 * erano indistinguibili, quindi il rimborso poteva chiudere la bet sbagliata.
 *
 * Erlang pubblica su results_queue:
 *   {"type":"bet_rejected","bet_id":"<uuid>","round":R,"reason":"betting_closed"|"undo"}
 *
 * L'idempotenza e' garantita dallo stato stesso della bet: una riconsegna del
 * messaggio trova la scommessa gia' REFUNDED e non accredita una seconda volta.
 */
@Component
public class BetRejectionHandler {

    private static final Logger log = LoggerFactory.getLogger(BetRejectionHandler.class);

    @Autowired
    private PlayerRepository playerRepository;

    @Autowired
    private BetRepository betRepository;

    @Transactional
    public void rejectBet(JsonNode root) {
        String betId = root.path("bet_id").asText(null);
        String reason = root.path("reason").asText("unknown");

        if (betId == null || betId.isEmpty()) {
            log.warn("bet_rejected senza bet_id: {}", root);
            return;
        }

        Optional<Bet> optionalBet = betRepository.findByBetId(betId);
        if (optionalBet.isEmpty()) {
            log.warn("bet_rejected per una bet sconosciuta: {}", betId);
            return;
        }

        Bet bet = optionalBet.get();
        if (!"PENDING".equals(bet.getStatus())) {
            // Gia' chiusa: messaggio riconsegnato dal broker, oppure rimborso
            // gia' applicato. Non si accredita due volte.
            log.info("bet_rejected ignorato, bet {} gia' in stato {}", betId, bet.getStatus());
            return;
        }

        Player player = playerRepository.findByUsernameForUpdate(bet.getUsername()).orElse(null);
        if (player == null) {
            log.warn("Giocatore {} non trovato per il rimborso della bet {}", bet.getUsername(), betId);
            return;
        }

        player.setBalance(player.getBalance().add(bet.getAmount()));
        playerRepository.save(player);

        bet.setStatus("REFUNDED");
        bet.setPayout(bet.getAmount());
        betRepository.save(bet);

        log.info("Bet {} rimborsata ({}): ${} riaccreditati a {}",
                 betId, reason, bet.getAmount(), bet.getUsername());
    }
}
