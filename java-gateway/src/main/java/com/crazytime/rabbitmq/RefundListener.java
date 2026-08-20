package com.crazytime.rabbitmq;

import com.crazytime.entity.Bet;
import com.crazytime.entity.Player;
import com.crazytime.repository.BetRepository;
import com.crazytime.repository.PlayerRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

/**
 * Ascolta la coda refunds_queue.
 * Quando Erlang rifiuta una scommessa (fase chiusa), il worker pubblica
 * un messaggio di rimborso. Questo listener riaccredita il saldo.
 *
 * Use case PDF: "The System must: Reject any bets placed after the 'No more bets' signal"
 */
@Component
public class RefundListener {

    private static final Logger log = LoggerFactory.getLogger(RefundListener.class);

    @Autowired
    private PlayerRepository playerRepository;

    @Autowired
    private BetRepository betRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Transactional
    @RabbitListener(queues = "refunds_queue")
    public void receiveRefund(String message) {
        log.info("Rimborso ricevuto: {}", message);
        try {
            JsonNode root = objectMapper.readTree(message);
            String username = root.has("username") ? root.get("username").asText() : null;
            BigDecimal amount = root.has("amount") ? new BigDecimal(root.get("amount").asText()) : null;

            if (username == null || amount == null) {
                log.warn("Rimborso malformato: {}", message);
                return;
            }

            Optional<Player> optionalPlayer = playerRepository.findByUsernameForUpdate(username);
            if (optionalPlayer.isPresent()) {
                Player player = optionalPlayer.get();
                player.setBalance(player.getBalance().add(amount));
                playerRepository.save(player);
                log.info("Rimborso di ${} accreditato a {}", amount, username);
            } else {
                log.warn("Utente per rimborso non trovato: {}", username);
            }

            // Segna la bet come REFUNDED così PayoutListener, quando arriva il risultato del round,
            // non la trova più PENDING e non la paga una seconda volta
            List<Bet> pendingBets = betRepository.findByUsernameAndStatus(username, "PENDING");
            for (Bet bet : pendingBets) {
                if (bet.getAmount().compareTo(amount) == 0) {
                    bet.setStatus("REFUNDED");
                    bet.setPayout(amount);
                    betRepository.save(bet);
                    log.info("Bet #{} marcata come REFUNDED per {}", bet.getId(), username);
                    break;  // Refund one matching bet at a time
                }
            }
        } catch (Exception e) {
            log.error("Errore processando rimborso: {}", e.getMessage(), e);
        }
    }
}
