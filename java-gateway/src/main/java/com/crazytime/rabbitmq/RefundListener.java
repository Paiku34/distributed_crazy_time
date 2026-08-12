package com.crazytime.rabbitmq;

import com.crazytime.entity.Player;
import com.crazytime.repository.PlayerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    @RabbitListener(queues = "refunds_queue")
    public void receiveRefund(String message) {
        log.info("Rimborso ricevuto: {}", message);
        try {
            String username = extractField(message, "username");
            String amountStr = extractField(message, "amount");

            if (username == null || amountStr == null) {
                log.warn("Rimborso malformato: {}", message);
                return;
            }

            BigDecimal amount = new BigDecimal(amountStr);
            Optional<Player> optionalPlayer = playerRepository.findByUsername(username);
            if (optionalPlayer.isPresent()) {
                Player player = optionalPlayer.get();
                player.setBalance(player.getBalance().add(amount));
                playerRepository.save(player);
                log.info("Rimborso di ${} accreditato a {}", amount, username);
            } else {
                log.warn("Utente per rimborso non trovato: {}", username);
            }
        } catch (Exception e) {
            log.error("Errore processando rimborso: {}", e.getMessage());
        }
    }

    private String extractField(String json, String field) {
        Pattern p = Pattern.compile("\"" + field + "\"\\s*:\\s*\"?([^,\"\\}]+)\"?");
        Matcher m = p.matcher(json);
        return m.find() ? m.group(1).trim() : null;
    }
}
