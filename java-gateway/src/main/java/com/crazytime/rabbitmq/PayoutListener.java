package com.crazytime.rabbitmq;

import com.crazytime.entity.Bet;
import com.crazytime.entity.Player;
import com.crazytime.repository.BetRepository;
import com.crazytime.repository.PlayerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Ascolta results_queue per elaborare i pagamenti ai giocatori vincenti.
 * Aggiorna lo status delle Bet (WON/LOST) e accredita i payout.
 *
 * Use case PDF: "The System must: Maintain and update the Players' wallet balances securely"
 */
@Component
public class PayoutListener {

    private static final Logger log = LoggerFactory.getLogger(PayoutListener.class);

    @Autowired
    private PlayerRepository playerRepository;

    @Autowired
    private BetRepository betRepository;

    public void processPayouts(String message) {
        log.info("Processando payout dal risultato: {}", message);
        try {
            // Estrai il segmento vincente dal risultato
            String winner = extractField(message, "winner");
            String multiplierStr = extractField(message, "multiplier");

            if (winner == null || multiplierStr == null) {
                log.warn("Risultato senza winner o multiplier: {}", message);
                return;
            }

            BigDecimal multiplier = new BigDecimal(multiplierStr);

            // Trova tutte le bet PENDING e aggiorna il loro status
            List<Bet> pendingBets = betRepository.findByStatus("PENDING");
            for (Bet bet : pendingBets) {
                if (bet.getSegment().equals(winner)) {
                    // VINCITA (restituisce la puntata iniziale + la vincita)
                    BigDecimal winnings = bet.getAmount().multiply(multiplier);
                    BigDecimal payout = bet.getAmount().add(winnings);
                    bet.setStatus("WON");
                    bet.setPayout(payout);
                    betRepository.save(bet);

                    // Accredita la vincita al giocatore
                    Optional<Player> optPlayer = playerRepository.findByUsername(bet.getUsername());
                    if (optPlayer.isPresent()) {
                        Player player = optPlayer.get();
                        player.setBalance(player.getBalance().add(payout));
                        playerRepository.save(player);
                        log.info("Payout di ${} accreditato a {} (bet su {})",
                                payout, bet.getUsername(), bet.getSegment());
                    }
                } else {
                    // PERDITA
                    bet.setStatus("LOST");
                    bet.setPayout(BigDecimal.ZERO);
                    betRepository.save(bet);
                }
            }
        } catch (Exception e) {
            log.error("Errore processando payout: {}", e.getMessage());
        }
    }

    private String extractField(String json, String field) {
        Pattern p = Pattern.compile("\"" + field + "\"\\s*:\\s*\"?([^,\"\\}]+)\"?");
        Matcher m = p.matcher(json);
        return m.find() ? m.group(1).trim() : null;
    }
}
