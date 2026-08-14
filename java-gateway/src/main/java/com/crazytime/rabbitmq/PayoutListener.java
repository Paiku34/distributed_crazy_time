package com.crazytime.rabbitmq;

import com.crazytime.entity.Bet;
import com.crazytime.entity.Player;
import com.crazytime.repository.BetRepository;
import com.crazytime.repository.PlayerRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

/**
 * Ascolta results_queue per elaborare i pagamenti ai giocatori vincenti.
 * Aggiorna lo status delle Bet (WON/LOST) e accredita i payout.
 */
@Component
public class PayoutListener {

    private static final Logger log = LoggerFactory.getLogger(PayoutListener.class);

    @Autowired
    private PlayerRepository playerRepository;

    @Autowired
    private BetRepository betRepository;

    @Autowired
    private ObjectMapper objectMapper;

    public void processPayouts(String message) {
        log.info("Processando payout dal risultato: {}", message);
        try {
            JsonNode root = objectMapper.readTree(message);
            String winner = root.has("winner") ? root.get("winner").asText() : null;
            
            if (winner == null) {
                log.warn("Risultato senza winner: {}", message);
                return;
            }

            JsonNode payoutsNode = root.get("payouts");
            
            // Trova tutte le bet PENDING e aggiorna il loro status
            List<Bet> pendingBets = betRepository.findByStatus("PENDING");
            for (Bet bet : pendingBets) {
                if (bet.getSegment().equals(winner)) {
                    // Cerca il payout specifico calcolato da Erlang
                    BigDecimal finalPayout = null;
                    
                    if (payoutsNode != null && payoutsNode.isArray()) {
                        for (JsonNode p : payoutsNode) {
                            if (p.get("username").asText().equals(bet.getUsername())) {
                                if (p.has("payout")) {
                                    finalPayout = new BigDecimal(p.get("payout").asText());
                                }
                                break;
                            }
                        }
                    }
                    
                    if (finalPayout == null) {
                        // Fallback al calcolo base se payouts non è presente
                        String multStr = root.has("multiplier") ? root.get("multiplier").asText() : "1";
                        BigDecimal winnings = bet.getAmount().multiply(new BigDecimal(multStr));
                        finalPayout = bet.getAmount().add(winnings);
                    }
                    
                    bet.setStatus("WON");
                    bet.setPayout(finalPayout);
                    betRepository.save(bet);

                    // Accredita la vincita al giocatore
                    Optional<Player> optPlayer = playerRepository.findByUsername(bet.getUsername());
                    if (optPlayer.isPresent()) {
                        Player player = optPlayer.get();
                        player.setBalance(player.getBalance().add(finalPayout));
                        playerRepository.save(player);
                        log.info("Payout di ${} accreditato a {} (bet su {})",
                                finalPayout, bet.getUsername(), bet.getSegment());
                    }
                } else {
                    // PERDITA
                    bet.setStatus("LOST");
                    bet.setPayout(BigDecimal.ZERO);
                    betRepository.save(bet);
                }
            }
        } catch (Exception e) {
            log.error("Errore processando payout: {}", e.getMessage(), e);
        }
    }
}
