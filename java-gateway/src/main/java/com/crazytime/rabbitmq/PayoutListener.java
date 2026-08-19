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
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Iterator;
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

    // FIX 0.1.2: Add @Transactional + pessimistic locking to prevent lost updates
    @Transactional
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
                    // FIX 0.1.4: Cerca il payout specifico e RIMUOVILO per evitare duplicati
                    BigDecimal finalPayout = null;
                    
                    if (payoutsNode != null && payoutsNode.isArray()) {
                        Iterator<JsonNode> it = payoutsNode.iterator();
                        while (it.hasNext()) {
                            JsonNode p = it.next();
                            if (p.has("username") && p.get("username").asText().equals(bet.getUsername())) {
                                if (p.has("payout")) {
                                    finalPayout = new BigDecimal(p.get("payout").asText());
                                }
                                it.remove();  // Prevent this entry from being matched again
                                break;
                            }
                        }
                    }
                    
                    if (finalPayout == null) {
                        // Fallback al calcolo base se payouts non è presente
                        String multStr = root.has("multiplier") ? root.get("multiplier").asText() : "1";
                        BigDecimal multiplier = new BigDecimal(multStr);
                        // Don't use fallback if multiplier is 0 or negative (async minigame marker)
                        if (multiplier.compareTo(BigDecimal.ZERO) <= 0) {
                            log.warn("Payout non trovato per {} e multiplier non valido ({}), skip",
                                    bet.getUsername(), multStr);
                            continue;
                        }
                        BigDecimal winnings = bet.getAmount().multiply(multiplier);
                        finalPayout = bet.getAmount().add(winnings);
                    }
                    
                    bet.setStatus("WON");
                    bet.setPayout(finalPayout);
                    betRepository.save(bet);

                    // FIX 0.1.2: Use pessimistic locking for balance update
                    Optional<Player> optPlayer = playerRepository.findByUsernameForUpdate(bet.getUsername());
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
