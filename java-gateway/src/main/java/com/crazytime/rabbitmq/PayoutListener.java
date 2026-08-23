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

    @Transactional
    public void processPayouts(String message) {
        log.info("Processando payout dal risultato: {}", message);
        JsonNode root;
        try {
            root = objectMapper.readTree(message);
        } catch (Exception e) {
            // Solo il parsing e' fuori dalla transazione: un JSON malformato non
            // e' un errore di dominio. Tutto cio' che segue deve poter fallire
            // facendo ROLLBACK, quindi non va avvolto in un catch.
            log.error("Risultato non parsabile: {}", e.getMessage(), e);
            return;
        }

        String winner = root.path("winner").asText(null);
        if (winner == null) {
            log.warn("Risultato senza winner: {}", message);
            return;
        }

        int round = root.path("round").asInt(-1);
        JsonNode payoutsNode = root.get("payouts");

        // Query per ROUND, non scansione globale delle PENDING: quella pagava
        // anche bet di round estranei. Il round e' quello autoritativo di
        // Erlang, che LedgerListener ha gia' scritto sulle bet del ledger.
        List<Bet> pendingBets = (round >= 0)
                ? betRepository.findByRoundAndStatus(round, "PENDING")
                : betRepository.findByStatus("PENDING");

        for (Bet bet : pendingBets) {
            if (!bet.getSegment().equals(winner)) {
                bet.setStatus("LOST");
                bet.setPayout(BigDecimal.ZERO);
                betRepository.save(bet);
                continue;
            }

            BigDecimal finalPayout = findPayout(payoutsNode, bet);

            if (finalPayout == null) {
                // Fallback al calcolo base se payouts non e' utilizzabile.
                BigDecimal multiplier = new BigDecimal(root.path("multiplier").asText("1"));
                // multiplier <= 0 e' il marcatore dei minigiochi asincroni:
                // li' l'importo sta solo nell'array payouts.
                if (multiplier.compareTo(BigDecimal.ZERO) <= 0) {
                    log.warn("Payout non trovato per la bet {} e multiplier non valido ({}), skip",
                            bet.getBetId(), multiplier);
                    continue;
                }
                finalPayout = bet.getAmount().add(bet.getAmount().multiply(multiplier));
            }

            bet.setStatus("WON");
            bet.setPayout(finalPayout);
            betRepository.save(bet);

            Optional<Player> optPlayer = playerRepository.findByUsernameForUpdate(bet.getUsername());
            if (optPlayer.isPresent()) {
                Player player = optPlayer.get();
                player.setBalance(player.getBalance().add(finalPayout));
                playerRepository.save(player);
                log.info("Payout di ${} accreditato a {} (bet {} su {})",
                        finalPayout, bet.getUsername(), bet.getBetId(), bet.getSegment());
            }
        }
    }

    /**
     * Cerca il payout della singola scommessa per bet_id.
     *
     * Il match per username era ambiguo: due puntate dello stesso giocatore
     * sullo stesso segmento trovavano la stessa entry, e per evitare di
     * pagarla due volte la si rimuoveva dall'array. Con l'identificativo la
     * corrispondenza e' esatta e non serve consumare la lista.
     */
    private BigDecimal findPayout(JsonNode payoutsNode, Bet bet) {
        if (payoutsNode == null || !payoutsNode.isArray() || bet.getBetId() == null) {
            return null;
        }
        for (JsonNode p : payoutsNode) {
            if (bet.getBetId().equals(p.path("bet_id").asText(null)) && p.has("payout")) {
                return new BigDecimal(p.get("payout").asText());
            }
        }
        return null;
    }
}
