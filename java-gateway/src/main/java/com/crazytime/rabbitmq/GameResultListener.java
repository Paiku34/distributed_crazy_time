package com.crazytime.rabbitmq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

/**
 * Ascolta le code RabbitMQ su cui Erlang pubblica gli aggiornamenti
 * e li inoltra ai client WebSocket connessi.
 * Aggiorna anche la GameStateCache per le richieste REST.
 *
 * Topics WebSocket:
 *   /topic/game-results  → esito del giro di ruota e pagamenti
 *   /topic/game-timer    → countdown e stato della fase di gioco
 */
@Component
public class GameResultListener {

    private static final Logger log = LoggerFactory.getLogger(GameResultListener.class);

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    @Autowired
    private GameStateCache gameStateCache;
    
    @Autowired
    private PayoutListener payoutListener;

    @Autowired
    private BetRejectionHandler betRejectionHandler;

    @Autowired
    private LedgerListener ledgerListener;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * Unico punto di ingresso di results_queue. Il campo "type" decide chi
     * elabora il messaggio: prima veniva ignorato e QUALUNQUE messaggio finiva
     * a PayoutListener come se fosse l'esito di un round.
     */
    @RabbitListener(queues = "results_queue")
    public void receiveGameResult(String message) {
        log.info("Messaggio ricevuto da Erlang: {}", message);

        String type = "result";
        JsonNode root = null;
        try {
            root = objectMapper.readTree(message);
            type = root.path("type").asText("result");
        } catch (Exception e) {
            log.warn("Messaggio non parsabile su results_queue: {}", e.getMessage());
        }

        switch (type) {
            case "result" -> {
                payoutListener.processPayouts(message);
                gameStateCache.setLastResult(message);
                messagingTemplate.convertAndSend("/topic/game-results", message);
            }
            case "round_ledger" -> ledgerListener.processLedger(root);
            case "bet_rejected" -> betRejectionHandler.rejectBet(root);
            default -> log.warn("Tipo di messaggio non gestito su results_queue: {}", type);
        }
    }
    
    @RabbitListener(queues = "state_queue")
    public void receiveGameState(String message) {
        log.debug("Stato gioco: {}", message);
        updateCache(message);
        messagingTemplate.convertAndSend("/topic/game-timer", message);
    }

    private void updateCache(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);

            String phase = root.has("phase") ? root.get("phase").asText() : null;
            int timeLeft = root.has("time_left") ? root.get("time_left").asInt() : -1;
            int round = root.has("round") ? root.get("round").asInt() : -1;

            if (phase != null && timeLeft >= 0 && round >= 0) {
                gameStateCache.update(phase, timeLeft, round);
            } else {
                // Partial update fallback
                if (phase != null) gameStateCache.setPhase(phase);
                if (timeLeft >= 0) gameStateCache.setTimeLeft(timeLeft);
                if (round >= 0) gameStateCache.setRound(round);
            }
        } catch (Exception e) {
            log.warn("Errore parsing stato: {}", e.getMessage());
        }
    }
}
