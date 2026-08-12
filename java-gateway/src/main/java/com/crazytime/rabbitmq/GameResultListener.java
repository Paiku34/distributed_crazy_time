package com.crazytime.rabbitmq;

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

    @RabbitListener(queues = "results_queue")
    public void receiveGameResult(String message) {
        log.info("Risultato ricevuto da Erlang: {}", message);
        payoutListener.processPayouts(message);
        gameStateCache.setLastResult(message);
        messagingTemplate.convertAndSend("/topic/game-results", message);
    }
    
    @RabbitListener(queues = "state_queue")
    public void receiveGameState(String message) {
        log.debug("Stato gioco: {}", message);
        // Parsing minimale per aggiornare la cache
        updateCache(message);
        messagingTemplate.convertAndSend("/topic/game-timer", message);
    }

    /**
     * Parsing semplice del JSON di stato per aggiornare la cache.
     * Formato atteso:
     *   {"type":"timer","round":1,"time_left":8,"phase":"betting"}
     *   {"type":"timer","round":1,"time_left":0,"phase":"spinning","winner_index":23,"winner":"5"}
     *   {"type":"timer","round":1,"time_left":7,"phase":"minigame","minigame":"Pachinko"}
     */
    private void updateCache(String json) {
        try {
            String phase = extractStringField(json, "phase");
            if (phase != null) gameStateCache.setPhase(phase);

            String timeLeft = extractStringField(json, "time_left");
            if (timeLeft != null) gameStateCache.setTimeLeft(Integer.parseInt(timeLeft));

            String round = extractStringField(json, "round");
            if (round != null) gameStateCache.setRound(Integer.parseInt(round));
        } catch (Exception e) {
            log.warn("Errore parsing stato: {}", e.getMessage());
        }
    }

    private String extractStringField(String json, String field) {
        // Cerca "field":"value" o "field":value
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(
            "\"" + field + "\"\\s*:\\s*\"?([^,\"\\}]+)\"?");
        java.util.regex.Matcher m = p.matcher(json);
        return m.find() ? m.group(1).trim() : null;
    }
}
