package com.crazytime.controller;

import com.crazytime.rabbitmq.GameStateCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import java.util.Map;

/**
 * Controller per lo stato del gioco.
 */
@RestController
@RequestMapping("/api/game")
public class GameController {

    @Autowired
    private GameStateCache gameStateCache;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    /**
     * GET /api/game/state
     * Restituisce fase corrente, tempo rimanente, numero round.
     */
    @GetMapping("/state")
    public ResponseEntity<Map<String, Object>> getGameState() {
        return ResponseEntity.ok(Map.of(
            "success", true,
            "phase", gameStateCache.getPhase(),
            "time_left", gameStateCache.getTimeLeft(),
            "round", gameStateCache.getRound(),
            "last_result", gameStateCache.getLastResult()
        ));
    }

    /**
     * POST /api/game/choice
     * Invia la scelta del minigioco a Erlang.
     */
    @PostMapping("/choice")
    public ResponseEntity<Map<String, Object>> makeChoice(
            @RequestParam String username,
            @RequestParam String minigame,
            @RequestParam String choice) {
        
        String jsonPayload = String.format(
            "{\"type\":\"minigame_choice\",\"username\":\"%s\",\"minigame\":\"%s\",\"choice\":\"%s\"}",
            username.replace("\"", "\\\""), 
            minigame.replace("\"", "\\\""), 
            choice.replace("\"", "\\\"")
        );
        
        rabbitTemplate.convertAndSend("bets_queue", jsonPayload);
        
        return ResponseEntity.ok(Map.of("success", true, "message", "Choice registered"));
    }
}
