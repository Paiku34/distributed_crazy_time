package com.crazytime.controller;

import com.crazytime.dto.GameChoiceRequest;
import com.crazytime.entity.Player;
import com.crazytime.rabbitmq.GameStateCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import org.springframework.amqp.core.AmqpTemplate;
import java.util.Map;

@RestController
@RequestMapping("/api/game")
public class GameController {

    @Autowired
    private GameStateCache gameStateCache;

    @Autowired
    private AmqpTemplate rabbitTemplate;

    @GetMapping("/state")
    public ResponseEntity<Map<String, Object>> getGameState() {
        return ResponseEntity.ok(Map.of(
            "success", true,
            "phase", gameStateCache.getPhase(),
            "time_left", gameStateCache.getTimeLeft(),
            "round", gameStateCache.getRound(),
            "last_result", gameStateCache.getLastResult() != null ? gameStateCache.getLastResult() : "None"
        ));
    }

    @PostMapping("/choice")
    public ResponseEntity<Map<String, Object>> makeChoice(
            @RequestAttribute("player") Player player,
            @RequestBody GameChoiceRequest request) {
        
        String minigame = request.minigame();
        String choice = request.choice();

        String jsonPayload = String.format(
            "{\"type\":\"minigame_choice\",\"username\":\"%s\",\"minigame\":\"%s\",\"choice\":\"%s\"}",
            player.getUsername().replace("\"", "\\\""), 
            minigame.replace("\"", "\\\""), 
            choice.replace("\"", "\\\"")
        );
        
        rabbitTemplate.convertAndSend("bets_queue", jsonPayload);
        
        return ResponseEntity.ok(Map.of("success", true, "message", "Choice registered"));
    }
}
