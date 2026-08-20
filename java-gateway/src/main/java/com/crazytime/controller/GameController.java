package com.crazytime.controller;

import com.crazytime.dto.GameChoiceRequest;
import com.crazytime.entity.Player;
import com.crazytime.rabbitmq.GameStateCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import org.springframework.amqp.core.AmqpTemplate;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;

@RestController
@RequestMapping("/api/game")
public class GameController {

    @Autowired
    private GameStateCache gameStateCache;

    @Autowired
    private AmqpTemplate rabbitTemplate;

    @Autowired
    private ObjectMapper objectMapper;

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
    public ResponseEntity<?> makeChoice(
            @RequestAttribute("player") Player player,
            @RequestBody GameChoiceRequest request) {
        
        if (request.minigame() == null || request.choice() == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "Minigame e choice richiesti"));
        }

        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("type", "minigame_choice");
        payload.put("username", player.getUsername());
        payload.put("minigame", request.minigame());
        payload.put("choice", request.choice());

        rabbitTemplate.convertAndSend("bets_queue", payload.toString());
        
        return ResponseEntity.ok(Map.of("success", true, "message", "Choice registered"));
    }
}
