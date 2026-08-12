package com.crazytime.controller;

import com.crazytime.rabbitmq.GameStateCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * Controller per lo stato del gioco.
 * Use case dal PDF: "A Logged Player can: View the live countdown timer
 * and the current state of the wheel/mini-games"
 */
@RestController
@RequestMapping("/api/game")
public class GameController {

    @Autowired
    private GameStateCache gameStateCache;

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
}
