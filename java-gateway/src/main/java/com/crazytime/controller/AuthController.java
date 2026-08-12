package com.crazytime.controller;

import com.crazytime.entity.Player;
import com.crazytime.repository.PlayerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Optional;

/**
 * Controller per autenticazione: login e logout.
 * Use cases dal PDF:
 *   - "An Unlogged User can: Login to the service as a Player"
 *   - "A Logged Player can: Logout"
 */
@RestController
@RequestMapping("/api/auth")
public class AuthController {

    private static final Logger log = LoggerFactory.getLogger(AuthController.class);

    @Autowired
    private PlayerRepository playerRepository;

    /**
     * POST /api/auth/login?username=X&password=Y
     */
    @PostMapping("/login")
    public ResponseEntity<Map<String, Object>> login(
            @RequestParam String username,
            @RequestParam String password) {

        Optional<Player> optionalPlayer = playerRepository.findByUsername(username);
        if (optionalPlayer.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "error", "Utente non trovato"
            ));
        }

        Player player = optionalPlayer.get();
        if (!player.checkPassword(password)) {
            return ResponseEntity.status(401).body(Map.of(
                "success", false,
                "error", "Password errata"
            ));
        }

        if (player.isLoggedIn()) {
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Utente già loggato",
                "username", player.getUsername(),
                "balance", player.getBalance()
            ));
        }

        player.setLoggedIn(true);
        playerRepository.save(player);
        log.info("Login effettuato: {}", username);

        return ResponseEntity.ok(Map.of(
            "success", true,
            "username", player.getUsername(),
            "balance", player.getBalance()
        ));
    }

    /**
     * POST /api/auth/logout?username=X
     */
    @PostMapping("/logout")
    public ResponseEntity<Map<String, Object>> logout(@RequestParam String username) {
        Optional<Player> optionalPlayer = playerRepository.findByUsername(username);
        if (optionalPlayer.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "error", "Utente non trovato"
            ));
        }

        Player player = optionalPlayer.get();
        if (!player.isLoggedIn()) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "error", "Utente non è loggato"
            ));
        }

        player.setLoggedIn(false);
        playerRepository.save(player);
        log.info("Logout effettuato: {}", username);

        return ResponseEntity.ok(Map.of(
            "success", true,
            "message", "Logout effettuato con successo"
        ));
    }
}
