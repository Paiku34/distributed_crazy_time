package com.crazytime.service;

import com.crazytime.entity.Player;
import com.crazytime.repository.PlayerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessageSendingOperations;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Service
public class AuthService {

    private static final Logger log = LoggerFactory.getLogger(AuthService.class);
    private static final int TOKEN_EXPIRATION_MINUTES = 60;

    @Autowired
    private PlayerRepository playerRepository;

    @Autowired
    private PasswordEncoder passwordEncoder;

    @Autowired
    private SimpMessageSendingOperations messagingTemplate;

    @Transactional
    public Player register(String username, String password, BigDecimal initialBalance) {
        if (playerRepository.existsByUsername(username)) {
            throw new IllegalArgumentException("Utente già registrato");
        }
        
        String encodedPassword = passwordEncoder.encode(password);
        Player newPlayer = new Player(username, encodedPassword, initialBalance);
        return playerRepository.save(newPlayer);
    }

    @Transactional
    public Player login(String username, String password) {
        Player player = playerRepository.findByUsername(username)
                .orElseThrow(() -> new IllegalArgumentException("Utente non trovato"));

        if (!passwordEncoder.matches(password, player.getPasswordHash())) {
            throw new IllegalArgumentException("Password errata");
        }

        if (player.getSessionToken() != null) {
            // Kick della vecchia sessione
            log.info("Kick della precedente sessione per l'utente: {}", username);
            messagingTemplate.convertAndSendToUser(
                    username, 
                    "/queue/session", 
                    Map.of("type", "SESSION_TERMINATED", "reason", "new_login")
            );
        }

        String newToken = UUID.randomUUID().toString();
        player.setSessionToken(newToken);
        player.setSessionCreatedAt(LocalDateTime.now());
        
        return playerRepository.save(player);
    }

    @Transactional
    public void logout(String token) {
        playerRepository.findBySessionToken(token).ifPresent(player -> {
            player.setSessionToken(null);
            player.setSessionCreatedAt(null);
            playerRepository.save(player);
            log.info("Logout effettuato per l'utente: {}", player.getUsername());
        });
    }

    @Transactional(readOnly = true)
    public Optional<Player> resolveToken(String token) {
        if (token == null || token.isBlank()) {
            return Optional.empty();
        }

        return playerRepository.findBySessionToken(token).filter(player -> {
            if (player.getSessionCreatedAt() == null) {
                return false;
            }
            LocalDateTime expirationTime = player.getSessionCreatedAt().plusMinutes(TOKEN_EXPIRATION_MINUTES);
            return LocalDateTime.now().isBefore(expirationTime);
        });
    }
}
