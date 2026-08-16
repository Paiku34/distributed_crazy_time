package com.crazytime.controller;

import com.crazytime.dto.AuthResponse;
import com.crazytime.dto.LoginRequest;
import com.crazytime.dto.RegisterRequest;
import com.crazytime.entity.Player;
import com.crazytime.service.AuthService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;

@RestController
@RequestMapping("/api/auth")
public class AuthController {

    private static final Logger log = LoggerFactory.getLogger(AuthController.class);

    @Autowired
    private AuthService authService;

    @PostMapping("/register")
    public ResponseEntity<AuthResponse> register(@RequestBody RegisterRequest request) {
        if (request.username() == null || request.username().isBlank()) {
            return ResponseEntity.badRequest().body(AuthResponse.error("Username non può essere vuoto"));
        }
        if (request.password() == null || request.password().length() < 4) {
            return ResponseEntity.badRequest().body(AuthResponse.error("Password deve avere almeno 4 caratteri"));
        }
        
        BigDecimal initBal = request.initialBalance() != null ? request.initialBalance() : new BigDecimal("100.00");
        if (initBal.compareTo(BigDecimal.ZERO) < 0 || initBal.compareTo(new BigDecimal("1000.00")) > 0) {
            return ResponseEntity.badRequest().body(AuthResponse.error("Il saldo iniziale deve essere compreso tra 0 e 1000"));
        }

        try {
            Player player = authService.register(request.username(), request.password(), initBal);
            log.info("Nuovo utente registrato: {} con saldo {}", player.getUsername(), player.getBalance());
            return ResponseEntity.ok(AuthResponse.success(null, player.getUsername(), player.getBalance(), "Registrazione completata"));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(AuthResponse.error(e.getMessage()));
        }
    }

    @PostMapping("/login")
    public ResponseEntity<AuthResponse> login(@RequestBody LoginRequest request) {
        try {
            Player player = authService.login(request.username(), request.password());
            log.info("Login effettuato: {}", player.getUsername());
            return ResponseEntity.ok(AuthResponse.success(
                player.getSessionToken(),
                player.getUsername(),
                player.getBalance(),
                "Login effettuato con successo"
            ));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(401).body(AuthResponse.error(e.getMessage()));
        }
    }

    @PostMapping("/logout")
    public ResponseEntity<AuthResponse> logout(@RequestHeader(value = "Authorization", required = false) String authHeader) {
        if (authHeader != null && authHeader.startsWith("Bearer ")) {
            String token = authHeader.substring(7);
            authService.logout(token);
        }
        return ResponseEntity.ok(AuthResponse.success(null, null, null, "Logout effettuato"));
    }
}
