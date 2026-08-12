package com.crazytime.controller;

import com.crazytime.entity.Bet;
import com.crazytime.entity.Player;
import com.crazytime.repository.BetRepository;
import com.crazytime.repository.PlayerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/wallet")
public class WalletController {

    private static final Logger log = LoggerFactory.getLogger(WalletController.class);
    private static final BigDecimal WELCOME_BONUS = new BigDecimal("100.00");

    @Autowired
    private PlayerRepository playerRepository;

    @Autowired
    private BetRepository betRepository;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    /**
     * POST /api/wallet/register?username=X&password=Y
     * Registra un nuovo giocatore con un saldo iniziale di $100.
     * Use case PDF: "An Unregistered User can: Register to the service to create a wallet"
     */
    @PostMapping("/register")
    public ResponseEntity<Map<String, Object>> register(
            @RequestParam String username,
            @RequestParam String password,
            @RequestParam(required = false, defaultValue = "100.00") BigDecimal initialBalance) {

        if (username == null || username.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "error", "Username non può essere vuoto"
            ));
        }
        if (password == null || password.length() < 4) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "error", "Password deve avere almeno 4 caratteri"
            ));
        }
        if (initialBalance.compareTo(BigDecimal.ZERO) < 0 || initialBalance.compareTo(new BigDecimal("1000.00")) > 0) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "error", "Il saldo iniziale deve essere compreso tra 0 e 1000"
            ));
        }
        if (playerRepository.findByUsername(username).isPresent()) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "error", "Utente già registrato"
            ));
        }

        Player newPlayer = new Player(username, password, initialBalance);
        playerRepository.save(newPlayer);
        log.info("Nuovo utente registrato: {} con saldo {}", username, initialBalance);

        return ResponseEntity.ok(Map.of(
            "success", true,
            "username", username,
            "balance", initialBalance
        ));
    }

    /**
     * GET /api/wallet/balance?username=X
     * Restituisce il saldo attuale del giocatore.
     * Use case PDF: "A Logged Player can: View their current wallet balance"
     */
    @GetMapping("/balance")
    public ResponseEntity<Map<String, Object>> getBalance(@RequestParam String username) {
        Optional<Player> optionalPlayer = playerRepository.findByUsername(username);
        if (optionalPlayer.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "error", "Utente non trovato"
            ));
        }
        Player player = optionalPlayer.get();
        return ResponseEntity.ok(Map.of(
            "success", true,
            "username", player.getUsername(),
            "balance", player.getBalance()
        ));
    }

    /**
     * POST /api/wallet/force-result?segment=Pachinko
     * Forza l'esito del prossimo round (solo per test).
     */
    @PostMapping("/force-result")
    public ResponseEntity<Map<String, Object>> forceResult(@RequestParam String segment) {
        log.info("Ricevuto comando DEV force-result per segmento: {}", segment);
        String json = String.format("{\"username\":\"admin\",\"amount\":0,\"segment\":\"FORCE_%s\"}", segment);
        rabbitTemplate.convertAndSend("bets_queue", json);
        return ResponseEntity.ok(Map.of("success", true, "segment", segment));
    }

    /**
     * POST /api/wallet/place-bet?username=X&amount=N&segment=Y
     * Piazza una scommessa.
     * Use case PDF: "A Logged Player can: Place bets on one or more wheel segments"
     */
    @PostMapping("/place-bet")
    public ResponseEntity<Map<String, Object>> placeBet(
            @RequestParam String username,
            @RequestParam BigDecimal amount,
            @RequestParam String segment) {

        // Validazione importo
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "error", "L'importo deve essere maggiore di zero"
            ));
        }

        // Validazione segmento
        List<String> validSegments = List.of("1", "2", "5", "10", "Pachinko", "CoinFlip", "CashHunt", "CrazyTime");
        if (!validSegments.contains(segment)) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "error", "Segmento non valido. Valori ammessi: " + validSegments
            ));
        }

        Optional<Player> optionalPlayer = playerRepository.findByUsername(username);
        if (optionalPlayer.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "error", "Utente non trovato"
            ));
        }

        Player player = optionalPlayer.get();

        // Verifica che l'utente sia loggato
        if (!player.isLoggedIn()) {
            return ResponseEntity.status(401).body(Map.of(
                "success", false,
                "error", "Devi effettuare il login prima di scommettere"
            ));
        }

        if (player.getBalance().compareTo(amount) < 0) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "error", "Saldo insufficiente",
                "balance", player.getBalance()
            ));
        }

        // Scala il saldo
        player.setBalance(player.getBalance().subtract(amount));
        playerRepository.save(player);

        // Salva la scommessa nello storico
        Bet bet = new Bet(username, amount, segment);
        betRepository.save(bet);

        // Invia il messaggio a RabbitMQ come JSON
        String message = String.format(
            "{\"username\":\"%s\",\"amount\":%.2f,\"segment\":\"%s\"}",
            username, amount, segment);
        rabbitTemplate.convertAndSend("bets_queue", message);
        log.info("Bet inviata a RabbitMQ: {}", message);

        return ResponseEntity.ok(Map.of(
            "success", true,
            "username", username,
            "amount", amount,
            "segment", segment,
            "new_balance", player.getBalance()
        ));
    }

    /**
     * GET /api/wallet/history?username=X
     * Restituisce lo storico delle scommesse del giocatore.
     * Use case PDF: "A Logged Player can: View their betting history"
     */
    @GetMapping("/history")
    public ResponseEntity<Map<String, Object>> getHistory(@RequestParam String username) {
        Optional<Player> optionalPlayer = playerRepository.findByUsername(username);
        if (optionalPlayer.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "error", "Utente non trovato"
            ));
        }

        List<Bet> bets = betRepository.findByUsernameOrderByTimestampDesc(username);
        List<Map<String, Object>> betList = bets.stream().map(b -> Map.<String, Object>of(
            "id", b.getId(),
            "amount", b.getAmount(),
            "segment", b.getSegment(),
            "status", b.getStatus(),
            "payout", b.getPayout(),
            "timestamp", b.getTimestamp().toString()
        )).collect(Collectors.toList());

        return ResponseEntity.ok(Map.of(
            "success", true,
            "username", username,
            "bets", betList
        ));
    }
}