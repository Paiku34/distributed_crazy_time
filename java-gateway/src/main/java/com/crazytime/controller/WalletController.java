package com.crazytime.controller;

import com.crazytime.dto.PlaceBetRequest;
import com.crazytime.entity.Bet;
import com.crazytime.entity.Player;
import com.crazytime.rabbitmq.GameStateCache;
import com.crazytime.repository.BetRepository;
import com.crazytime.repository.PlayerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/wallet")
public class WalletController {

    private static final Logger log = LoggerFactory.getLogger(WalletController.class);

    @Autowired
    private PlayerRepository playerRepository;

    @Autowired
    private BetRepository betRepository;

    @Autowired
    private AmqpTemplate rabbitTemplate;

    @Autowired
    private GameStateCache gameStateCache;

    private Player reloadPlayer(Player player) {
        return playerRepository.findById(player.getId())
                .orElseThrow(() -> new IllegalArgumentException("Player non trovato nel DB"));
    }

    @GetMapping("/balance")
    public ResponseEntity<Map<String, Object>> getBalance(@RequestAttribute("player") Player player) {
        Player updatedPlayer = reloadPlayer(player);
        return ResponseEntity.ok(Map.of(
            "success", true,
            "username", updatedPlayer.getUsername(),
            "balance", updatedPlayer.getBalance()
        ));
    }

    // FIX 0.1.7: Restrict force-result to admin-only
    @PostMapping("/force-result")
    public ResponseEntity<Map<String, Object>> forceResult(@RequestParam String segment,
                                                            @RequestAttribute("player") Player player) {
        if (!"admin".equals(player.getUsername())) {
            return ResponseEntity.status(403).body(Map.of("success", false, "error", "Admin only"));
        }
        log.info("Ricevuto comando DEV force-result per segmento: {}", segment);
        String json = String.format("{\"username\":\"admin\",\"amount\":0,\"segment\":\"FORCE_%s\"}", segment);
        rabbitTemplate.convertAndSend("bets_queue", json);
        return ResponseEntity.ok(Map.of("success", true, "segment", segment));
    }

    // FIX 0.1.1 + 0.1.5 + 0.1.6: @Transactional + pessimistic locking + phase check
    @Transactional
    @PostMapping("/place-bet")
    public ResponseEntity<Map<String, Object>> placeBet(
            @RequestAttribute("player") Player player,
            @RequestBody PlaceBetRequest request) {

        BigDecimal amount = request.amount();
        String segment = request.segment();

        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "error", "L'importo deve essere maggiore di zero"
            ));
        }

        List<String> validSegments = List.of("1", "2", "5", "10", "Pachinko", "CoinFlip", "CashHunt", "CrazyTime");
        if (!validSegments.contains(segment)) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "error", "Segmento non valido. Valori ammessi: " + validSegments
            ));
        }

        // FIX 0.1.6: Check game phase before accepting bets
        String phase = gameStateCache.getPhase();
        if (!"betting".equals(phase)) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "error", "Le scommesse sono chiuse (fase: " + phase + ")"
            ));
        }

        // FIX 0.1.1: Use pessimistic locking to prevent double-spending
        Player updatedPlayer = playerRepository.findByUsernameForUpdate(player.getUsername())
                .orElseThrow(() -> new RuntimeException("Player not found"));

        if (updatedPlayer.getBalance().compareTo(amount) < 0) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "error", "Saldo insufficiente",
                "balance", updatedPlayer.getBalance()
            ));
        }

        updatedPlayer.setBalance(updatedPlayer.getBalance().subtract(amount));
        playerRepository.save(updatedPlayer);

        Bet bet = new Bet(updatedPlayer.getUsername(), amount, segment);
        betRepository.save(bet);

        String message = String.format(java.util.Locale.US,
            "{\"username\":\"%s\",\"amount\":%.2f,\"segment\":\"%s\"}",
            updatedPlayer.getUsername(), amount, segment);
        rabbitTemplate.convertAndSend("bets_queue", message);
        log.info("Bet inviata a RabbitMQ: {}", message);

        return ResponseEntity.ok(Map.of(
            "success", true,
            "username", updatedPlayer.getUsername(),
            "amount", amount,
            "segment", segment,
            "new_balance", updatedPlayer.getBalance()
        ));
    }

    @PostMapping("/undo-bets")
    public ResponseEntity<Map<String, Object>> undoBets(@RequestAttribute("player") Player player) {
        Player updatedPlayer = reloadPlayer(player);
        
        String message = String.format("{\"username\":\"%s\",\"amount\":0.0,\"segment\":\"UNDO_BETS\"}", updatedPlayer.getUsername());
        rabbitTemplate.convertAndSend("bets_queue", message);
        log.info("Comando undo_bets inviato a RabbitMQ: {}", message);

        return ResponseEntity.ok(Map.of(
            "success", true,
            "message", "Richiesta di annullamento inviata."
        ));
    }

    @GetMapping("/history")
    public ResponseEntity<Map<String, Object>> getHistory(@RequestAttribute("player") Player player) {
        Player updatedPlayer = reloadPlayer(player);
        
        List<Bet> bets = betRepository.findByUsernameOrderByTimestampDesc(updatedPlayer.getUsername());
        List<Map<String, Object>> betList = bets.stream().map(b -> Map.<String, Object>of(
            "id", b.getId(),
            "amount", b.getAmount(),
            "segment", b.getSegment(),
            "status", b.getStatus() != null ? b.getStatus() : "PENDING",
            "payout", b.getPayout() != null ? b.getPayout() : BigDecimal.ZERO,
            "timestamp", b.getTimestamp().toString()
        )).collect(Collectors.toList());

        return ResponseEntity.ok(Map.of(
            "success", true,
            "username", updatedPlayer.getUsername(),
            "bets", betList
        ));
    }
}