package com.crazytime.controller;

import com.crazytime.dto.PlaceBetRequest;
import com.crazytime.entity.Bet;
import com.crazytime.entity.Player;
import com.crazytime.repository.BetRepository;
import com.crazytime.repository.PlayerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
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

    @Autowired
    private PlayerRepository playerRepository;

    @Autowired
    private BetRepository betRepository;

    @Autowired
    private AmqpTemplate rabbitTemplate;

    @Autowired(required = false)
    private com.crazytime.rabbitmq.GameStateCache gameStateCache;

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

    @PostMapping("/force-result")
    public ResponseEntity<Map<String, Object>> forceResult(@RequestParam String segment) {
        log.info("Ricevuto comando DEV force-result per segmento: {}", segment);
        String json = String.format("{\"username\":\"admin\",\"amount\":0,\"segment\":\"FORCE_%s\"}", segment);
        rabbitTemplate.convertAndSend("bets_queue", json);
        return ResponseEntity.ok(Map.of("success", true, "segment", segment));
    }

    @PostMapping("/place-bet")
    @org.springframework.transaction.annotation.Transactional
    public ResponseEntity<Map<String, Object>> placeBet(
            @RequestAttribute("player") Player player,
            @RequestBody com.fasterxml.jackson.databind.JsonNode requestBody) {

        List<PlaceBetRequest> rawItems = new java.util.ArrayList<>();
        if (requestBody.isArray()) {
            for (com.fasterxml.jackson.databind.JsonNode node : requestBody) {
                BigDecimal amount = node.has("amount") ? new BigDecimal(node.get("amount").asText()) : null;
                String segment = node.has("segment") ? node.get("segment").asText() : null;
                rawItems.add(new PlaceBetRequest(amount, segment));
            }
        } else if (requestBody.isObject()) {
            BigDecimal amount = requestBody.has("amount") ? new BigDecimal(requestBody.get("amount").asText()) : null;
            String segment = requestBody.has("segment") ? requestBody.get("segment").asText() : null;
            rawItems.add(new PlaceBetRequest(amount, segment));
        } else {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "error", "Formato richiesta non valido"
            ));
        }

        if (rawItems.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "error", "Nessuna scommessa fornita"
            ));
        }

        List<String> validSegments = List.of("1", "2", "5", "10", "Pachinko", "CoinFlip", "CashHunt", "CrazyTime");
        
        // Aggrega scommesse per lo stesso segmento e convalida
        Map<String, BigDecimal> aggregatedBets = new java.util.LinkedHashMap<>();
        for (PlaceBetRequest item : rawItems) {
            BigDecimal amount = item.amount();
            String segment = item.segment();

            if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
                return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "error", "L'importo deve essere maggiore di zero"
                ));
            }

            if (segment == null || !validSegments.contains(segment)) {
                return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "error", "Segmento non valido. Valori ammessi: " + validSegments
                ));
            }

            aggregatedBets.merge(segment, amount, BigDecimal::add);
        }

        BigDecimal totalAmount = aggregatedBets.values().stream()
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        Player updatedPlayer = reloadPlayer(player);

        if (updatedPlayer.getBalance().compareTo(totalAmount) < 0) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "error", "Saldo insufficiente",
                "balance", updatedPlayer.getBalance()
            ));
        }

        // Scalata atomica del totale dal saldo
        updatedPlayer.setBalance(updatedPlayer.getBalance().subtract(totalAmount));
        playerRepository.save(updatedPlayer);

        // Salvataggio di 1 singola riga per segmento nel DB e invio a RabbitMQ
        int currentRound = gameStateCache != null ? gameStateCache.getRound() : 0;
        List<Map<String, Object>> savedBetsInfo = new java.util.ArrayList<>();
        for (Map.Entry<String, BigDecimal> entry : aggregatedBets.entrySet()) {
            String segment = entry.getKey();
            BigDecimal segAmount = entry.getValue();

            Bet bet = new Bet(updatedPlayer.getUsername(), segAmount, segment, currentRound);
            betRepository.save(bet);

            String message = String.format(java.util.Locale.US,
                "{\"username\":\"%s\",\"amount\":%.2f,\"segment\":\"%s\"}",
                updatedPlayer.getUsername(), segAmount, segment);
            rabbitTemplate.convertAndSend("bets_queue", message);
            log.info("Bet inviata a RabbitMQ: {}", message);

            savedBetsInfo.add(Map.of(
                "id", bet.getId(),
                "segment", segment,
                "amount", segAmount,
                "round", currentRound
            ));
        }

        return ResponseEntity.ok(Map.of(
            "success", true,
            "username", updatedPlayer.getUsername(),
            "total_amount", totalAmount,
            "new_balance", updatedPlayer.getBalance(),
            "bets", savedBetsInfo
        ));
    }

    public ResponseEntity<Map<String, Object>> placeBet(Player player, PlaceBetRequest request) {
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        return placeBet(player, mapper.valueToTree(request));
    }

    public ResponseEntity<Map<String, Object>> placeBet(Player player, List<PlaceBetRequest> requests) {
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        return placeBet(player, mapper.valueToTree(requests));
    }

    @PostMapping("/undo-bets")
    public ResponseEntity<Map<String, Object>> undoBets(@RequestAttribute("player") Player player) {
        return ResponseEntity.ok(Map.of(
            "success", true,
            "message", "Annullamento gestito localmente nella schedina (Bet Slip)."
        ));
    }

    @GetMapping("/history")
    public ResponseEntity<Map<String, Object>> getHistory(@RequestAttribute("player") Player player) {
        Player updatedPlayer = reloadPlayer(player);
        
        List<Bet> bets = betRepository.findByUsernameOrderByTimestampDesc(updatedPlayer.getUsername());
        List<Map<String, Object>> betList = bets.stream().map(b -> Map.<String, Object>of(
            "id", b.getId(),
            "round", b.getRound() != null ? b.getRound() : 0,
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