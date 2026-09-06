package com.crazytime.controller;

import com.crazytime.dto.PlaceBetRequest;
import com.crazytime.entity.Bet;
import com.crazytime.entity.Player;
import com.crazytime.repository.BetRepository;
import com.crazytime.repository.PlayerRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class WalletControllerTest {

    @Mock
    private PlayerRepository playerRepository;

    @Mock
    private BetRepository betRepository;

    @Mock
    private AmqpTemplate rabbitTemplate;

    // Istanza reale: il controller serializza il payload della bet con createObjectNode(),
    // che su un mock restituirebbe null.
    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    private WalletController walletController;

    private Player mockPlayer;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        mockPlayer = new Player("testuser", "hashed", new BigDecimal("100.00"));
        mockPlayer.setId(1L);

        // In produzione l'id della Bet lo assegna JPA dentro save(); sul mock resterebbe
        // null e la costruzione della risposta (Map.of) fallirebbe con NPE.
        AtomicLong betIdSeq = new AtomicLong(0);
        when(betRepository.save(any(Bet.class))).thenAnswer(inv -> {
            Bet saved = inv.getArgument(0);
            if (saved.getId() == null) {
                saved.setId(betIdSeq.incrementAndGet());
            }
            return saved;
        });
    }

    @Test
    public void testPlaceBetSuccess() {
        when(playerRepository.findByUsernameForUpdate("testuser")).thenReturn(Optional.of(mockPlayer));
        
        ResponseEntity<Map<String, Object>> response = walletController.placeBet(
            mockPlayer, new PlaceBetRequest(new BigDecimal("20.00"), "Pachinko"));
        
        assertEquals(200, response.getStatusCode().value());
        assertTrue((Boolean) response.getBody().get("success"));
        assertEquals(new BigDecimal("80.00"), mockPlayer.getBalance());
        verify(playerRepository, times(1)).save(mockPlayer);
        verify(betRepository, times(1)).save(any(Bet.class));
        verify(rabbitTemplate, times(1)).convertAndSend(eq("bets_queue"), anyString());
    }

    @Test
    public void testPlaceBetSlipMultipleItemsSuccess() {
        when(playerRepository.findByUsernameForUpdate("testuser")).thenReturn(Optional.of(mockPlayer));
        
        List<PlaceBetRequest> slip = List.of(
            new PlaceBetRequest(new BigDecimal("10.00"), "Pachinko"),
            new PlaceBetRequest(new BigDecimal("5.00"), "10"),
            new PlaceBetRequest(new BigDecimal("5.00"), "Pachinko") // duplicate segment to test aggregation
        );

        ResponseEntity<Map<String, Object>> response = walletController.placeBet(mockPlayer, slip);
        
        assertEquals(200, response.getStatusCode().value());
        assertTrue((Boolean) response.getBody().get("success"));
        // Total should be 20.00 (15 on Pachinko + 5 on 10)
        assertEquals(new BigDecimal("80.00"), mockPlayer.getBalance());
        // Confronto numerico: Jackson normalizza i BigDecimal in valueToTree (stripTrailingZeros),
        // quindi il totale arriva come 20 e non 20.00. La scala non e' significativa qui.
        assertEquals(0, new BigDecimal("20.00")
            .compareTo((BigDecimal) response.getBody().get("total_amount")));
        verify(playerRepository, times(1)).save(mockPlayer);
        // 2 distinct segments saved (Pachinko and 10)
        verify(betRepository, times(2)).save(any(Bet.class));
        verify(rabbitTemplate, times(2)).convertAndSend(eq("bets_queue"), anyString());
    }

    @Test
    public void testPlaceBetInsufficientFunds() {
        mockPlayer.setBalance(new BigDecimal("10.00"));
        when(playerRepository.findByUsernameForUpdate("testuser")).thenReturn(Optional.of(mockPlayer));
        
        ResponseEntity<Map<String, Object>> response = walletController.placeBet(
            mockPlayer, new PlaceBetRequest(new BigDecimal("20.00"), "Pachinko"));
        
        assertEquals(400, response.getStatusCode().value());
        assertFalse((Boolean) response.getBody().get("success"));
        assertEquals(new BigDecimal("10.00"), mockPlayer.getBalance());
        verify(playerRepository, never()).save(mockPlayer);
    }

    @Test
    public void testGetBalanceSuccess() {
        when(playerRepository.findById(1L)).thenReturn(Optional.of(mockPlayer));
        
        ResponseEntity<Map<String, Object>> response = walletController.getBalance(mockPlayer);
        
        assertEquals(200, response.getStatusCode().value());
        assertTrue((Boolean) response.getBody().get("success"));
        assertEquals(new BigDecimal("100.00"), response.getBody().get("balance"));
    }

    @Test
    public void testGetHistorySuccess() {
        when(playerRepository.findById(1L)).thenReturn(Optional.of(mockPlayer));

        Bet bet1 = new Bet("testuser", new BigDecimal("10.00"), "Pachinko");
        bet1.setId(1L);
        when(betRepository.findByUsernameOrderByTimestampDesc("testuser"))
            .thenReturn(List.of(bet1));

        ResponseEntity<Map<String, Object>> response = walletController.getHistory(mockPlayer);

        assertEquals(200, response.getStatusCode().value());
        assertTrue((Boolean) response.getBody().get("success"));
        List<?> bets = (List<?>) response.getBody().get("bets");
        assertEquals(1, bets.size());
    }
}
