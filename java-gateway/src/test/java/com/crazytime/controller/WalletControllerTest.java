package com.crazytime.controller;

import com.crazytime.dto.PlaceBetRequest;
import com.crazytime.entity.Bet;
import com.crazytime.entity.Player;
import com.crazytime.repository.BetRepository;
import com.crazytime.repository.PlayerRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    @InjectMocks
    private WalletController walletController;

    private Player mockPlayer;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        mockPlayer = new Player("testuser", "hashed", new BigDecimal("100.00"));
        mockPlayer.setId(1L);
    }

    @Test
    public void testPlaceBetSuccess() {
        when(playerRepository.findById(1L)).thenReturn(Optional.of(mockPlayer));
        
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
        when(playerRepository.findById(1L)).thenReturn(Optional.of(mockPlayer));
        
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
        assertEquals(new BigDecimal("20.00"), response.getBody().get("total_amount"));
        verify(playerRepository, times(1)).save(mockPlayer);
        // 2 distinct segments saved (Pachinko and 10)
        verify(betRepository, times(2)).save(any(Bet.class));
        verify(rabbitTemplate, times(2)).convertAndSend(eq("bets_queue"), anyString());
    }

    @Test
    public void testPlaceBetInsufficientFunds() {
        mockPlayer.setBalance(new BigDecimal("10.00"));
        when(playerRepository.findById(1L)).thenReturn(Optional.of(mockPlayer));
        
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
