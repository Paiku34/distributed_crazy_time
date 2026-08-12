package com.crazytime.controller;

import com.crazytime.entity.Bet;
import com.crazytime.entity.Player;
import com.crazytime.repository.BetRepository;
import com.crazytime.repository.PlayerRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
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
    private RabbitTemplate rabbitTemplate;

    @InjectMocks
    private WalletController walletController;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    // --- REGISTER ---

    @Test
    public void testRegisterNewUser() {
        when(playerRepository.findByUsername("testuser")).thenReturn(Optional.empty());
        
        ResponseEntity<Map<String, Object>> response = walletController.register("testuser", "pass1234", new BigDecimal("100.00"));
        
        assertEquals(200, response.getStatusCode().value());
        assertTrue((Boolean) response.getBody().get("success"));
        assertEquals("testuser", response.getBody().get("username"));
        verify(playerRepository, times(1)).save(any(Player.class));
    }

    @Test
    public void testRegisterExistingUser() {
        Player existing = new Player("testuser", "pass1234", new BigDecimal("100.00"));
        when(playerRepository.findByUsername("testuser")).thenReturn(Optional.of(existing));
        
        ResponseEntity<Map<String, Object>> response = walletController.register("testuser", "pass1234", new BigDecimal("100.00"));
        
        assertEquals(400, response.getStatusCode().value());
        assertFalse((Boolean) response.getBody().get("success"));
        verify(playerRepository, never()).save(any(Player.class));
    }

    @Test
    public void testRegisterShortPassword() {
        ResponseEntity<Map<String, Object>> response = walletController.register("testuser", "ab", new BigDecimal("100.00"));
        
        assertEquals(400, response.getStatusCode().value());
        assertFalse((Boolean) response.getBody().get("success"));
        assertTrue(response.getBody().get("error").toString().contains("4 caratteri"));
    }

    @Test
    public void testRegisterBlankUsername() {
        ResponseEntity<Map<String, Object>> response = walletController.register("  ", "pass1234", new BigDecimal("100.00"));
        
        assertEquals(400, response.getStatusCode().value());
        assertFalse((Boolean) response.getBody().get("success"));
    }

    @Test
    public void testRegisterInvalidBalance() {
        ResponseEntity<Map<String, Object>> response = walletController.register("testuser", "pass1234", new BigDecimal("1500.00"));
        
        assertEquals(400, response.getStatusCode().value());
        assertFalse((Boolean) response.getBody().get("success"));
        assertTrue(response.getBody().get("error").toString().contains("1000"));
    }

    // --- PLACE BET ---

    @Test
    public void testPlaceBetSuccess() {
        Player player = new Player("testuser", "pass1234", new BigDecimal("100.00"));
        player.setLoggedIn(true);
        when(playerRepository.findByUsername("testuser")).thenReturn(Optional.of(player));
        
        ResponseEntity<Map<String, Object>> response = walletController.placeBet(
            "testuser", new BigDecimal("20.00"), "Pachinko");
        
        assertEquals(200, response.getStatusCode().value());
        assertTrue((Boolean) response.getBody().get("success"));
        assertEquals(new BigDecimal("80.00"), player.getBalance());
        verify(playerRepository, times(1)).save(player);
        verify(betRepository, times(1)).save(any(Bet.class));
        verify(rabbitTemplate, times(1)).convertAndSend(eq("bets_queue"), anyString());
    }

    @Test
    public void testPlaceBetInsufficientFunds() {
        Player player = new Player("testuser", "pass1234", new BigDecimal("10.00"));
        player.setLoggedIn(true);
        when(playerRepository.findByUsername("testuser")).thenReturn(Optional.of(player));
        
        ResponseEntity<Map<String, Object>> response = walletController.placeBet(
            "testuser", new BigDecimal("20.00"), "Pachinko");
        
        assertEquals(400, response.getStatusCode().value());
        assertFalse((Boolean) response.getBody().get("success"));
        assertEquals(new BigDecimal("10.00"), player.getBalance());
        verify(playerRepository, never()).save(player);
    }

    @Test
    public void testPlaceBetUserNotFound() {
        when(playerRepository.findByUsername("ghost")).thenReturn(Optional.empty());
        
        ResponseEntity<Map<String, Object>> response = walletController.placeBet(
            "ghost", new BigDecimal("10.00"), "CrazyTime");
        
        assertEquals(400, response.getStatusCode().value());
        assertFalse((Boolean) response.getBody().get("success"));
        assertEquals("Utente non trovato", response.getBody().get("error"));
    }

    @Test
    public void testPlaceBetZeroAmount() {
        ResponseEntity<Map<String, Object>> response = walletController.placeBet(
            "testuser", BigDecimal.ZERO, "CoinFlip");
        
        assertEquals(400, response.getStatusCode().value());
        assertFalse((Boolean) response.getBody().get("success"));
    }

    @Test
    public void testPlaceBetInvalidSegment() {
        Player player = new Player("testuser", "pass1234", new BigDecimal("100.00"));
        player.setLoggedIn(true);
        when(playerRepository.findByUsername("testuser")).thenReturn(Optional.of(player));

        ResponseEntity<Map<String, Object>> response = walletController.placeBet(
            "testuser", new BigDecimal("10.00"), "SegmentoInventato");
        
        assertEquals(400, response.getStatusCode().value());
        assertFalse((Boolean) response.getBody().get("success"));
        assertTrue(response.getBody().get("error").toString().contains("non valido"));
    }

    @Test
    public void testPlaceBetNotLoggedIn() {
        Player player = new Player("testuser", "pass1234", new BigDecimal("100.00"));
        player.setLoggedIn(false);
        when(playerRepository.findByUsername("testuser")).thenReturn(Optional.of(player));

        ResponseEntity<Map<String, Object>> response = walletController.placeBet(
            "testuser", new BigDecimal("10.00"), "Pachinko");
        
        assertEquals(401, response.getStatusCode().value());
        assertFalse((Boolean) response.getBody().get("success"));
        assertTrue(response.getBody().get("error").toString().contains("login"));
    }

    // --- GET BALANCE ---

    @Test
    public void testGetBalanceSuccess() {
        Player player = new Player("testuser", "pass1234", new BigDecimal("75.50"));
        when(playerRepository.findByUsername("testuser")).thenReturn(Optional.of(player));
        
        ResponseEntity<Map<String, Object>> response = walletController.getBalance("testuser");
        
        assertEquals(200, response.getStatusCode().value());
        assertTrue((Boolean) response.getBody().get("success"));
        assertEquals(new BigDecimal("75.50"), response.getBody().get("balance"));
    }

    @Test
    public void testGetBalanceUserNotFound() {
        when(playerRepository.findByUsername("nobody")).thenReturn(Optional.empty());
        
        ResponseEntity<Map<String, Object>> response = walletController.getBalance("nobody");
        
        assertEquals(400, response.getStatusCode().value());
        assertFalse((Boolean) response.getBody().get("success"));
    }

    // --- BETTING HISTORY ---

    @Test
    public void testGetHistorySuccess() {
        Player player = new Player("testuser", "pass1234", new BigDecimal("100.00"));
        when(playerRepository.findByUsername("testuser")).thenReturn(Optional.of(player));

        Bet bet1 = new Bet("testuser", new BigDecimal("10.00"), "Pachinko");
        bet1.setId(1L);
        Bet bet2 = new Bet("testuser", new BigDecimal("5.00"), "CoinFlip");
        bet2.setId(2L);
        when(betRepository.findByUsernameOrderByTimestampDesc("testuser"))
            .thenReturn(List.of(bet1, bet2));

        ResponseEntity<Map<String, Object>> response = walletController.getHistory("testuser");

        assertEquals(200, response.getStatusCode().value());
        assertTrue((Boolean) response.getBody().get("success"));
        List<?> bets = (List<?>) response.getBody().get("bets");
        assertEquals(2, bets.size());
    }

    @Test
    public void testGetHistoryUserNotFound() {
        when(playerRepository.findByUsername("ghost")).thenReturn(Optional.empty());

        ResponseEntity<Map<String, Object>> response = walletController.getHistory("ghost");

        assertEquals(400, response.getStatusCode().value());
        assertFalse((Boolean) response.getBody().get("success"));
    }
}
