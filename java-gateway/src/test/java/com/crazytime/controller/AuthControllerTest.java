package com.crazytime.controller;

import com.crazytime.entity.Player;
import com.crazytime.repository.PlayerRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Test per AuthController — Login e Logout.
 * Use cases dal PDF:
 *   - "An Unlogged User can: Login to the service as a Player"
 *   - "A Logged Player can: Logout"
 */
public class AuthControllerTest {

    @Mock
    private PlayerRepository playerRepository;

    @InjectMocks
    private AuthController authController;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    // --- LOGIN ---

    @Test
    public void testLoginSuccess() {
        Player player = new Player("alice", "pass1234", new BigDecimal("100.00"));
        when(playerRepository.findByUsername("alice")).thenReturn(Optional.of(player));

        ResponseEntity<Map<String, Object>> response = authController.login("alice", "pass1234");

        assertEquals(200, response.getStatusCode().value());
        assertTrue((Boolean) response.getBody().get("success"));
        assertEquals("alice", response.getBody().get("username"));
        assertTrue(player.isLoggedIn());
        verify(playerRepository, times(1)).save(player);
    }

    @Test
    public void testLoginUserNotFound() {
        when(playerRepository.findByUsername("ghost")).thenReturn(Optional.empty());

        ResponseEntity<Map<String, Object>> response = authController.login("ghost", "pass1234");

        assertEquals(400, response.getStatusCode().value());
        assertFalse((Boolean) response.getBody().get("success"));
        assertEquals("Utente non trovato", response.getBody().get("error"));
    }

    @Test
    public void testLoginWrongPassword() {
        Player player = new Player("alice", "pass1234", new BigDecimal("100.00"));
        when(playerRepository.findByUsername("alice")).thenReturn(Optional.of(player));

        ResponseEntity<Map<String, Object>> response = authController.login("alice", "wrongpass");

        assertEquals(401, response.getStatusCode().value());
        assertFalse((Boolean) response.getBody().get("success"));
        assertEquals("Password errata", response.getBody().get("error"));
    }

    @Test
    public void testLoginAlreadyLoggedIn() {
        Player player = new Player("alice", "pass1234", new BigDecimal("100.00"));
        player.setLoggedIn(true);
        when(playerRepository.findByUsername("alice")).thenReturn(Optional.of(player));

        ResponseEntity<Map<String, Object>> response = authController.login("alice", "pass1234");

        assertEquals(200, response.getStatusCode().value());
        assertTrue((Boolean) response.getBody().get("success"));
        // Should return "Utente già loggato" message
        assertEquals("Utente già loggato", response.getBody().get("message"));
        // Should NOT call save again (player was already logged in)
        verify(playerRepository, never()).save(any(Player.class));
    }

    // --- LOGOUT ---

    @Test
    public void testLogoutSuccess() {
        Player player = new Player("alice", "pass1234", new BigDecimal("100.00"));
        player.setLoggedIn(true);
        when(playerRepository.findByUsername("alice")).thenReturn(Optional.of(player));

        ResponseEntity<Map<String, Object>> response = authController.logout("alice");

        assertEquals(200, response.getStatusCode().value());
        assertTrue((Boolean) response.getBody().get("success"));
        assertFalse(player.isLoggedIn());
        verify(playerRepository, times(1)).save(player);
    }

    @Test
    public void testLogoutUserNotFound() {
        when(playerRepository.findByUsername("ghost")).thenReturn(Optional.empty());

        ResponseEntity<Map<String, Object>> response = authController.logout("ghost");

        assertEquals(400, response.getStatusCode().value());
        assertFalse((Boolean) response.getBody().get("success"));
    }

    @Test
    public void testLogoutUserNotLoggedIn() {
        Player player = new Player("alice", "pass1234", new BigDecimal("100.00"));
        player.setLoggedIn(false);
        when(playerRepository.findByUsername("alice")).thenReturn(Optional.of(player));

        ResponseEntity<Map<String, Object>> response = authController.logout("alice");

        assertEquals(400, response.getStatusCode().value());
        assertFalse((Boolean) response.getBody().get("success"));
        assertEquals("Utente non è loggato", response.getBody().get("error"));
    }
}
