package com.crazytime.service;

import com.crazytime.entity.Player;
import com.crazytime.repository.PlayerRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.messaging.simp.SimpMessageSendingOperations;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class AuthServiceTest {

    @Mock
    private PlayerRepository playerRepository;

    @Mock
    private PasswordEncoder passwordEncoder;

    @Mock
    private SimpMessageSendingOperations messagingTemplate;

    @InjectMocks
    private AuthService authService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void registerNewUser() {
        when(playerRepository.existsByUsername("alice")).thenReturn(false);
        when(passwordEncoder.encode("pass123")).thenReturn("encoded123");
        
        Player savedPlayer = new Player("alice", "encoded123", new BigDecimal("100"));
        when(playerRepository.save(any())).thenReturn(savedPlayer);

        Player result = authService.register("alice", "pass123", new BigDecimal("100"));
        
        assertNotNull(result);
        assertEquals("alice", result.getUsername());
    }

    @Test
    void loginSuccess() {
        Player player = new Player("alice", "encoded123", new BigDecimal("100"));
        when(playerRepository.findByUsername("alice")).thenReturn(Optional.of(player));
        when(passwordEncoder.matches("pass123", "encoded123")).thenReturn(true);
        when(playerRepository.save(any())).thenReturn(player);

        Player result = authService.login("alice", "pass123");
        
        assertNotNull(result.getSessionToken());
        assertNotNull(result.getSessionCreatedAt());
        verify(messagingTemplate, never()).convertAndSendToUser(any(), any(), any());
    }

    @Test
    void loginWithKick() {
        Player player = new Player("alice", "encoded123", new BigDecimal("100"));
        player.setSessionToken("old-token");
        when(playerRepository.findByUsername("alice")).thenReturn(Optional.of(player));
        when(passwordEncoder.matches("pass123", "encoded123")).thenReturn(true);
        when(playerRepository.save(any())).thenReturn(player);

        Player result = authService.login("alice", "pass123");
        
        assertNotNull(result.getSessionToken());
        assertNotEquals("old-token", result.getSessionToken());
        verify(messagingTemplate).convertAndSendToUser(eq("alice"), eq("/queue/session"), any());
    }

    @Test
    void resolveTokenValid() {
        Player player = new Player("alice", "encoded123", new BigDecimal("100"));
        player.setSessionToken("valid-token");
        player.setSessionCreatedAt(LocalDateTime.now().minusMinutes(30)); // 30 min ago (valid)
        when(playerRepository.findBySessionToken("valid-token")).thenReturn(Optional.of(player));

        Optional<Player> result = authService.resolveToken("valid-token");
        assertTrue(result.isPresent());
    }

    @Test
    void resolveTokenExpired() {
        Player player = new Player("alice", "encoded123", new BigDecimal("100"));
        player.setSessionToken("expired-token");
        player.setSessionCreatedAt(LocalDateTime.now().minusMinutes(65)); // 65 min ago (expired)
        when(playerRepository.findBySessionToken("expired-token")).thenReturn(Optional.of(player));

        Optional<Player> result = authService.resolveToken("expired-token");
        assertTrue(result.isEmpty());
    }
}
