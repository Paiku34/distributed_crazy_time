package com.crazytime.controller;

import com.crazytime.dto.AuthResponse;
import com.crazytime.dto.LoginRequest;
import com.crazytime.dto.RegisterRequest;
import com.crazytime.entity.Player;
import com.crazytime.service.AuthService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class AuthControllerTest {

    @Mock
    private AuthService authService;

    @InjectMocks
    private AuthController authController;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testLoginSuccess() {
        Player player = new Player("alice", "hashed", new BigDecimal("100.00"));
        player.setSessionToken("token-123");
        when(authService.login("alice", "pass1234")).thenReturn(player);

        ResponseEntity<AuthResponse> response = authController.login(new LoginRequest("alice", "pass1234"));

        assertEquals(200, response.getStatusCode().value());
        assertTrue(response.getBody().success());
        assertEquals("token-123", response.getBody().token());
    }

    @Test
    public void testLoginWrongPassword() {
        when(authService.login("alice", "wrongpass")).thenThrow(new IllegalArgumentException("Password errata"));

        ResponseEntity<AuthResponse> response = authController.login(new LoginRequest("alice", "wrongpass"));

        assertEquals(401, response.getStatusCode().value());
        assertFalse(response.getBody().success());
        assertEquals("Password errata", response.getBody().error());
    }

    @Test
    public void testRegisterSuccess() {
        Player player = new Player("alice", "hashed", new BigDecimal("100.00"));
        when(authService.register(eq("alice"), eq("pass1234"), any())).thenReturn(player);

        ResponseEntity<AuthResponse> response = authController.register(new RegisterRequest("alice", "pass1234", new BigDecimal("100.00")));

        assertEquals(200, response.getStatusCode().value());
        assertTrue(response.getBody().success());
        assertEquals("alice", response.getBody().username());
    }
}
