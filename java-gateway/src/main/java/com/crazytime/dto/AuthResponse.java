package com.crazytime.dto;

import java.math.BigDecimal;

public record AuthResponse(
    boolean success,
    String message,
    String token,
    String username,
    BigDecimal balance,
    String error
) {
    public static AuthResponse success(String token, String username, BigDecimal balance, String message) {
        return new AuthResponse(true, message, token, username, balance, null);
    }

    public static AuthResponse error(String error) {
        return new AuthResponse(false, null, null, null, null, error);
    }
}
