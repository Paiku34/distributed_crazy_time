package com.crazytime.dto;

import java.math.BigDecimal;

public record RegisterRequest(String username, String password, BigDecimal initialBalance) {
}
