package com.crazytime.dto;

import java.math.BigDecimal;

public record PlaceBetRequest(BigDecimal amount, String segment) {
}
