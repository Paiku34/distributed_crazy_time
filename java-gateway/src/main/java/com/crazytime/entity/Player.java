package com.crazytime.entity;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "players")
public class Player {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(unique = true, nullable = false)
    private String username;

    @Column(nullable = false)
    private String passwordHash;
    
    @Column(nullable = false)
    private BigDecimal balance;

    @Column(unique = true, nullable = true)
    private String sessionToken;

    @Column(nullable = true)
    private LocalDateTime sessionCreatedAt;

    // Costruttore vuoto obbligatorio per JPA
    public Player() {}

    public Player(String username, String passwordHash, BigDecimal balance) {
        this.username = username;
        this.passwordHash = passwordHash;
        this.balance = balance;
    }

    // Getters e Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }
    public String getPasswordHash() { return passwordHash; }
    public void setPasswordHash(String passwordHash) { this.passwordHash = passwordHash; }
    public BigDecimal getBalance() { return balance; }
    public void setBalance(BigDecimal balance) { this.balance = balance; }
    public String getSessionToken() { return sessionToken; }
    public void setSessionToken(String sessionToken) { this.sessionToken = sessionToken; }
    public LocalDateTime getSessionCreatedAt() { return sessionCreatedAt; }
    public void setSessionCreatedAt(LocalDateTime sessionCreatedAt) { this.sessionCreatedAt = sessionCreatedAt; }
}