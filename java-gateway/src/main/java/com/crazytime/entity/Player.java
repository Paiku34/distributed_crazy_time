package com.crazytime.entity;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

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

    @Column(nullable = false)
    private boolean loggedIn = false;

    // Costruttore vuoto obbligatorio per JPA
    public Player() {}

    public Player(String username, String password, BigDecimal balance) {
        this.username = username;
        this.passwordHash = hashPassword(password);
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
    public boolean isLoggedIn() { return loggedIn; }
    public void setLoggedIn(boolean loggedIn) { this.loggedIn = loggedIn; }

    /** Verifica che la password in chiaro corrisponda all'hash memorizzato */
    public boolean checkPassword(String plainPassword) {
        return this.passwordHash.equals(hashPassword(plainPassword));
    }

    /** Hash SHA-256 semplice per la password */
    public static String hashPassword(String password) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(password.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 non disponibile", e);
        }
    }
}