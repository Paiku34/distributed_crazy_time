package com.crazytime.entity;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Entità per lo storico delle scommesse.
 * Ogni piazzamento di una bet viene registrato qui.
 */
@Entity
@Table(name = "bets")
public class Bet {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String username;

    @Column(nullable = false)
    private BigDecimal amount;

    @Column(nullable = false)
    private String segment;

    @Column(nullable = false)
    private LocalDateTime timestamp;

    @Column(nullable = false)
    private String status;  // PENDING, WON, LOST, REFUNDED

    @Column
    private BigDecimal payout;

    @Column
    private Integer round;

    /**
     * Identificativo univoco della scommessa, generato dal gateway all'accettazione
     * e propagato nel messaggio AMQP. E' cio' che rende indirizzabile una singola
     * puntata: senza, rimborsi e payout devono essere riconciliati per importo o
     * per username, e due puntate di pari importo diventano indistinguibili.
     *
     * Nullable per compatibilita' con le righe gia' presenti nel database.
     */
    @Column(unique = true)
    private String betId;

    // Costruttore vuoto per JPA
    public Bet() {}

    public Bet(String username, BigDecimal amount, String segment) {
        this(username, amount, segment, 0);
    }

    public Bet(String username, BigDecimal amount, String segment, Integer round) {
        this.username = username;
        this.amount = amount;
        this.segment = segment;
        this.timestamp = LocalDateTime.now();
        this.status = "PENDING";
        this.payout = BigDecimal.ZERO;
        this.round = round != null ? round : 0;
    }

    // Getters e Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public String getSegment() { return segment; }
    public void setSegment(String segment) { this.segment = segment; }
    public LocalDateTime getTimestamp() { return timestamp; }
    public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public BigDecimal getPayout() { return payout; }
    public void setPayout(BigDecimal payout) { this.payout = payout; }
    public Integer getRound() { return round; }
    public void setRound(Integer round) { this.round = round; }
    public String getBetId() { return betId; }
    public void setBetId(String betId) { this.betId = betId; }
}
