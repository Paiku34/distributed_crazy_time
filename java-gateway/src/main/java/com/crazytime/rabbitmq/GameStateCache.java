package com.crazytime.rabbitmq;

import org.springframework.stereotype.Component;

/**
 * Cache in-memory dello stato corrente del gioco.
 * Aggiornato dal GameResultListener quando arrivano messaggi da Erlang.
 * Letto dal GameController per le richieste REST.
 */
@Component
public class GameStateCache {

    private volatile String phase = "waiting";
    private volatile int timeLeft = 0;
    private volatile int round = 0;
    private volatile String lastResult = "{}";

    public String getPhase() { return phase; }
    public void setPhase(String phase) { this.phase = phase; }

    public int getTimeLeft() { return timeLeft; }
    public void setTimeLeft(int timeLeft) { this.timeLeft = timeLeft; }

    public int getRound() { return round; }
    public void setRound(int round) { this.round = round; }

    public String getLastResult() { return lastResult; }
    public void setLastResult(String lastResult) { this.lastResult = lastResult; }
}
