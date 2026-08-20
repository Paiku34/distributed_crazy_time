package com.crazytime.rabbitmq;

import org.springframework.stereotype.Component;

/**
 * Cache in-memory dello stato corrente del gioco.
 * Aggiornato dal GameResultListener quando arrivano messaggi da Erlang.
 * Letto dal GameController per le richieste REST.
 *
 * Lo stato è un record immutabile dietro un campo volatile, così un lettore non può
 * mai osservare un aggiornamento parziale (es. nuovo round con fase vecchia).
 */
@Component
public class GameStateCache {

    private record GameState(String phase, int timeLeft, int round, String lastResult) {}
    private volatile GameState state = new GameState("waiting", 0, 0, "{}");

    public void update(String phase, int timeLeft, int round) {
        state = new GameState(phase, timeLeft, round, state.lastResult());
    }

    public String getPhase() { return state.phase(); }
    public void setPhase(String phase) { state = new GameState(phase, state.timeLeft(), state.round(), state.lastResult()); }

    public int getTimeLeft() { return state.timeLeft(); }
    public void setTimeLeft(int timeLeft) { state = new GameState(state.phase(), timeLeft, state.round(), state.lastResult()); }

    public int getRound() { return state.round(); }
    public void setRound(int round) { state = new GameState(state.phase(), state.timeLeft(), round, state.lastResult()); }

    public String getLastResult() { return state.lastResult(); }
    public void setLastResult(String lastResult) { state = new GameState(state.phase(), state.timeLeft(), state.round(), lastResult); }
}
