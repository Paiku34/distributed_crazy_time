package com.crazytime.repository;

import com.crazytime.entity.Bet;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.Optional;

public interface BetRepository extends JpaRepository<Bet, Long> {
    List<Bet> findByUsernameOrderByTimestampDesc(String username);
    List<Bet> findByStatus(String status);
    List<Bet> findByUsernameAndStatus(String username, String status);

    /** Ricerca puntuale per identificativo: sostituisce il match per importo. */
    Optional<Bet> findByBetId(String betId);

    /** Bet di un singolo round in un dato stato: sostituisce la scansione globale. */
    List<Bet> findByRoundAndStatus(Integer round, String status);
}
