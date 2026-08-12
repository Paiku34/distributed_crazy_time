package com.crazytime.repository;

import com.crazytime.entity.Bet;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface BetRepository extends JpaRepository<Bet, Long> {
    List<Bet> findByUsernameOrderByTimestampDesc(String username);
    List<Bet> findByStatus(String status);
    List<Bet> findByUsernameAndStatus(String username, String status);
}
