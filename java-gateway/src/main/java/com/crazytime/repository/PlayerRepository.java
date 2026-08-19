package com.crazytime.repository;

import com.crazytime.entity.Player;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import java.util.Optional;

public interface PlayerRepository extends JpaRepository<Player, Long> {
    Optional<Player> findByUsername(String username);
    boolean existsByUsername(String username);
    Optional<Player> findBySessionToken(String sessionToken);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT p FROM Player p WHERE p.username = :username")
    Optional<Player> findByUsernameForUpdate(@Param("username") String username);
}