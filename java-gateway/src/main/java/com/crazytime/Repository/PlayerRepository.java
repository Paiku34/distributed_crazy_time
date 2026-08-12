package com.crazytime.repository;

import com.crazytime.entity.Player;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;

public interface PlayerRepository extends JpaRepository<Player, Long> {
    // Spring capisce da solo che deve fare "SELECT * FROM Player WHERE username = ?"
    Optional<Player> findByUsername(String username);
}