package com.crazytime;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class GatewayApplication {

    @Bean
    public Queue betsQueue() {
        return new Queue("bets_queue", true);
    }

    @Bean
    public Queue resultsQueue() {
        return new Queue("results_queue", true);
    }
    
    @Bean
    public Queue stateQueue() {
        return new Queue("state_queue", true);
    }

    @Bean
    public Queue refundsQueue() {
        return new Queue("refunds_queue", true);
    }

    public static void main(String[] args) {
        SpringApplication.run(GatewayApplication.class, args);
    }
}