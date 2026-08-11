package com.crazytime;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@RestController
public class GatewayApplication {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    // Dice a Spring Boot di creare la coda "bets_queue" su RabbitMQ all'avvio
    @Bean
    public Queue betsQueue() {
        return new Queue("bets_queue", true);
    }

    public static void main(String[] args) {
        SpringApplication.run(GatewayApplication.class, args);
    }

    @PostMapping("/api/test-bet")
    public String sendTestBet() {
        String message = "Nuova scommessa piazzata dal nodo Java!";
        rabbitTemplate.convertAndSend("bets_queue", message);
        System.out.println("Inviato a RabbitMQ: " + message);
        return "Scommessa inviata con successo!";
    }
}