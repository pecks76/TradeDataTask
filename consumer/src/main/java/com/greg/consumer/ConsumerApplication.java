package com.greg.consumer;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class ConsumerApplication {
    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplication.class, args);
    }

    // Graceful shutdown hook to print aggregates
    @Bean
    public CommandLineRunner runner(com.greg.consumer.TradeDataConsumer consumer) {
        return args -> {
            System.out.println("✅ Consumer is running. Waiting for messages...");
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n🛑 Shutting down. Printing final aggregates:");
                consumer.printAggregates();
            }));
        };
    }
}

