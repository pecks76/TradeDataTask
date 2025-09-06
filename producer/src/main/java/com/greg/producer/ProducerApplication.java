package com.greg.producer;

import com.greg.producer.service.TradeDataProducerService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.net.URL;

@SpringBootApplication
public class ProducerApplication implements CommandLineRunner {

    private final TradeDataProducerService producerService;

    @Value("${csv.url}")
    private String csvUrl;

    public ProducerApplication(TradeDataProducerService producerService) {
        this.producerService = producerService;
    }

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        // Stream the CSV to Kafka
        URL url = new URL(csvUrl);
        producerService.streamCsv(url.openStream());
    }
}

