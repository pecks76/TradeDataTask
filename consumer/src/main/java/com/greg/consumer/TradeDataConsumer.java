package com.greg.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.greg.consumer.service.TradeAggregateService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class TradeDataConsumer {

    private final TradeAggregateService aggregateService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String aggTopic;

    public TradeDataConsumer(TradeAggregateService aggregateService,
                             KafkaTemplate<String, String> kafkaTemplate,
                             @Value("${app.kafka.agg-topic}") String aggTopic) {
        this.aggregateService = aggregateService;
        this.kafkaTemplate = kafkaTemplate;
        this.aggTopic = aggTopic;
    }

    @KafkaListener(topics = "${app.kafka.topic}")
    public void consume(String message) throws Exception {

        System.out.println("Received message: " + message);

        if (message.contains("\"type\":\"DONE\"")) {
            System.out.println("DONE message received, sending aggregates...");
            String json = new ObjectMapper().writeValueAsString(aggregateService.getAggregates());
            kafkaTemplate.send(aggTopic, json);
            return;
        }

        JsonNode json = objectMapper.readTree(message);
        aggregateService.processRecord(json);
    }
}


