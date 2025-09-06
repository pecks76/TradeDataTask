package com.greg.producer.listener;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class AggregateListener {

    @KafkaListener(topics = "${app.kafka.agg-topic}", groupId = "producer")
    public void listen(String message) {
        System.out.println("Aggregates received from Consumer:");
        System.out.println(message);
    }
}
