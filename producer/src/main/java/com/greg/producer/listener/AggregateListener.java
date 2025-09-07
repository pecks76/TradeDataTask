package com.greg.producer.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AggregateListener {

    @KafkaListener(topics = "${app.kafka.aggregation-topic}", groupId = "producer")
    public void listen(String message) {
        log.info("Aggregates received from Consumer: {}", message);
    }
}
