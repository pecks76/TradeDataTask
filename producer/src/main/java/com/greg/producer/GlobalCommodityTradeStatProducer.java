package com.greg.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

// todo: why is this a Component and the Consumer a Service?
@Component
class GlobalCommodityTradeStatProducer implements CommandLineRunner {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${csv.url}")
    private String csvUrl;

    @Value("${app.kafka.topic}")
    private String topic;

    @Value("${app.kafka.agg-topic}")
    private String aggTopic;

    public GlobalCommodityTradeStatProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    // todo: you might want to separate this out.
    // essentially it converts to JSON, and sends to Kafka
    public void run(String... args) throws Exception {
        URL url = new URL(csvUrl);

        //System.out.println("Starting CSV streaming from: " + csvUrl);

        try (CSVReader reader = new CSVReader(new InputStreamReader(url.openStream()))) {
            String[] headers = reader.readNext(); // First line
            if (headers == null) {
                System.err.println("❌ CSV is empty, aborting.");
                return;
            }

            String[] values;
            int count = 0;
            while ((values = reader.readNext()) != null) {

                Map<String, String> jsonMap = new HashMap<>();
                for (int i = 0; i < headers.length && i < values.length; i++) {
                    jsonMap.put(headers[i], values[i]);
                }

                String json = objectMapper.writeValueAsString(jsonMap);

                // this try/catch with sleep handles timing issues with Kafka not being ready yet
                try {
                    kafkaTemplate.send(topic, json).get(5, TimeUnit.SECONDS);
                    count++;
                    if (count % 1000 == 0) {
                        System.out.println("Sent " + count + " messages...");
                    }
                } catch (Exception e) {
                    System.err.println("⚠️ Kafka send failed: " + e.getMessage());
                    Thread.sleep(2000);
                }
            }
        }

        // Send DONE message
        kafkaTemplate.send(topic, "{\"type\":\"DONE\"}");
        System.out.println("CSV streaming complete.");
    }

    @KafkaListener(topics = "${app.kafka.agg-topic}", groupId = "producer")
    public void listenForAggregates(String message) {
        System.out.println("Aggregates received from Consumer:");
        System.out.println(message);
    }
}

