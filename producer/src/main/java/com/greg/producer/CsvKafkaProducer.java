package com.greg.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
class CsvKafkaProducer implements CommandLineRunner {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${csv.url}")
    private String csvUrl;

    @Value("${kafka.topic}")
    private String topic;

    public CsvKafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

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
                //System.out.println("Producing: " + json);

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

        System.out.println("CSV streaming complete.");
    }
}

