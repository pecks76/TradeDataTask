package com.greg.producer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class TradeDataProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String topic;

    public TradeDataProducerService(KafkaTemplate<String, String> kafkaTemplate,
                                    @Value("${app.kafka.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void streamCsv(InputStream inputStream) throws Exception {

        try (CSVReader reader = new CSVReader(new InputStreamReader(inputStream))) {
            String[] headers = reader.readNext(); // First line
            if (headers == null) {
                log.error("CSV is empty, aborting.");
                return;
            }

            String[] values;
            int count = 0;
            while ((values = reader.readNext()) != null) {
                sendCsvRowAsJson(headers, values);
                count++;
                if (count % 1000 == 0) {
                    log.info("Sent {} messages", count);
                }
            }
        }

        // Send DONE message
        kafkaTemplate.send(topic, "{\"type\":\"DONE\"}");
        log.info("CSV streaming complete");
    }

    private void sendCsvRowAsJson(String[] headers, String[] values) {
        try {
            Map<String, String> jsonMap = new HashMap<>();
            for (int i = 0; i < headers.length && i < values.length; i++) {
                jsonMap.put(headers[i], values[i]);
            }
            String json = objectMapper.writeValueAsString(jsonMap);
            kafkaTemplate.send(topic, json).get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Failed to send message for row: {}", Arrays.toString(values), e);
        }
    }
}

