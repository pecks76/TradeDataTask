package com.greg.producer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class TradeDataProducerServiceTest {

    private KafkaTemplate<String, String> kafkaTemplate;
    private TradeDataProducerService producerService;

    @BeforeEach
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        when(kafkaTemplate.send(anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        producerService = new TradeDataProducerService(kafkaTemplate, "trade-data-topic");
    }

    @Test
    void testStreamCsv_fromInputStream() throws Exception {
        String csv = "country_or_area,year,flow,trade_usd\n" +
                "Bulgaria,1998,Import,1000\n" +
                "France,1998,Export,2000\n";

        InputStream inputStream = new ByteArrayInputStream(csv.getBytes());

        producerService.streamCsv(inputStream);

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(3)).send(eq("trade-data-topic"), captor.capture());
        // 2 rows + DONE message

        List<String> messages = captor.getAllValues();

        // Check first message
        assertEquals("Bulgaria", new ObjectMapper().readTree(messages.get(0)).get("country_or_area").asText());
        // Check second message
        assertEquals("France", new ObjectMapper().readTree(messages.get(1)).get("country_or_area").asText());
        // Check DONE message
        assertEquals("{\"type\":\"DONE\"}", messages.get(2));
    }
}
