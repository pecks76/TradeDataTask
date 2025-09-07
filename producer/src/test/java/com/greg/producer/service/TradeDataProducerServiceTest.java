package com.greg.producer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
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
    void testStreamCsv_sendsAllRowsAndDoneMessage() throws Exception {
        String csv = "country_or_area,year,flow,trade_usd\n" +
                "Bulgaria,1998,Import,1000\n" +
                "France,1998,Export,2000\n";

        InputStream inputStream = new ByteArrayInputStream(csv.getBytes());

        producerService.streamCsv(inputStream);

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(3)).send(eq("trade-data-topic"), captor.capture());

        List<String> messages = captor.getAllValues();
        ObjectMapper objectMapper = new ObjectMapper();

        JsonNode msg1 = objectMapper.readTree(messages.get(0));
        JsonNode msg2 = objectMapper.readTree(messages.get(1));

        assertEquals("Bulgaria", msg1.get("country_or_area").asText());
        assertEquals("1000", msg1.get("trade_usd").asText());
        assertEquals("France", msg2.get("country_or_area").asText());
        assertEquals("2000", msg2.get("trade_usd").asText());
        assertEquals("{\"type\":\"DONE\"}", messages.get(2));
    }

    @Test
    void testStreamCsv_emptyCsv() throws Exception {
        ByteArrayInputStream inputStream = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));

        producerService.streamCsv(inputStream);

        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }

    @Test
    void testSendCsvRowAsJson_moreColumnsThanHeaders() throws Exception {

        String csv = "country_or_area\n" +
                "Bulgaria,1998,Import,1000\n" +
                "France,1998,Export,2000\n";

        InputStream inputStream = new ByteArrayInputStream(csv.getBytes());

        producerService.streamCsv(inputStream);

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(3)).send(eq("trade-data-topic"), captor.capture());

        List<String> messages = captor.getAllValues();

        assertEquals("{\"country_or_area\":\"Bulgaria\"}", messages.get(0));
        assertEquals("{\"country_or_area\":\"France\"}", messages.get(1));
        assertEquals("{\"type\":\"DONE\"}", messages.get(2));
    }

    @Test
    void testStreamCsv_moreHeadersThanColumns() throws Exception {
        String csv = "country_or_area,year,flow,trade_usd\n" +
                "Bulgaria\n" +
                "France\n";

        InputStream inputStream = new ByteArrayInputStream(csv.getBytes());

        producerService.streamCsv(inputStream);

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(3)).send(eq("trade-data-topic"), captor.capture());

        List<String> messages = captor.getAllValues();

        assertEquals("{\"country_or_area\":\"Bulgaria\"}", messages.get(0));
        assertEquals("{\"country_or_area\":\"France\"}", messages.get(1));
        assertEquals("{\"type\":\"DONE\"}", messages.get(2));
    }
}
