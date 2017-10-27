package com.example.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;

@Service
public class Sender {

    private static final Logger LOG = LoggerFactory.getLogger(Sender.class);

    @Value("${endpoint}")
    private String url;

    @Autowired
    private RestTemplate restTemplate;

    @Async
    public void send(Map<Integer, List<Integer>> map) {
        if (map != null) {
            map.forEach((key, values) -> {
                LOG.trace("Preparing payload for row:{} values:{}", key, values);
                send(values.get(0), values.get(1));
                send(values.get(2), values.get(3));
                send(values.get(4), values.get(5));
            });
        }
    }

    @Async
    public void send(final Integer accountId, final Integer orderId) {
        try {
            LOG.debug("Preparing to send Account:{} Order:{} to Rest Service.", accountId, orderId);

            final String message = getMessage(accountId, orderId);
            final HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            final HttpEntity<String> request = new HttpEntity<>(message, headers);

            LOG.trace("Request: URI={} Body={}", url, request);

            final ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);

            LOG.trace("Response={}", response);
        } catch (Exception e) {
            LOG.error("Error to call Rest Service.", e);
            if (e.getMessage().contains("Connection refused")) {
                throw new RuntimeException(e);
            }
        }
    }

    private String getMessage(final Integer accountId, final Integer orderId) throws JsonProcessingException {
        final ObjectMapper mapper = new ObjectMapper();
        final ObjectNode root = mapper.createObjectNode();
        final ObjectNode data = mapper.createObjectNode();
        root.putPOJO("data", data);
        data.put("ACCOUNT_NUMBER", accountId);
        data.put("ORDER_NUMBER", orderId);

        return mapper.writeValueAsString(root);
    }
}
