package com.example;

import com.example.service.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;

@SpringBootApplication
public class Application implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(Application.class);

    @Autowired
    private Processor processor;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder builder) {
        return builder.build();
    }

    @Override
    public void run(String... args) throws Exception {
        try {
            LOG.info("Start process message to send to Rest Service on {}", LocalDateTime.now());
            processor.readFile();
            LOG.info("Finish process message to send to Rest Service on {}", LocalDateTime.now());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }
}
