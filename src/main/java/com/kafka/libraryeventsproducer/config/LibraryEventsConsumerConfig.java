package com.kafka.libraryeventsproducer.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

    @Bean
    ConcurrentKafkaListenerContainerFactory<?,?> kafkaListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
        ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConcurrency(3);
        factory.setErrorHandler((thrownException, data) -> {
            log.info("Exception in consumerConfig is {} and the record is {}", thrownException.getMessage(), data);
        });
        factory.setRetryTemplate(retryTemplate());
        return factory;
    }

    private RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(simpleRetryPolicy());
        retryTemplate.setBackOffPolicy(setBackoffPolicy());
        return retryTemplate;
    }

    private BackOffPolicy setBackoffPolicy() {
        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(1000);
        return fixedBackOffPolicy;
    }

    private RetryPolicy simpleRetryPolicy() {
        Map<Class<? extends Throwable>, Boolean> exceptionMaps = new HashMap<>();
        exceptionMaps.put(IllegalArgumentException.class, false);
        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(3, exceptionMaps, true);
        return simpleRetryPolicy;
    }
}
