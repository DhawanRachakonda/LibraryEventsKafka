package com.kafka.libraryeventsproducer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.libraryeventsproducer.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
public class LibraryEventProducer {

    private final KafkaTemplate<Integer, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Autowired
    public LibraryEventProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        ListenableFuture<SendResult<Integer, String>> listenableFuture = this.kafkaTemplate.sendDefault(key, value);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable throwable) {
                handleFailure(key, value, throwable);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> integerStringSendResult) {
                handleSuccess(key, value, integerStringSendResult);
            }
        });
    }

    public SendResult<Integer, String> sendLibraryEventSync(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        SendResult<Integer, String> message = this.kafkaTemplate.sendDefault(key, value).get();
        return message;
    }

    public void sendLibraryEventUsingProducerRecord(LibraryEvent libraryEvent) throws JsonProcessingException {

        String topic = "library-events";
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        ProducerRecord<Integer, String> record = this.buildProducerRecord(topic, key, value);
        ListenableFuture<SendResult<Integer, String>> listenableFuture = this.kafkaTemplate.send(record);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable throwable) {
                handleFailure(key, value, throwable);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> integerStringSendResult) {
                handleSuccess(key, value, integerStringSendResult);
            }
        });
    }

    private void handleFailure(Integer key, String value, Throwable throwable) {
        log.error("Error sending the message and the exception is {}", throwable.getMessage());
        try {
            throw throwable;
        } catch (Throwable ex) {
            log.error("Error in onFailure {}", throwable.getMessage());
        }
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message sent successfully with key {}, value {} to partition {}", key, value, result.getRecordMetadata().partition());
    }

    private ProducerRecord<Integer, String> buildProducerRecord(String topic, Integer key, String value) {
        List<Header> headers = Arrays.asList(new RecordHeader("event-source", "scanner".getBytes()));
        return new ProducerRecord<Integer, String>(topic, null, key, value, headers);
    }
}
