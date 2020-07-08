package com.kafka.libraryeventsproducer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafka.libraryeventsproducer.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventsConsumer {

    private final LibraryEventsService libraryEventsService;

    @Autowired
    public LibraryEventsConsumer(final LibraryEventsService libraryEventsService) {
        this.libraryEventsService = libraryEventsService;
    }

    @KafkaListener(topics = {"library-events"})
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("Consumer Record {}", consumerRecord);
        this.libraryEventsService.processLibraryEvent(consumerRecord);
    }
}
