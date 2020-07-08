package com.kafka.libraryeventsproducer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.libraryeventsproducer.domain.LibraryEvent;
import com.kafka.libraryeventsproducer.repositories.LibraryEventsRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventsService {

    private final ObjectMapper objectMapper;
    private final LibraryEventsRepository libraryEventRepository;

    @Autowired
    public LibraryEventsService(final ObjectMapper objectMapper, final LibraryEventsRepository libraryEventRepository) {
        this.objectMapper = objectMapper;
        this.libraryEventRepository = libraryEventRepository;
    }

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = this.objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        switch (libraryEvent.getLibraryEventType()) {
            case NEW:
                this.saveLibraryEvent(libraryEvent);
                break;
            case UPDATE:
                this.validateLibraryEvent(libraryEvent);
                this.saveLibraryEvent(libraryEvent);
                break;
            default:
                break;

        }
    }

    private void validateLibraryEvent(LibraryEvent libraryEvent) {
        if(libraryEvent.getLibraryEventId() == null) {
            throw new RuntimeException("EventId can't be null");
        }
        Optional<LibraryEvent> libraryEventOptional = libraryEventRepository.findById(libraryEvent.getLibraryEventId());
        if(!libraryEventOptional.isPresent()) {
            throw new IllegalArgumentException("Invalid library Event");
        }
        log.info("Successfully validated");
    }

    private void saveLibraryEvent(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        this.libraryEventRepository.save(libraryEvent);
        log.info("Successfully save libraryEvent object {}", libraryEvent);
    }
}
