package com.kafka.libraryeventsproducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafka.libraryeventsproducer.domain.LibraryEvent;
import com.kafka.libraryeventsproducer.domain.LibraryEventType;
import com.kafka.libraryeventsproducer.producer.LibraryEventProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
public class LibraryEventsController {

    private final LibraryEventProducer producer;

    @Autowired
    public LibraryEventsController(LibraryEventProducer producer) {
        this.producer = producer;
    }

//    @PostMapping("/v1/libraryevent")
//    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
//        this.producer.sendLibraryEvent(libraryEvent);
//        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
//    }
//
//    @PostMapping("/v1/libraryeventsync")
//    public ResponseEntity<LibraryEvent> postLibraryEventSync(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {
//        this.producer.sendLibraryEventSync(libraryEvent);
//        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
//    }

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEventUsingProducerRecord(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {
        libraryEvent.setLibraryEventType(LibraryEventType.NEW);
        this.producer.sendLibraryEventUsingProducerRecord(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("/v1/libraryevent")
    public ResponseEntity<?> updateLibraryEventUsingProducerRecord(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {
        if(libraryEvent.getLibraryEventId() == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Pass libraryEventId");
        }
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        this.producer.sendLibraryEventUsingProducerRecord(libraryEvent);
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }

}
