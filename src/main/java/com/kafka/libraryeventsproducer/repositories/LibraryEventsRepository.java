package com.kafka.libraryeventsproducer.repositories;

import com.kafka.libraryeventsproducer.domain.LibraryEvent;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface LibraryEventsRepository extends CrudRepository<LibraryEvent, Integer> {
}
