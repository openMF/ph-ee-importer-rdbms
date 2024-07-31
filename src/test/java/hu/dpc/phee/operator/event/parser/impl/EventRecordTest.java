package hu.dpc.phee.operator.event.parser.impl;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EventRecordTest {

    @Test
    void create() {
        List<String> jsonEvents = new ArrayList<>();
        jsonEvents.add(loadFile("incident/1.json"));
        jsonEvents.add(loadFile("job/1.json"));
        jsonEvents.add(loadFile("process_instance/1.json"));
        jsonEvents.add(loadFile("process_instance/2.json"));
        jsonEvents.add(loadFile("process_instance/3.json"));
        jsonEvents.add(loadFile("process_instance/process_activated.json"));
        jsonEvents.add(loadFile("process_instance/process_completed.json"));
        jsonEvents.add(loadFile("variable/1.json"));
        jsonEvents.add(loadFile("ignore/1.json"));

        List<EventRecord> records = EventRecord.listBuilder().jsonEvents(jsonEvents).build();

        assertEquals(8, records.size());
    }

    private String loadFile(String filename) {
        try {
            URI path = Objects.requireNonNull(getClass().getClassLoader().getResource("event-samples/" + filename)).toURI();
            return Files.readString(Path.of(path));
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}