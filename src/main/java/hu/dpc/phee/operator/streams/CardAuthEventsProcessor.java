package hu.dpc.phee.operator.streams;

import com.baasflow.commons.events.Event;
import hu.dpc.phee.operator.event.parser.impl.card.auth.CardAuthEventParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class CardAuthEventsProcessor {

    @Autowired
    private CardAuthEventParser cardAuthEventParser;

    @KafkaListener(topics = "${baasflow.events.channels.technical.topic}", containerFactory = "cardAuthKafkaListenerContainerFactory")
    public void processEvent(ConsumerRecord<String, Event> record) {
        String key = record.key();
        Event event = record.value();
        try {
            if (cardAuthEventParser.isAbleToProcess(event)) {
                cardAuthEventParser.process(event);
            }
        } catch (Exception e) {
            log.trace("error {}\n{}", e.getMessage(), event.toString());
            log.error("could not process record for key {}", key, e);
        }
    }
}