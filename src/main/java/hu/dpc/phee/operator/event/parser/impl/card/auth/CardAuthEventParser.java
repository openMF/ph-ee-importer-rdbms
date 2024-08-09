package hu.dpc.phee.operator.event.parser.impl.card.auth;

import com.baasflow.commons.events.Event;
import com.baasflow.commons.events.EventType;
import hu.dpc.phee.operator.entity.card.CardTransactionRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

@Component
@Slf4j
public class CardAuthEventParser {

    @Autowired
    private CardTransactionRepository cardTransactionRepository;

    @Autowired
    private TransactionTemplate transactionTemplate;

    public boolean isAbleToProcess(Event event) {
        return "dipocket-connector".equals(event.getSourceModule()) &&
                "transaction-feed-request".equals(event.getEvent())
                && EventType.audit.equals(event.getEventType());
    }

    public void process(Event event) {
        log.debug("processing card auth event record");
        log.trace("event: {}", event);
    }
}