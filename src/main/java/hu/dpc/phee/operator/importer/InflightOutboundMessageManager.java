package hu.dpc.phee.operator.importer;

import hu.dpc.phee.operator.entity.outboundmessages.OutboudMessages;
import hu.dpc.phee.operator.entity.outboundmessages.OutboundMessagesRepository;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class InflightOutboundMessageManager {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final Map<Long, OutboudMessages> inflightOutboundMessage = new HashMap<>();

    @Autowired
    private OutboundMessagesRepository outboundMessagesRepository;

    public void outboundMessageStarted(Long workflowInstanceKey, Long timestamp, String direction) {
        OutboudMessages outboudMessages = getOrCreateOutboundMessage(workflowInstanceKey);
        if (outboudMessages.getSubmittedOnDate() == null) {
            outboudMessages.setSubmittedOnDate(new Date(timestamp));
            outboundMessagesRepository.save(outboudMessages);
        } else {
            logger.debug("outboudMessages {} already started at {}", workflowInstanceKey, outboudMessages.getSubmittedOnDate());
        }
    }

    public void outboundMessageEnded(Long workflowInstanceKey, Long timestamp) {
        synchronized (inflightOutboundMessage) {
            OutboudMessages outboudMessages = inflightOutboundMessage.remove(workflowInstanceKey);
            if (outboudMessages == null) {
                logger.error("failed to remove in-flight outbound message {}", workflowInstanceKey);
                outboudMessages = outboundMessagesRepository.findByInternalId(workflowInstanceKey).orElse(null);
                if (outboudMessages == null || outboudMessages.getDeliveredOnDate() != null) {
                    logger.error("completed event arrived for non existent outbound messages {} or it was already finished!",
                            workflowInstanceKey);
                    return;
                }
            }
            if (outboudMessages.getDeliveryStatus() == 300) {
                Timestamp localTime = new Timestamp(new Date().getTime());
                outboudMessages.setDeliveredOnDate(localTime);
            }
            outboundMessagesRepository.save(outboudMessages);
            logger.debug("outbound messages finished {}", outboudMessages.getInternalId());
        }
    }

    public OutboudMessages getOrCreateOutboundMessage(Long workflowInstanceKey) {
        synchronized (inflightOutboundMessage) {
            OutboudMessages outboudMessages = inflightOutboundMessage.get(workflowInstanceKey);
            if (outboudMessages == null) {
                outboudMessages = outboundMessagesRepository.findByInternalId(workflowInstanceKey).orElse(null);
                if (outboudMessages == null) {
                    outboudMessages = new OutboudMessages(workflowInstanceKey); // Sets status to ONGOING
                }
                inflightOutboundMessage.put(workflowInstanceKey, outboudMessages);
            }
            return outboudMessages;
        }
    }
}
