package hu.dpc.phee.operator.streams;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.config.TransferTransformerConfig;
import hu.dpc.phee.operator.entity.outboundmessages.OutboudMessages;
import hu.dpc.phee.operator.entity.outboundmessages.OutboundMessagesRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class InflightOutboundMessageManager {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private OutboundMessagesRepository outboundMessagesRepository;

    @Autowired
    TransferTransformerConfig transferTransformerConfig;

    public OutboudMessages retrieveOrCreateOutboundMessage(String bpmn, DocumentContext record) {
        Long processInstanceKey = record.read("$.value.processInstanceKey", Long.class);
        Optional<TransferTransformerConfig.Flow> config = transferTransformerConfig.findFlow(bpmn);
        OutboudMessages outboudMessages = outboundMessagesRepository.findByInternalId(processInstanceKey);

        if (outboudMessages != null) {
            logger.debug("creating new OutboudMessages for processInstanceKey: {}", processInstanceKey);
            OutboudMessages newOutboudMessages = new OutboudMessages(processInstanceKey);
            outboudMessages = outboundMessagesRepository.save(newOutboudMessages);
        } else {
            logger.info("found existing OutboudMessages for processInstanceKey: {}", processInstanceKey);
        }

        return outboudMessages;
    }
}
