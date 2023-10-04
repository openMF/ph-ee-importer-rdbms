package hu.dpc.phee.operator.streams;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.config.TransferTransformerConfig;
import hu.dpc.phee.operator.entity.outboundmessages.OutboudMessages;
import hu.dpc.phee.operator.entity.outboundmessages.OutboundMessagesRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Optional;

public class InflightOutboundMessageManager {

    @Autowired
    OutboundMessagesRepository outboundMessagesRepository;

    @Autowired
    TransferTransformerConfig transferTransformerConfig;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public OutboudMessages retrieveOrCreateOutboundMessage(String bpmn, DocumentContext record) {
        Long processInstanceKey = record.read("$.value.processInstanceKey", Long.class);
        Optional<TransferTransformerConfig.Flow> config = transferTransformerConfig.findFlow(bpmn);
        // This code block should also process transaction/batch/outboundMsg Type
        Optional<OutboudMessages> outboudMessages = outboundMessagesRepository.findByInternalId(processInstanceKey);
        if (outboudMessages == null) {
            logger.debug("creating new Batch for processInstanceKey: {}", processInstanceKey);
            outboudMessages = Optional.of(new OutboudMessages(processInstanceKey));
            if (config.isPresent()) {
//                outboudMessages.setDirection(config.get().getDirection());
            } else {
                logger.error("No config found for bpmn: {}", bpmn);
            }
            outboundMessagesRepository.save(outboudMessages);
        } else {
            logger.info("found existing Batch for processInstanceKey: {}", processInstanceKey);
        }
        return outboudMessages;
    }
}
