package hu.dpc.phee.operator.streams;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.config.TransferTransformerConfig;
import hu.dpc.phee.operator.entity.batch.Batch;
import hu.dpc.phee.operator.entity.batch.BatchRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Optional;

public class InflightBatchManager {

    @Autowired
    BatchRepository batchRepository;

    @Autowired
    TransferTransformerConfig transferTransformerConfig;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public Batch retrieveOrCreateBatch(String bpmn, DocumentContext record) {
        Long processInstanceKey = record.read("$.value.processInstanceKey", Long.class);
        Optional<TransferTransformerConfig.Flow> config = transferTransformerConfig.findFlow(bpmn);
        Batch batch = batchRepository.findByWorkflowInstanceKey(processInstanceKey);
        if (batch == null) {
            logger.debug("creating new Batch for processInstanceKey: {}", processInstanceKey);
            batch = new Batch(processInstanceKey);
            batchRepository.save(batch);
        } else {
            logger.info("found existing Batch for processInstanceKey: {}", processInstanceKey);
        }
        return batch;
    }
}
