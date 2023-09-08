package hu.dpc.phee.operator.importer;

import hu.dpc.phee.operator.entity.batch.Batch;
import hu.dpc.phee.operator.entity.batch.BatchRepository;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class InflightBatchManager {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final Map<Long, Batch> inflightBatches = new HashMap<>();

    @Autowired
    private BatchRepository batchRepository;

    public void batchStarted(Long workflowInstanceKey, Long timestamp, String direction) {
        Batch batch = getOrCreateBatch(workflowInstanceKey);
        if (batch.getStartedAt() == null) {
            batch.setStartedAt(new Date(timestamp));
            batchRepository.save(batch);
            logger.debug("saving batch {}", batch.getWorkflowInstanceKey());
        } else {
            logger.debug("batch {} already started at {}", workflowInstanceKey, batch.getStartedAt());
        }
    }

    public void batchEnded(Long workflowInstanceKey, Long timestamp) {
        synchronized (inflightBatches) {
            Batch batch = inflightBatches.remove(workflowInstanceKey);
            if (batch == null) {
                logger.error("failed to remove in-flight batch {}", workflowInstanceKey);
                batch = batchRepository.findByWorkflowInstanceKey(workflowInstanceKey);
                if (batch == null || batch.getCompletedAt() != null) {
                    logger.error("completed event arrived for non existent batch {} or it was already finished!", workflowInstanceKey);
                    return;
                }
            }

            batch.setCompletedAt(new Date(timestamp));
            batchRepository.save(batch);
            logger.debug("batch {} finished", batch.getWorkflowInstanceKey());
        }
    }

    public Batch getOrCreateBatch(Long workflowInstanceKey) {
        synchronized (inflightBatches) {
            Batch batch = inflightBatches.get(workflowInstanceKey);
            if (batch == null) {
                batch = batchRepository.findByWorkflowInstanceKey(workflowInstanceKey);
                if (batch == null) {
                    batch = new Batch(workflowInstanceKey);
                    logger.debug("started in-flight batch {}", batch.getWorkflowInstanceKey());
                }
                inflightBatches.put(workflowInstanceKey, batch);
            }
            return batch;
        }
    }
}
