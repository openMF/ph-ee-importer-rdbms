package hu.dpc.phee.operator.streams;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.config.TransferTransformerConfig;
import hu.dpc.phee.operator.entity.batch.Batch;
import hu.dpc.phee.operator.entity.batch.BatchRepository;
import hu.dpc.phee.operator.entity.batch.Transaction;
import hu.dpc.phee.operator.entity.tenant.ThreadLocalContextUtil;
import hu.dpc.phee.operator.entity.transfer.Transfer;
import hu.dpc.phee.operator.entity.transfer.TransferRepository;
import hu.dpc.phee.operator.entity.transfer.TransferStatus;
import hu.dpc.phee.operator.file.CsvFileService;
import hu.dpc.phee.operator.file.FileTransferService;
import hu.dpc.phee.operator.util.BatchFormatToTransferMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static hu.dpc.phee.operator.util.OperatorUtils.strip;

@Component
public class InflightBatchManager {

    @Autowired
    BatchRepository batchRepository;

    @Autowired
    TransferTransformerConfig transferTransformerConfig;

    @Autowired
    private FileTransferService fileTransferService;

    @Autowired
    private CsvFileService csvFileService;

    @Value("${application.bucket-name}")
    private String bucketName;

    @Autowired
    TransferRepository transferRepository;

    private final Map<Long, String> workflowKeyBatchFileNameAssociations = new HashMap<>();

    private final Map<Long, String> workflowKeyBatchIdAssociations = new HashMap<>();

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

    public void checkWorkerIdAndUpdateTransferData(Batch batch, Long workflowInstanceKey, Long timestamp) {
        updateTransferTableForBatch(batch, workflowInstanceKey, timestamp);

    }

    private void updateTransferTableForBatch(Batch batch, Long workflowInstanceKey, Long completeTimestamp) {
        String filename = getBatchFileName(workflowInstanceKey);
        logger.info("Filename {}", filename);
        if (filename == null) {
            return;
        }
        filename = strip(filename);
        String localFilePath = fileTransferService.downloadFile(filename, bucketName);
        if (localFilePath == null) {
            logger.error("Null localFilePath, Error updating transfer table for batch with instance key {} and batch filename {}",
                    workflowInstanceKey, filename);
            return;
        }
        List<Transaction> transactionList = csvFileService.getTransactionList(filename);

        for (Transaction transaction : transactionList) {
            Transfer transfer = BatchFormatToTransferMapper.mapToTransferEntity(transaction);
            transfer.setWorkflowInstanceKey(workflowInstanceKey);

            String batchId = getBatchId(workflowInstanceKey);
            transfer.setBatchId(strip(batchId));
            transfer.setCompletedAt(new Date(completeTimestamp));
            transfer.setTransactionId(transaction.getRequestId());

            transfer.setPayeeDfspId(batch.getPaymentMode());
            transfer.setPayerDfspId(ThreadLocalContextUtil.getTenant().toString());

            transfer.setPayeeFeeCurrency(transaction.getCurrency());
            transfer.setPayeeFee(BigDecimal.ZERO);
            transfer.setPayerFeeCurrency(transaction.getCurrency());
            transfer.setPayerFee(BigDecimal.ZERO);

            BatchFormatToTransferMapper.updateTransferUsingBatchDetails(transfer, batch);
            transferRepository.save(transfer);
        }

    }

    public void updateTransferTableWithFailedTransaction(Long workflowInstanceKey, String filename) {
            logger.info("Filename {}", filename);
            if (filename == null) {
                return;
            }
            filename = strip(filename);
            String localFilePath = fileTransferService.downloadFile(filename, bucketName);
            List<Transaction> transactionList = csvFileService.getTransactionList(localFilePath);
            for (Transaction transaction : transactionList) {
                Transfer transfer = BatchFormatToTransferMapper.mapToTransferEntity(transaction);
                transfer.setStatus(TransferStatus.FAILED);
                transfer.setWorkflowInstanceKey(workflowInstanceKey);;
                transfer.setBatchId(strip(getBatchId(workflowInstanceKey)));
                transfer.setStartedAt(new Date());
                transfer.setCompletedAt(new Date());
                transfer.setErrorInformation(transaction.getNote());
                transfer.setClientCorrelationId(UUID.randomUUID().toString());
                transfer.setTransactionId(UUID.randomUUID().toString());
                logger.debug("Inserting failed txn: {}", transfer);
                logger.info("Inserting failed txn with note: {}", transaction.getNote());
                transferRepository.save(transfer);
            }
        }

    public String getBatchFileName(Long workflowKey) {
        synchronized (workflowKeyBatchFileNameAssociations) {
            return workflowKeyBatchFileNameAssociations.get(workflowKey);
        }
    }

    public void storeBatchId(Long workflowKey, String batchId) {
        synchronized (workflowKeyBatchFileNameAssociations) {
            workflowKeyBatchIdAssociations.put(workflowKey, batchId);
        }
    }

    public String getBatchId(Long workflowKey) {
        synchronized (workflowKeyBatchFileNameAssociations) {
            return workflowKeyBatchIdAssociations.get(workflowKey);
        }
    }

    public void storeBatchFileName(Long workflowKey, String filename) {
        synchronized (workflowKeyBatchFileNameAssociations) {
            workflowKeyBatchFileNameAssociations.put(workflowKey, filename);
        }
    }
}
