package hu.dpc.phee.operator.streams;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.config.TransferTransformerConfig;
import hu.dpc.phee.operator.entity.batch.Batch;
import hu.dpc.phee.operator.entity.batch.BatchRepository;
import hu.dpc.phee.operator.entity.batch.Transaction;
import hu.dpc.phee.operator.entity.tenant.ThreadLocalContextUtil;
import hu.dpc.phee.operator.entity.transfer.Transfer;
import hu.dpc.phee.operator.entity.transfer.TransferRepository;
import hu.dpc.phee.operator.file.FileTransferService;
import hu.dpc.phee.operator.util.BatchFormatToTransferMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
    private CsvMapper csvMapper;

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
        List<Transaction> transactionList;
        try {
            CsvSchema schema = CsvSchema.emptySchema().withHeader();
            Reader reader = Files.newBufferedReader(Paths.get(filename), Charset.defaultCharset());
            MappingIterator<Transaction> readValues = csvMapper.readerWithSchemaFor(Transaction.class).with(schema).readValues(reader);
            transactionList = new ArrayList<>();
            while (readValues.hasNext()) {
                Transaction current = readValues.next();
                transactionList.add(current);
            }
        } catch (IOException e) {
            logger.debug(e.getMessage());
            logger.error("Error building TransactionList for batch with instance key {} and batch filename {}", workflowInstanceKey,
                    filename);
            return;
        }

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
