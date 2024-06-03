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
import hu.dpc.phee.operator.util.PaymentModeEnum;
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
    TransferRepository transferRepository;
    @Autowired
    TransferTransformerConfig transferTransformerConfig;

    @Autowired
    private FileTransferService fileTransferService;

    @Autowired
    private CsvFileService csvFileService;

    @Value("${application.bucket-name}")
    private String bucketName;

    private final Map<Long, String> workflowKeyBatchFileNameAssociations = new HashMap<>();

    private final Map<Long, String> workflowKeyBatchIdAssociations = new HashMap<>();

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public Batch retrieveOrCreateBatch(String bpmn, DocumentContext record) {
        Long processInstanceKey = record.read("$.value.processInstanceKey", Long.class);
        Optional<TransferTransformerConfig.Flow> config = transferTransformerConfig.findFlow(bpmn);

        Optional<Batch> batchOptional = batchRepository.findByWorkflowInstanceKey(processInstanceKey);
        if (batchOptional.isEmpty()) {
            logger.debug("Creating new Batch for processInstanceKey: {}", processInstanceKey);
            String batchId = getBatchId(processInstanceKey);

            if (batchId != null && batchRepository.findByBatchIdAndSubBatchIdIsNull(batchId).isEmpty()) {
                Batch batch = new Batch(processInstanceKey);
                batchRepository.save(batch);
                return batch;
            }
            else if (batchId != null) {
                Batch batch = batchRepository.findByBatchIdAndSubBatchIdIsNull(batchId).orElse(null);
                assert batch != null;
                return batch;
            }
            else{
                Batch batch = new Batch(processInstanceKey);
                batchRepository.save(batch);
                return batch;
            }
        } else {
            logger.info("Found existing Batch for processInstanceKey: {}", processInstanceKey);
        }

        return batchOptional.orElse(null);
    }


    public void checkWorkerIdAndUpdateTransferData(Batch batch, Long workflowInstanceKey, Long timestamp) {
        updateTransferTableForBatch(batch, workflowInstanceKey, timestamp);

    }

    private void  updateTransferTableForBatch(Batch batch, Long workflowInstanceKey, Long completeTimestamp) {
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
            transfer.setCompletedAt(new Date(completeTimestamp));
            transfer.setTransactionId(transaction.getRequestId());
            transfer.setClientCorrelationId(UUID.randomUUID().toString());
            transfer.setPayeeDfspId(batch.getPaymentMode());
            transfer.setPayerDfspId(ThreadLocalContextUtil.getTenantName());
            String batchId = getBatchId(workflowInstanceKey);
            if(transaction.getBatchId() == null || transaction.getBatchId().isEmpty())
            {
                transfer.setBatchId(batchId);
            }else {
                transfer.setBatchId(transaction.getBatchId());
            }
            transfer.setPayeeFeeCurrency(transaction.getCurrency());
            transfer.setPayeeFee(BigDecimal.ZERO);
            transfer.setPayerFeeCurrency(transaction.getCurrency());
            transfer.setPayerFee(BigDecimal.ZERO);
            if(transaction.getPaymentMode().equals(PaymentModeEnum.CLOSED_LOOP.getValue())) {

                BatchFormatToTransferMapper.updateTransferUsingBatchDetails(transfer, batch);
                transfer = updatedExistingRecord(transfer, batchId);
                transferRepository.save(transfer);
            }
            logger.debug("Saved transfer with batchId: {}", transfer.getBatchId());
        }

    }

    public Transfer updatedExistingRecord(Transfer transfer, String batchId) {
        // Attempt to find an existing transfer with the provided batchId
        Optional<Transfer> existingTransferOpt = transferRepository.findByTransactionIdAndBatchId(transfer.getTransactionId(), batchId);

        // If not found, attempt to find with the transfer's own batchId
        Transfer existingTransfer = existingTransferOpt.orElseGet(() ->
                transferRepository.findByTransactionIdAndBatchId(transfer.getTransactionId(), transfer.getBatchId())
                        .orElse(null));

        // If still not found, return the original transfer
        if (existingTransfer == null) {
            return transfer;
        }

        // If found, update the existing transfer with the provided transfer's details and return
        return updateTransfer(existingTransfer, transfer);
    }

    public Transfer updateTransfer(Transfer transfer1, Transfer transfer2) {
        transfer1.setWorkflowInstanceKey(transfer2.getWorkflowInstanceKey());
        transfer1.setTransactionId(transfer2.getTransactionId());
        transfer1.setStartedAt(transfer2.getStartedAt());
        transfer1.setCompletedAt(transfer2.getCompletedAt());
        transfer1.setStatus(transfer2.getStatus());
        transfer1.setStatusDetail(transfer2.getStatusDetail());
        transfer1.setPayeeDfspId(transfer2.getPayeeDfspId());
        transfer1.setPayeePartyId(transfer2.getPayeePartyId());
        transfer1.setPayeePartyIdType(transfer2.getPayeePartyIdType());
        transfer1.setPayeeFee(transfer2.getPayeeFee());
        transfer1.setPayeeFeeCurrency(transfer2.getPayeeFeeCurrency());
        transfer1.setPayeeQuoteCode(transfer2.getPayeeQuoteCode());
        transfer1.setPayerDfspId(transfer2.getPayerDfspId());
        transfer1.setPayerPartyId(transfer2.getPayerPartyId());
        transfer1.setPayerPartyIdType(transfer2.getPayerPartyIdType());
        transfer1.setPayerFee(transfer2.getPayerFee());
        transfer1.setPayerFeeCurrency(transfer2.getPayerFeeCurrency());
        transfer1.setPayerQuoteCode(transfer2.getPayerQuoteCode());
        transfer1.setAmount(transfer2.getAmount());
        transfer1.setCurrency(transfer2.getCurrency());
        transfer1.setDirection(transfer2.getDirection());
        transfer1.setErrorInformation(transfer2.getErrorInformation());
        transfer1.setBatchId(transfer2.getBatchId());
        transfer1.setClientCorrelationId(transfer2.getClientCorrelationId());
        return transfer1;
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
                String batchId = getBatchId(workflowInstanceKey);
                if(transaction.getBatchId() == null || transaction.getBatchId().isEmpty())
                {
                    transfer.setBatchId(batchId);
                }else {
                    transfer.setBatchId(transaction.getBatchId());
                }
                transfer.setStartedAt(new Date());
                transfer.setCompletedAt(new Date());
                transfer.setErrorInformation(transaction.getNote());
                transfer.setClientCorrelationId(UUID.randomUUID().toString());
                transfer.setTransactionId(transaction.getRequestId());
                logger.debug("Inserting failed txn: {}", transfer);
                logger.info("Inserting failed txn with note: {}", transaction.getNote());
                transfer = updatedExistingRecord(transfer, batchId);
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
