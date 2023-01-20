package hu.dpc.phee.operator.util;

import hu.dpc.phee.operator.entity.batch.Batch;
import hu.dpc.phee.operator.entity.batch.Transaction;
import hu.dpc.phee.operator.entity.transfer.Transfer;
import hu.dpc.phee.operator.entity.transfer.TransferStatus;

import java.math.BigDecimal;
import java.util.Objects;

public class BatchFormatToTransferMapper {

    public static Transfer mapToTransferEntity(Transaction transaction) {
        Transfer transfer = new Transfer();

        transfer.setAmount(BigDecimal.valueOf(Long.parseLong(transaction.getAmount())));
        // below condition check will make sure backward compatibility with old batch format
        if (transaction.getAccountNumber() != null && !transaction.getAccountNumber().equalsIgnoreCase("")) {
            transfer.setPayeePartyId(transaction.getAccountNumber());
            transfer.setPayeePartyIdType("accountnumber");
        } else {
            transfer.setPayeePartyId(transaction.getPayeeIdentifier());
            transfer.setPayeePartyIdType(transaction.getPayeeIdentifierType());
        }
        // below 2 fields will work only for new CSV specs
        transfer.setPayerPartyIdType(transaction.getPayerIdentifierType());
        transfer.setPayerPartyId(transaction.getPayerIdentifier());
        transfer.setDirection("UNKNOWN");
        transfer.setCurrency(transaction.getCurrency());
        TransferStatus status;
        if (transaction.getStatus() != null) {
            String st = transaction.getStatus().toUpperCase();
            if (st.contains("SUCCESS") || st.contains("COMPLETED")) {
                status = TransferStatus.COMPLETED;
            } else if (st.equals("FAILED") || st.equals("ERROR")) {
                status = TransferStatus.FAILED;
                transfer.setErrorInformation(transaction.getStatus());
            } else {
                status = TransferStatus.UNKNOWN;
            }
        } else {
            status = TransferStatus.UNKNOWN;
        }
        transfer.setStatus(status);

        return transfer;
    }

    public static Transfer updateTransferUsingBatchDetails(Transfer transfer, Batch batch) {
        if (batch == null) {
            return transfer;
        }

        if (transfer.getStatus() == TransferStatus.UNKNOWN) {
            if (batch.getCompletedAt() != null) {
                transfer.setStatus(TransferStatus.COMPLETED);
            } else {
                if (batch.getTotalTransactions() != null && batch.getCompleted() != null) {
                    if (Objects.equals(batch.getTotalTransactions(), batch.getCompleted())) {
                        transfer.setStatus(TransferStatus.COMPLETED);
                    }
                }
                if (batch.getCompleted() != null &&
                        batch.getTotalTransactions() != null &&
                        batch.getCompleted().longValue() == batch.getTotalTransactions().longValue()) {
                    transfer.setStatus(TransferStatus.COMPLETED);
                } else if (batch.getOngoing() != null &&
                        batch.getOngoing() != 0 && batch.getCompletedAt() == null) {
                    transfer.setStatus(TransferStatus.IN_PROGRESS);
                } else if (batch.getFailed() != null && batch.getFailed() != null &&
                        batch.getFailed().longValue() == batch.getFailed().longValue()) {
                    transfer.setStatus(TransferStatus.FAILED);
                } else {
                    transfer.setStatus(TransferStatus.IN_PROGRESS);
                }
            }
        }

        if (batch.getStartedAt() != null) {
            transfer.setStartedAt(batch.getStartedAt());
        }

        return transfer;
    }

}
