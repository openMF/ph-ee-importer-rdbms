package hu.dpc.phee.operator.util;

import hu.dpc.phee.operator.entity.batch.Transaction;
import hu.dpc.phee.operator.entity.transfer.Transfer;
import hu.dpc.phee.operator.entity.transfer.TransferStatus;

import java.math.BigDecimal;

public class BatchFormatToTransferMapper {

    public static Transfer mapToTransferEntity(Transaction transaction) {
        Transfer transfer = new Transfer();

        transfer.setAmount(BigDecimal.valueOf(Long.parseLong(transaction.getAmount())));
        transfer.setPayeePartyIdType("accountnumber");
        transfer.setPayeePartyId(transaction.getAccount_number());
        transfer.setDirection("UNKNOWN");
        transfer.setCurrency(transaction.getCurrency());
        TransferStatus status;
        String st = transaction.getStatus().toUpperCase();
        if (st.contains("SUCCESS") || st.contains("COMPLETED")) {
            status = TransferStatus.COMPLETED;
        } else if (st.equals("FAILED") || st.equals("ERROR")) {
            status = TransferStatus.FAILED;
            transfer.setErrorInformation(transaction.getStatus());
        } else {
            status = TransferStatus.UNKNOWN;
        }
        transfer.setStatus(status);

        return transfer;
    }

}
