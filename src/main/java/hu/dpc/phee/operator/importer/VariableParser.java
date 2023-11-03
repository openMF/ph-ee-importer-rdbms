package hu.dpc.phee.operator.importer;

import static hu.dpc.phee.operator.OperatorUtils.strip;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.OperatorUtils;
import hu.dpc.phee.operator.entity.batch.Batch;
import hu.dpc.phee.operator.entity.batch.BatchRepository;
import hu.dpc.phee.operator.entity.outboundmessages.OutboudMessages;
import hu.dpc.phee.operator.entity.transactionrequest.TransactionRequest;
import hu.dpc.phee.operator.entity.transactionrequest.TransactionRequestState;
import hu.dpc.phee.operator.entity.transfer.Transfer;
import hu.dpc.phee.operator.entity.transfer.TransferStatus;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Component;

@Component
public class VariableParser {

    @Value("${bpmn.incoming-direction}")
    private String incomingDirection;

    @Value("${bpmn.outgoing-direction}")
    private String outgoingDirection;
    @Autowired
    private BatchRepository batchRepository;
    private final Logger logger = LoggerFactory.getLogger(VariableParser.class);
    private final Map<String, Consumer<Pair<Transfer, String>>> transferParsers = new HashMap<>();
    private final Map<String, Consumer<Pair<TransactionRequest, String>>> transactionRequestParsers = new HashMap<>();
    private final Map<String, Consumer<Pair<Batch, String>>> batchParsers = new HashMap<>();

    private final Map<String, Consumer<Pair<OutboudMessages, String>>> outboundMessageParsers = new HashMap<>();

    public VariableParser() {
        transferParsers.put("localQuoteResponse", pair -> parseTransferLocalQuoteResponse(pair.getFirst(), strip(pair.getSecond())));
        transferParsers.put("quoteSwitchRequest", pair -> parseQuoteSwitchRequest(pair.getFirst(), strip(pair.getSecond())));
        transferParsers.put("payeeQuoteResponse", pair -> parsePayeeQuoteResponse(pair.getFirst(), pair.getSecond()));
        transferParsers.put("quoteId", pair -> pair.getFirst().setPayeeQuoteCode(strip(pair.getSecond())));
        transferParsers.put("transferResponse-PREPARE", pair -> parseTransferResponsePrepare(pair.getFirst(), pair.getSecond()));
        transferParsers.put("transferResponse-CREATE", pair -> parseTransferResponse(pair.getFirst(), strip(pair.getSecond())));
        transferParsers.put("transferCreateFailed", pair -> parseTransferCreateFailed(pair.getFirst(), pair.getSecond()));
        transferParsers.put("transactionId", pair -> pair.getFirst().setTransactionId(strip(pair.getSecond())));
        transferParsers.put("partyLookupFspId", pair -> pair.getFirst().setPayeeDfspId(strip(pair.getSecond())));
        transferParsers.put("initiatorFspId", pair -> pair.getFirst().setPayerDfspId(strip(pair.getSecond())));
        transferParsers.put("channelRequest", pair -> parseChannelRequest(pair.getFirst(), pair.getSecond()));
        transferParsers.put("errorInformation", pair -> {
            parseErrorInformation(pair.getFirst(), pair.getSecond());
            parseTransferCreateFailed(pair.getFirst(), pair.getSecond());
        });
        transferParsers.put("batchId", pair -> pair.getFirst().setBatchId(strip(pair.getSecond())));
        transferParsers.put("subBatchId", pair -> pair.getFirst().setSubBatchId(strip(pair.getSecond())));
        transferParsers.put("clientCorrelationId", pair -> parseClientCorrelationIdTransfers(pair.getFirst(), pair.getSecond()));

        transactionRequestParsers.put("authType", pair -> pair.getFirst().setAuthType(strip(pair.getSecond())));
        transactionRequestParsers.put("transactionId", pair -> pair.getFirst().setTransactionId(strip(pair.getSecond())));
        transactionRequestParsers.put("partyLookupFspId", pair -> pair.getFirst().setPayerDfspId(strip(pair.getSecond())));
        transactionRequestParsers.put("initiatorFspId", pair -> parseInitiatorFspId(pair.getFirst(), pair.getSecond()));
        transactionRequestParsers.put("channelRequest", pair -> parseTransactionChannelRequest(pair.getFirst(), pair.getSecond()));
        transactionRequestParsers.put("transactionRequestResponse",
                pair -> parseTransactionRequestResponse(pair.getFirst(), pair.getSecond()));
        transactionRequestParsers.put("transactionRequestFailed", pair -> parseTransactionRequestFailed(pair.getFirst(), pair.getSecond()));
        transactionRequestParsers.put("transactionRequest", pair -> parseTransactionRequest(pair.getFirst(), pair.getSecond()));
        transactionRequestParsers.put("localQuoteResponse",
                pair -> parseTransactionRequestLocalQuoteResponse(pair.getFirst(), pair.getSecond()));
        transactionRequestParsers.put("payeeQuoteResponse",
                pair -> parseTransactionRequestPayeeQuoteResponse(pair.getFirst(), pair.getSecond()));
        transactionRequestParsers.put("quoteId", pair -> pair.getFirst().setPayeeQuoteCode(strip(pair.getSecond())));
        transactionRequestParsers.put("transactionState", pair -> parseTransActionState(pair.getFirst(), pair.getSecond()));
        transactionRequestParsers.put("mpesaChannelRequest", pair -> parseTransactionMpesaRequest(pair.getFirst(), pair.getSecond()));
        transactionRequestParsers.put("externalId", pair -> parseExternalId(pair.getFirst(), pair.getSecond()));
        transactionRequestParsers.put("partyLookupFailed", pair -> parsePartyLookUpState(pair.getFirst(), pair.getSecond()));
        transactionRequestParsers.put("transactionFailed", pair -> parseTransactionFailed(pair.getFirst(), pair.getSecond()));
        transactionRequestParsers.put("transferSettlementFailed", pair -> parseSettlementFiled(pair.getFirst(), pair.getSecond()));
        transactionRequestParsers.put("clientCorrelationId", pair -> parseClientCorrelationId(pair.getFirst(), pair.getSecond()));
        transactionRequestParsers.put("errorInformation", pair -> parseErrorInformation(pair.getFirst(), pair.getSecond()));

        batchParsers.put("batchId", pair -> pair.getFirst().setBatchId(strip(pair.getSecond())));
        batchParsers.put("filename", pair -> pair.getFirst().setRequestFile(strip(pair.getSecond())));
        batchParsers.put("requestId", pair -> pair.getFirst().setRequestId(strip(pair.getSecond())));
        batchParsers.put("note", pair -> pair.getFirst().setNote(strip(pair.getSecond())));
        batchParsers.put("subBatchId", pair -> pair.getFirst().setSubBatchId(strip(pair.getSecond())));
        batchParsers.put("totalTransactions", pair -> pair.getFirst().setTotalTransactions(Long.parseLong(pair.getSecond())));
        batchParsers.put("ongoingTransactions", pair -> pair.getFirst().setOngoing(Long.parseLong(pair.getSecond())));
        batchParsers.put("failedTransactions", pair -> pair.getFirst().setFailed(Long.parseLong(pair.getSecond())));
        batchParsers.put("completedTransactions", pair -> pair.getFirst().setCompleted(Long.parseLong(pair.getSecond())));
        batchParsers.put("totalAmount", pair -> pair.getFirst().setTotalAmount(Long.parseLong(pair.getSecond())));
        batchParsers.put("ongoingAmount", pair -> pair.getFirst().setOngoingAmount(Long.parseLong(pair.getSecond())));
        batchParsers.put("failedAmount", pair -> pair.getFirst().setFailedAmount(Long.parseLong(pair.getSecond())));
        batchParsers.put("completedAmount", pair -> pair.getFirst().setCompletedAmount(Long.parseLong(pair.getSecond())));
        batchParsers.put("resultFile", pair -> pair.getFirst().setResultFile(strip(pair.getSecond())));
        batchParsers.put("paymentMode", pair -> pair.getFirst().setPaymentMode(strip(pair.getSecond())));
        batchParsers.put("registeringInstituteId", pair -> pair.getFirst().setRegisteringInstitutionId(pair.getSecond()));
        batchParsers.put("payerIdentifier", pair -> pair.getFirst().setPayerFsp(pair.getSecond()));
        batchParsers.put("clientCorrelationId", pair -> pair.getFirst().setCorrelationId(pair.getSecond()));
        batchParsers.put("subBatchDetails", pair -> {
            try {
                parseSubBatchDetails(pair.getSecond());
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });

        outboundMessageParsers.put("tenantId", pair -> pair.getFirst().setTenantId(Long.parseLong(pair.getSecond())));
        outboundMessageParsers.put("externalId", pair -> pair.getFirst().setExternalId(strip(pair.getSecond())));
        outboundMessageParsers.put("internalId", pair -> pair.getFirst().setInternalId(Long.parseLong(pair.getSecond())));
        outboundMessageParsers.put("deliveryStatus", pair -> pair.getFirst().setDeliveryStatus(Integer.parseInt(pair.getSecond())));
        outboundMessageParsers.put("deliveryErrorMessage", pair -> pair.getFirst().setDeliveryErrorMessage(strip(pair.getSecond())));
        outboundMessageParsers.put("sourceAddress", pair -> pair.getFirst().setSourceAddress(strip(pair.getSecond())));
        outboundMessageParsers.put("phoneNumber", pair -> pair.getFirst().setMobileNumber(strip(pair.getSecond())));
        outboundMessageParsers.put("deliveryMessage", pair -> pair.getFirst().setMessage(strip(pair.getSecond())));
        outboundMessageParsers.put("bridgeId", pair -> pair.getFirst().setBridgeId(Long.parseLong(pair.getSecond())));
    }

    public void parseSubBatchDetails(String jsonString) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        List<Batch> batches = Arrays.asList(objectMapper.readValue(jsonString, Batch[].class));
        for (Batch bt : batches) {
            batchRepository.save(bt);
        }
    }

    public Map<String, Consumer<Pair<Transfer, String>>> getTransferParsers() {
        return transferParsers;
    }

    public Map<String, Consumer<Pair<TransactionRequest, String>>> getTransactionRequestParsers() {
        return transactionRequestParsers;
    }

    public void parseSettlementFiled(TransactionRequest request, String jsonString) {
        if (jsonString == null || jsonString.isEmpty()) {
            return;
        }
        if (jsonString.equals("true")) {
            request.setState(TransactionRequestState.FAILED);
        } else {
            request.setState(TransactionRequestState.ACCEPTED);
        }
    }

    public void parseTransactionFailed(TransactionRequest request, String jsonString) {
        if (jsonString == null || jsonString.isEmpty()) {
            return;
        }
        if (jsonString.equals("true")) {
            request.setState(TransactionRequestState.FAILED);
        } else {
            request.setState(TransactionRequestState.IN_PROGRESS);
        }
    }

    public void parsePartyLookUpState(TransactionRequest request, String jsonString) {
        if (jsonString == null || jsonString.isEmpty()) {
            return;
        }
        if (jsonString.equals("true")) {
            request.setState(TransactionRequestState.FAILED);
        } else {
            request.setState(TransactionRequestState.RECEIVED);
        }
    }

    public Map<String, Consumer<Pair<Batch, String>>> getBatchParsers() {
        return batchParsers;
    }

    public Map<String, Consumer<Pair<OutboudMessages, String>>> getOutboundMessageParsers() {
        return outboundMessageParsers;
    }

    private void parseQuoteSwitchRequest(Transfer transfer, String jsonString) {
        DocumentContext json = JsonPathReader.parseEscaped(jsonString);
        transfer.setTransactionId(json.read("$.transactionId"));

        transfer.setPayeePartyIdType(json.read("$.payee.partyIdInfo.partyIdType"));
        transfer.setPayeePartyId(json.read("$.payee.partyIdInfo.partyIdentifier"));
        transfer.setPayeeDfspId(json.read("$.payee.partyIdInfo.fspId"));

        transfer.setPayerPartyIdType(json.read("$.payer.partyIdInfo.partyIdType"));
        transfer.setPayerPartyId(json.read("$.payer.partyIdInfo.partyIdentifier"));
        transfer.setPayerDfspId(json.read("$.payer.partyIdInfo.fspId"));

        transfer.setAmount(json.read("$.amount.amount", BigDecimal.class));
        transfer.setCurrency(json.read("$.amount.currency"));
    }

    private void parseTransferLocalQuoteResponse(Transfer transfer, String jsonString) {
        DocumentContext json = JsonPathReader.parseEscaped(jsonString);
        if (incomingDirection.equals(transfer.getDirection())) {
            transfer.setPayeeFee(json.read("$.fspFee.amount", BigDecimal.class));
            transfer.setPayeeFeeCurrency(json.read("$.fspFee.currency"));
            transfer.setPayeeQuoteCode(json.read("$.quoteCode"));
        } else {
            transfer.setPayerFee(json.read("$.fspFee.amount", BigDecimal.class));
            transfer.setPayerFeeCurrency(json.read("$.fspFee.currency"));
            transfer.setPayerQuoteCode(json.read("$.quoteCode"));
        }
    }

    private void parsePayeeQuoteResponse(Transfer transfer, String jsonString) {
        DocumentContext json = JsonPathReader.parseEscaped(jsonString);
        transfer.setPayeeFee(json.read("$.payeeFspFee.amount", BigDecimal.class));
        transfer.setPayeeFeeCurrency(json.read("$.payeeFspFee.currency"));
    }

    private void parseTransferResponse(Transfer transfer, String jsonString) {
        DocumentContext json = JsonPathReader.parseEscaped(jsonString);
        String completedAt = json.read("$.completedTimestamp", String.class);
        SimpleDateFormat dateFormat = OperatorUtils.dateFormat();
        try {
            transfer.setCompletedAt(dateFormat.parse(completedAt));
        } catch (ParseException e) {
            logger.error("failed to parse completedTimestamp", e);
        }
    }

    private void parseTransferCreateFailed(Transfer transfer, String value) { // Handles error only based on book funds
                                                                              // failure
        transfer.setStatus("false".equals(value) ? TransferStatus.COMPLETED : TransferStatus.FAILED);
    }

    private void parseTransferResponsePrepare(Transfer transfer, String jsonString) {
        DocumentContext json = JsonPathReader.parseEscaped(jsonString);
        String completedAt = json.read("$.completedTimestamp", String.class);
        SimpleDateFormat dateFormat = OperatorUtils.dateFormat();
        try {
            transfer.setCompletedAt(dateFormat.parse(completedAt));
        } catch (ParseException e) {
            logger.error("failed to parse completedTimestamp", e);
        }
    }

    private void parseChannelRequest(Transfer transfer, String jsonString) {
        DocumentContext json = JsonPathReader.parseEscaped(jsonString);

        transfer.setPayerPartyId(json.read("$.payer.partyIdInfo.partyIdentifier"));
        transfer.setPayerPartyIdType(json.read("$.payer.partyIdInfo.partyIdType"));

        transfer.setPayeePartyId(json.read("$.payee.partyIdInfo.partyIdentifier"));
        transfer.setPayeePartyIdType(json.read("$.payee.partyIdInfo.partyIdType"));

        transfer.setAmount(json.read("$.amount.amount", BigDecimal.class));
        transfer.setCurrency(json.read("$.amount.currency"));
    }

    private void parseErrorInformation(Transfer transfer, String jsonString) {
        transfer.setErrorInformation(strip(jsonString));
    }

    private void parseErrorInformation(TransactionRequest transactionRequest, String jsonString) {
        transactionRequest.setErrorInformation(strip(jsonString));
    }

    private void parseTransactionChannelRequest(TransactionRequest transactionRequest, String jsonString) {
        DocumentContext json = JsonPathReader.parseEscaped(jsonString);
        transactionRequest.setPayerPartyId(json.read("$.payer.partyIdInfo.partyIdentifier"));
        transactionRequest.setPayerPartyIdType(json.read("$.payer.partyIdInfo.partyIdType"));

        transactionRequest.setPayeePartyId(json.read("$.payee.partyIdInfo.partyIdentifier"));
        transactionRequest.setPayeePartyIdType(json.read("$.payee.partyIdInfo.partyIdType"));

        transactionRequest.setAmount(json.read("$.amount.amount", BigDecimal.class));
        transactionRequest.setCurrency(json.read("$.amount.currency"));

        if (transactionRequest.getInitiatorType() == null) {
            transactionRequest.setInitiatorType(json.read("$.transactionType.initiatorType"));
        }
        if (transactionRequest.getScenario() == null) {
            transactionRequest.setScenario(json.read("$.transactionType.scenario"));
        }
    }

    private void parseTransactionMpesaRequest(TransactionRequest transactionRequest, String jsonString) {
        DocumentContext json = JsonPathReader.parseEscaped(jsonString);
        transactionRequest.setInitiatorType(json.read("$.transactionType.initiatorType"));
        transactionRequest.setScenario(json.read("$.transactionType.scenario"));
    }

    private void parseExternalId(TransactionRequest transactionRequest, String externalId) {
        transactionRequest.setExternalId(externalId);
    }

    private void parseClientCorrelationId(TransactionRequest transactionRequest, String clientCorrelationId) {
        transactionRequest.setClientCorrelationId(clientCorrelationId);
    }

    private void parseClientCorrelationIdTransfers(Transfer transfer, String clientCorrelationId) {
        transfer.setClientCorrelationId(clientCorrelationId);
    }

    private void parseTransactionRequestResponse(TransactionRequest transactionRequest, String jsonString) {
        DocumentContext json = JsonPathReader.parseEscaped(jsonString);
        transactionRequest.setState(TransactionRequestState.valueOf(json.read("$.transactionRequestState")));
    }

    private void parseTransactionRequestFailed(TransactionRequest transactionRequest, String value) {
        if ("true".equals(value)) {
            transactionRequest.setState(TransactionRequestState.FAILED);
        }
    }

    private void parseTransactionRequest(TransactionRequest transactionRequest, String jsonString) {
        DocumentContext json = JsonPathReader.parseEscaped(jsonString);

        transactionRequest.setTransactionId(json.read("$.transactionRequestId"));
        transactionRequest.setAmount(json.read("$.amount.amount", BigDecimal.class));
        transactionRequest.setCurrency(json.read("$.amount.currency"));

        transactionRequest.setPayeePartyId(json.read("$.payee.partyIdInfo.partyIdentifier"));
        transactionRequest.setPayeePartyIdType(json.read("$.payee.partyIdInfo.partyIdType"));
        transactionRequest.setPayeeDfspId(json.read("$.payee.partyIdInfo.fspId"));

        transactionRequest.setPayerPartyId(json.read("$.payer.partyIdentifier"));
        transactionRequest.setPayerPartyIdType(json.read("$.payer.partyIdType"));
        transactionRequest.setPayerDfspId(json.read("$.payer.fspId"));

        String authenticationType = json.read("$.authenticationType");
        transactionRequest.setAuthType(authenticationType == null ? "NONE" : authenticationType);
        transactionRequest.setScenario(json.read("$.transactionType.scenario"));
        transactionRequest.setInitiatorType(json.read("$.transactionType.initiatorType"));
    }

    private void parseTransactionRequestLocalQuoteResponse(TransactionRequest transactionRequest, String jsonString) {
        DocumentContext json = JsonPathReader.parseEscaped(jsonString);
        transactionRequest.setPayerFee(json.read("$.fspFee.amount", BigDecimal.class));
        transactionRequest.setPayerQuoteCode(json.read("$.quoteCode"));
    }

    private void parseTransactionRequestPayeeQuoteResponse(TransactionRequest transactionRequest, String jsonString) {
        DocumentContext json = JsonPathReader.parseEscaped(jsonString);
        transactionRequest.setPayeeFee(json.read("$.payeeFspFee.amount", BigDecimal.class));
    }

    private void parseInitiatorFspId(TransactionRequest transactionRequest, String jsonString) {
        if (outgoingDirection.equals(transactionRequest.getDirection())) {
            transactionRequest.setPayeeDfspId(strip(jsonString));
        }
    }

    private void parseTransActionState(TransactionRequest transactionRequest, String jsonString) {
        if (incomingDirection.equals(transactionRequest.getDirection())) {
            transactionRequest.setState(TransactionRequestState.valueOf(strip(jsonString)));
        }
    }

}
