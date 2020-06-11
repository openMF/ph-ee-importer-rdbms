package hu.dpc.phee.operator.importer;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.OperatorUtils;
import hu.dpc.phee.operator.entity.transactionrequest.TransactionRequest;
import hu.dpc.phee.operator.entity.transactionrequest.TransactionRequestState;
import hu.dpc.phee.operator.entity.transfer.Transfer;
import hu.dpc.phee.operator.entity.transfer.TransferStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static hu.dpc.phee.operator.OperatorUtils.strip;

@Component
public class VariableParser {

    @Value("${bpmn.incoming-direction}")
    private String incomingDirection;

    @Value("${bpmn.outgoing-direction}")
    private String outgoingDirection;

    private final Logger logger = LoggerFactory.getLogger(VariableParser.class);
    private final Map<String, Consumer<Pair<Transfer, String>>> transferParsers = new HashMap<>();
    private final Map<String, Consumer<Pair<TransactionRequest, String>>> transactionRequestParsers = new HashMap<>();

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

        transactionRequestParsers.put("authType", pair -> pair.getFirst().setAuthType(strip(pair.getSecond())));
        transactionRequestParsers.put("transactionId", pair -> pair.getFirst().setTransactionId(strip(pair.getSecond())));
        transactionRequestParsers.put("partyLookupFspId", pair -> pair.getFirst().setPayerDfspId(strip(pair.getSecond())));
        transactionRequestParsers.put("initiatorFspId", pair -> pair.getFirst().setPayeeDfspId(strip(pair.getSecond())));
        transactionRequestParsers.put("channelRequest", pair -> parseTransactionChannelRequest(pair.getFirst(), pair.getSecond()));
        transactionRequestParsers.put("transactionRequestResponse", pair -> parseTransactionRequestResponse(pair.getFirst(), pair.getSecond()));
        transactionRequestParsers.put("transactionRequestFailed", pair -> parseTransactionRequestFailed(pair.getFirst(), pair.getSecond()));
        transactionRequestParsers.put("transactionRequest", pair -> parseTransactionRequest(pair.getFirst(), pair.getSecond()));
        transactionRequestParsers.put("localQuoteResponse", pair -> parseTransactionRequestLocalQuoteResponse(pair.getFirst(), pair.getSecond()));
        transactionRequestParsers.put("payeeQuoteResponse", pair -> parseTransactionRequestPayeeQuoteResponse(pair.getFirst(), pair.getSecond()));
        transactionRequestParsers.put("quoteId", pair -> pair.getFirst().setPayeeQuoteCode(strip(pair.getSecond())));
        transactionRequestParsers.put("transActionState", pair -> pair.getFirst().setState(TransactionRequestState.valueOf(strip(pair.getSecond()))));
    }

    public Map<String, Consumer<Pair<Transfer, String>>> getTransferParsers() {
        return transferParsers;
    }

    public Map<String, Consumer<Pair<TransactionRequest, String>>> getTransactionRequestParsers() {
        return transactionRequestParsers;
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

    private void parseTransferCreateFailed(Transfer transfer, String value) {
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

    private void parseTransactionChannelRequest(TransactionRequest transactionRequest, String jsonString) {
        DocumentContext json = JsonPathReader.parseEscaped(jsonString);
        transactionRequest.setPayerPartyId(json.read("$.payer.partyIdInfo.partyIdentifier"));
        transactionRequest.setPayerPartyIdType(json.read("$.payer.partyIdInfo.partyIdType"));

        transactionRequest.setPayeePartyId(json.read("$.payee.partyIdInfo.partyIdentifier"));
        transactionRequest.setPayeePartyIdType(json.read("$.payee.partyIdInfo.partyIdType"));

        transactionRequest.setAmount(json.read("$.amount.amount", BigDecimal.class));
        transactionRequest.setCurrency(json.read("$.amount.currency"));

        transactionRequest.setInitiatorType(json.read("$.transactionType.initiatorType"));
        transactionRequest.setScenario(json.read("$.transactionType.scenario"));
    }

    private void parseTransactionRequestResponse(TransactionRequest transactionRequest, String jsonString) {
        DocumentContext json = JsonPathReader.parseEscaped(jsonString);
        transactionRequest.setState(json.read("$.transactionRequestState"));
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

        transactionRequest.setAuthType(json.read("$.authenticationType"));
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
}