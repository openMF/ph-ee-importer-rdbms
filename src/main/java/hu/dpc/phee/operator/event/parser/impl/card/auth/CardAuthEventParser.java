package hu.dpc.phee.operator.event.parser.impl.card.auth;

import com.baasflow.commons.events.Event;
import com.baasflow.commons.events.EventType;
import hu.dpc.phee.operator.entity.card.CardTransaction;
import hu.dpc.phee.operator.entity.card.CardTransactionRepository;
import hu.dpc.phee.operator.event.parser.impl.card.auth.entity.TransactionFeedRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@Component
@Slf4j
public class CardAuthEventParser {

    private static final String DATETIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private static final SimpleDateFormat DATETIME_FORMAT = new SimpleDateFormat(DATETIME_PATTERN);

    @Autowired
    private CardTransactionRepository cardTransactionRepository;

    public boolean isAbleToProcess(Event event) {
        return "dipocket-connector".equals(event.getSourceModule()) &&
                "transaction-feed-request".equals(event.getEvent())
                && EventType.audit.equals(event.getEventType());
    }

    @Transactional
    public void process(Event event) {
        log.debug("processing card auth event record");
        log.trace("event: {}", event);
        String json = event.getPayload();
        log.trace("payload: {}", json);
        TransactionFeedRequest request = TransactionFeedRequest.fromJson(json);
        CardTransaction cardTransaction = new CardTransaction();
        cardTransaction.setWorkflowInstanceKey(event.getId().toString());
        Date now = new Date();
        cardTransaction.setStartedAt(now);
        cardTransaction.setCompletedAt(now);
        cardTransaction.setLastUpdated(now);
        cardTransaction.setBusinessProcessStatus("NOT_PROVIDED");
        Date transactionDateTime;
        try {
            transactionDateTime = DATETIME_FORMAT.parse(request.getEventDate());
        } catch (ParseException e) {
            throw new RuntimeException("failed to convert transactionDateTime to Date", e);
        }
        cardTransaction.setTransactionDateTime(transactionDateTime);
        cardTransaction.setAmount(request.getAccAmount());
        cardTransaction.setCurrency(request.getAccCurrencyCode());
        cardTransaction.setTransactionReference(request.getTrnRef());
        cardTransaction.setRequestId(request.getRequestId());
        cardTransaction.setMerchName(request.getMerchName());
        cardTransaction.setMerchCountry(request.getMerchCountry());
        cardTransaction.setInstructedAmount(request.getTrnAmount());
        cardTransaction.setInstructedCurrency(request.getTrnCurrencyCode());
        cardTransaction.setMerchCategoryCode(request.getMccCode());
        cardTransaction.setFeeAmount(request.getFeeAmount());
        cardTransaction.setHoldAmount(request.getHoldAmount());
        cardTransaction.setToken(request.getToken());
        cardTransaction.setCardAccountId(String.valueOf(request.getAccId().longValue()));
        cardTransaction.setRequest(json);
        cardTransaction.setPaymentTokenWallet(request.getPaymentTokenWallet());
        cardTransactionRepository.save(cardTransaction);
    }
}