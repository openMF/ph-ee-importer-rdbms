package hu.dpc.phee.operator.api;

import hu.dpc.phee.operator.business.Transaction;
import hu.dpc.phee.operator.business.TransactionRepository;
import hu.dpc.phee.operator.business.TransactionSpecs;
import hu.dpc.phee.operator.business.TransactionStatus;
import hu.dpc.phee.operator.business.Transaction_;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static hu.dpc.phee.operator.OperatorUtils.dateFormat;

@RestController
public class RestQueryApiController {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private TransactionRepository transactionRepository;


    @GetMapping("/transactions")
    public Page<Transaction> transactions(
            @RequestParam(value = "page") Integer page,
            @RequestParam(value = "size") Integer size,
            @RequestParam(value = "payerPartyId", required = false) String payerPartyId,
            @RequestParam(value = "payeePartyId", required = false) String payeePartyId,
            @RequestParam(value = "payeeDfspId", required = false) String payeeDfspId,
            @RequestParam(value = "transactionId", required = false) String transactionId,
            @RequestParam(value = "status", required = false) String status,
            @RequestParam(value = "amount", required = false) BigDecimal amount,
            @RequestParam(value = "currency", required = false) String currency,
            @RequestParam(value = "startFrom", required = false) String startFrom,
            @RequestParam(value = "startTo", required = false) String startTo
    ) {
        List<Specification<Transaction>> specs = new ArrayList<>();
        if (payerPartyId != null) {
            specs.add(TransactionSpecs.match(Transaction_.payerPartyId, payerPartyId));
        }
        if (payeePartyId != null) {
            specs.add(TransactionSpecs.match(Transaction_.payeePartyId, payeePartyId));
        }
        if (payeeDfspId != null) {
            specs.add(TransactionSpecs.match(Transaction_.payeeDfspId, payeeDfspId));
        }
        if (transactionId != null) {
            specs.add(TransactionSpecs.match(Transaction_.transactionId, transactionId));
        }
        if (status != null && parseStatus(status) != null) {
            specs.add(TransactionSpecs.match(Transaction_.status, parseStatus(status)));
        }
        if (amount != null) {
            specs.add(TransactionSpecs.match(Transaction_.amount, amount));
        }
        if (currency != null) {
            specs.add(TransactionSpecs.match(Transaction_.currency, currency));
        }
        try {
            if (startFrom != null && startTo != null) {
                specs.add(TransactionSpecs.between(Transaction_.startedAt, dateFormat().parse(startFrom), dateFormat().parse(startTo)));
            } else if (startFrom != null) {
                specs.add(TransactionSpecs.later(Transaction_.startedAt, dateFormat().parse(startFrom)));
            } else if (startTo != null) {
                specs.add(TransactionSpecs.earlier(Transaction_.startedAt, dateFormat().parse(startTo)));
            }
        } catch (Exception e) {
            logger.warn("failed to parse dates {} / {}", startFrom, startTo);
        }

        PageRequest pager = PageRequest.of(page, size, Sort.by("startedAt").ascending());

        if (specs.size() > 0) {
            Specification<Transaction> compiledSpecs = specs.get(0);
            for (int i = 1; i < specs.size(); i++) {
                compiledSpecs = compiledSpecs.and(specs.get(i));
            }

            return transactionRepository.findAll(compiledSpecs, pager);
        } else {
            return transactionRepository.findAll(pager);
        }

    }

    private TransactionStatus parseStatus(@RequestParam(value = "transactionStatus", required = false) String
                                                  transactionStatus) {
        try {
            return transactionStatus == null ? null : TransactionStatus.valueOf(transactionStatus);
        } catch (Exception e) {
            logger.warn("failed to parse transaction status {}, ignoring it", transactionStatus);
            return null;
        }
    }
}
