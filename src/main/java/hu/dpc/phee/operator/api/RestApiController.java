package hu.dpc.phee.operator.api;

import hu.dpc.phee.operator.audit.BusinessKey;
import hu.dpc.phee.operator.audit.BusinessKeyRepository;
import hu.dpc.phee.operator.audit.Task;
import hu.dpc.phee.operator.audit.TaskRepository;
import hu.dpc.phee.operator.audit.Variable;
import hu.dpc.phee.operator.audit.VariableRepository;
import hu.dpc.phee.operator.business.Transaction;
import hu.dpc.phee.operator.business.TransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@RestController
public class RestApiController {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private BusinessKeyRepository businessKeyRepository;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private VariableRepository variableRepository;

    @Autowired
    private TransactionRepository transactionRepository;


    @GetMapping("/transactions")
    public List<Transaction> transactions(
            @RequestParam(value = "page") Integer page,
            @RequestParam(value = "size") Integer size
    ) {
        return transactionRepository.findAllByCompletedAtNotNull(PageRequest.of(page, size, Sort.by("startedAt").ascending()));
    }

    @GetMapping("/variables")
    public List<List<Variable>> variables(
            @RequestParam(value = "businessKey") String businessKey,
            @RequestParam(value = "businessKeyType") String businessKeyType
    ) {
        return loadTransactions(businessKey, businessKeyType).stream()
                .map(transaction -> variableRepository.findByWorkflowInstanceKey(transaction.getWorkflowInstanceKey()))
                .collect(Collectors.toList());
    }

    @GetMapping("/tasks")
    public List<List<Task>> tasks(
            @RequestParam(value = "businessKey") String businessKey,
            @RequestParam(value = "businessKeyType") String businessKeyType
    ) {
        return loadTransactions(businessKey, businessKeyType).stream()
                .map(transaction -> taskRepository.findByWorkflowInstanceKey(transaction.getWorkflowInstanceKey()))
                .collect(Collectors.toList());
    }

    private List<BusinessKey> loadTransactions(@RequestParam("businessKey") String businessKey, @RequestParam("businessKeyType") String businessKeyType) {
        List<BusinessKey> businessKeys = businessKeyRepository.findByBusinessKeyAndBusinessKeyType(businessKey, businessKeyType);
        logger.debug("loaded {} transaction(s) for business key {} of type {}", businessKeys.size(), businessKey, businessKeyType);
        return businessKeys;
    }

}
