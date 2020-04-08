package hu.dpc.phee.operate.importer.api;

import hu.dpc.phee.operate.importer.persistence.Task;
import hu.dpc.phee.operate.importer.persistence.TaskRepository;
import hu.dpc.phee.operate.importer.persistence.Transaction;
import hu.dpc.phee.operate.importer.persistence.TransactionRepository;
import hu.dpc.phee.operate.importer.persistence.Variable;
import hu.dpc.phee.operate.importer.persistence.VariableRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@RestController
public class RestApiController {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private VariableRepository variableRepository;

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

    private List<Transaction> loadTransactions(@RequestParam("businessKey") String businessKey, @RequestParam("businessKeyType") String businessKeyType) {
        List<Transaction> transactions = transactionRepository.findByBusinessKeyAndBusinessKeyType(businessKey, businessKeyType);
        logger.debug("loaded {} transaction(s) for business key {} of type {}", transactions.size(), businessKey, businessKeyType);
        return transactions;
    }

}
