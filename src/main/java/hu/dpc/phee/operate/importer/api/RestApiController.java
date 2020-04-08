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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    public Map<String, List<Variable>> variables(
            @RequestParam(value = "businessKey") String businessKey,
            @RequestParam(value = "businessKeyType") String businessKeyType
    ) {
        Map<String, List<Variable>> results = new HashMap<>();

        int count = 0;
        for (Transaction transaction : loadTransactions(businessKey, businessKeyType)) {
            List<Variable> variables = variableRepository.findByWorkflowInstanceKey(transaction.getWorkflowInstanceKey());
            results.put(transaction.getBusinessKey() + "-" + ++count, variables);
        }

        return results;
    }

    @GetMapping("/tasks")
    public Map<String, List<Task>> tasks(
            @RequestParam(value = "businessKey") String businessKey,
            @RequestParam(value = "businessKeyType") String businessKeyType
    ) {
        Map<String, List<Task>> results = new HashMap<>();

        int count = 0;
        for (Transaction transaction : loadTransactions(businessKey, businessKeyType)) {
            List<Task> tasks = taskRepository.findByWorkflowInstanceKey(transaction.getWorkflowInstanceKey());
            results.put(transaction.getBusinessKey() + "-" + ++count, tasks);
        }

        return results;
    }

    private List<Transaction> loadTransactions(@RequestParam("businessKey") String businessKey, @RequestParam("businessKeyType") String businessKeyType) {
        List<Transaction> transactions = transactionRepository.findByBusinessKeyAndBusinessKeyType(businessKey, businessKeyType);
        logger.debug("loaded {} transaction(s) for business key {} of type {}", transactions.size(), businessKey, businessKeyType);
        return transactions;
    }

}
