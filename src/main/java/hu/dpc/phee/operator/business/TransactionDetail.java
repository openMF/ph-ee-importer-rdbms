package hu.dpc.phee.operator.business;

import hu.dpc.phee.operator.audit.Task;
import hu.dpc.phee.operator.audit.Variable;

import java.util.List;


public class TransactionDetail {
    private Transaction transaction;
    private List<Task> tasks;
    private List<Variable> variables;

    public TransactionDetail(Transaction transaction, List<Task> tasks, List<Variable> variables) {
        this.transaction = transaction;
        this.tasks = tasks;
        this.variables = variables;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    public List<Task> getTasks() {
        return tasks;
    }

    public void setTasks(List<Task> tasks) {
        this.tasks = tasks;
    }

    public List<Variable> getVariables() {
        return variables;
    }

    public void setVariables(List<Variable> variables) {
        this.variables = variables;
    }
}
