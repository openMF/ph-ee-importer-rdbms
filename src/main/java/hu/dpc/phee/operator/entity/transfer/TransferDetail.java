package hu.dpc.phee.operator.entity.transfer;

import hu.dpc.phee.operator.entity.task.Task;
import hu.dpc.phee.operator.entity.variable.Variable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.List;


@Getter
@Setter
@AllArgsConstructor
public class TransferDetail {
    private Transfer transfer;
    private List<Task> tasks;
    private List<Variable> variables;
}
