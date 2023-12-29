package hu.dpc.phee.operator.entity.variable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;

import java.io.Serializable;

@Data
@With
@NoArgsConstructor
@AllArgsConstructor
public class VariableId implements Serializable {

    private Long workflowInstanceKey;
    private String name;

}
