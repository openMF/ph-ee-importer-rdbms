package hu.dpc.phee.operator.config.transformer;

import lombok.Data;

import java.util.List;

@Data
public class Flow {
    private String name;
    private String direction;
    private List<Transformer> transformers;
}