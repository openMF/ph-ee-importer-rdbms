package hu.dpc.phee.operator.config.model;

import lombok.Data;

@Data
public class Transformer {
    private String field;
    private String variableName;
    private String jsonPath;
    private String constant;
    private String xpath;
    private String dateFormat;
}