package hu.dpc.phee.operator.value.transformer;

import hu.dpc.phee.operator.config.model.Flow;
import hu.dpc.phee.operator.config.model.Transformer;
import hu.dpc.phee.operator.event.parser.impl.transfer.config.TransferTransformerConfig;
import hu.dpc.phee.operator.event.parser.impl.transport.config.FileTransportTransformerConfig;
import hu.dpc.phee.operator.value.transformer.impl.*;
import jakarta.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class ValueTransformers {

    @Autowired
    private FileTransportTransformerConfig fileTransportTransformerConfig;

    @Autowired
    private TransferTransformerConfig transferTransformerConfig;

    private final Map<String, List<ValueTransformer>> transformers = new HashMap<>();

    @PostConstruct
    public void setup() {
        setupFlowTransformers(transferTransformerConfig.getFlows());
        setupFlowTransformers(fileTransportTransformerConfig.getFlows());
    }

    public boolean applyForVariable(String bpmnProcessId, String variableName, Object entity, String value) {
        List<ValueTransformer> validTransformers = transformers.get(bpmnProcessId).stream()
                .filter(transformer -> variableName.equals(transformer.getTransformer().getVariableName()))
                .toList();
        validTransformers.forEach(transformer -> transformer.apply(entity, value));
        return !validTransformers.isEmpty();
    }

    public void applyForConstants(String bpmnProcessId, Object entity) {
        transformers.get(bpmnProcessId).stream()
                .filter(transformer -> transformer.getTransformType().equals(TransformType.CONSTANT))
                .forEach(transformer -> transformer.apply(entity, null));
    }

    private void setupFlowTransformers(List<Flow> flows) {
        flows.forEach(flow -> transformers.put(flow.getName(), flow.getTransformers().stream()
                .map(transformer -> switch (getTransformType(transformer)) {
                    case CONSTANT -> new ConstTransformer(transformer);
                    case JSON_PATH -> new JsonTransformer(transformer);
                    case XML_PATH -> new XmlTransformer(transformer);
                    case VARIABLE -> new SimpleTransformer(transformer);
                    case NOOP -> new NoopTransformer(transformer);
                })
                .collect(Collectors.toList())));
    }

    private TransformType getTransformType(Transformer transformer) {
        if (StringUtils.isNotBlank(transformer.getConstant())) {
            return TransformType.CONSTANT;
        }
        if (Strings.isNotBlank(transformer.getJsonPath())) {
            return TransformType.JSON_PATH;
        }
        if (Strings.isNotBlank(transformer.getXpath())) {
            return TransformType.XML_PATH;
        }
        if (Strings.isNotBlank(transformer.getVariableName())) {
            return TransformType.VARIABLE;
        }
        return TransformType.NOOP;
    }
}