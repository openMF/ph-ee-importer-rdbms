package hu.dpc.phee.operator.value.transformer.impl;

import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.config.model.Transformer;
import hu.dpc.phee.operator.importer.JsonPathReader;
import hu.dpc.phee.operator.value.transformer.TransformType;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class JsonTransformer extends SimpleTransformer {

    public JsonTransformer(Transformer transformer) {
        super(transformer);
    }

    @Override
    public TransformType getTransformType() {
        return TransformType.JSON_PATH;
    }

    @Override
    public void apply(Object entity, Object value) {
        DocumentContext json = JsonPathReader.parse(String.valueOf(value));
        Object result = json.read(getTransformer().getJsonPath());
        log.trace("json path result is {} for variable {}", result, getTransformer().getVariableName());
        String jsonValue;
        if (result != null) {
            if (result instanceof List) {
                jsonValue = ((List<?>) result).stream().map(Object::toString).collect(Collectors.joining(" "));
            } else {
                jsonValue = result.toString();
            }
            setPropertyValue(entity, jsonValue);
        } else {
            log.warn("null result when setting field {} from variable {} using json path {} of value {}", getTransformer().getField(), getTransformer().getVariableName(), getTransformer().getJsonPath(), value);
        }
    }
}