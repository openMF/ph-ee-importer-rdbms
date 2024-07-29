package hu.dpc.phee.operator.value.transformer;

import hu.dpc.phee.operator.config.model.Transformer;

public interface ValueTransformer {

    TransformType getTransformType();

    Transformer getTransformer();

    void apply(Object entity, Object value);
}