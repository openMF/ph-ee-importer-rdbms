package hu.dpc.phee.operator.value.transformer.impl;

import hu.dpc.phee.operator.config.model.Transformer;
import hu.dpc.phee.operator.value.transformer.TransformType;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NoopTransformer extends SimpleTransformer {

    public NoopTransformer(Transformer transformer) {
        super(transformer);
    }

    @Override
    public TransformType getTransformType() {
        return TransformType.NOOP;
    }

    @Override
    public void apply(Object entity, Object value) {
    }
}