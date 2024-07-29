package hu.dpc.phee.operator.value.transformer.impl;

import hu.dpc.phee.operator.config.model.Transformer;
import hu.dpc.phee.operator.value.transformer.TransformType;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConstTransformer extends SimpleTransformer {

    public ConstTransformer(Transformer transformer) {
        super(transformer);
    }

    @Override
    public TransformType getTransformType() {
        return TransformType.CONSTANT;
    }

    @Override
    public void apply(Object entity, Object value) {
        setPropertyValue(entity, getTransformer().getConstant());
    }
}