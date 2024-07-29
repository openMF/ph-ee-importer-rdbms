package hu.dpc.phee.operator.value.transformer.impl;

import hu.dpc.phee.operator.config.model.Transformer;
import hu.dpc.phee.operator.value.transformer.TransformType;
import hu.dpc.phee.operator.value.transformer.ValueTransformer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@Slf4j
@Getter
public class SimpleTransformer implements ValueTransformer {

    private final Transformer transformer;

    public SimpleTransformer(Transformer transformer) {
        this.transformer = transformer;
    }

    @Override
    public TransformType getTransformType() {
        return TransformType.VARIABLE;
    }

    @Override
    public void apply(Object entity, Object value) {
        setPropertyValue(entity, value);
    }

    protected void setPropertyValue(Object entity, Object value) {
        log.trace("setting value {} for field {} in entity {}", value, transformer.getField(), entity);
        BeanWrapper wrapper = PropertyAccessorFactory.forBeanPropertyAccess(entity);
        String propertyType = wrapper.getPropertyType(transformer.getField()).getName();
        if (Date.class.getName().equals(propertyType)) {
            try {
                wrapper.setPropertyValue(transformer.getField(), new SimpleDateFormat(transformer.getDateFormat()).parse(String.valueOf(value)));
            } catch (ParseException pe) {
                log.warn("failed to parse date {} with format {}", value, transformer.getDateFormat());
            }
        } else {
            wrapper.setPropertyValue(transformer.getField(), value);
        }
    }
}