package hu.dpc.phee.operator.value.transformer;

import hu.dpc.phee.operator.config.model.Transformer;
import hu.dpc.phee.operator.value.transformer.impl.*;
import lombok.Data;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ValueTransformerTest {

    @Data
    public static class TestEntity {
        private String testField;
    }

    @Test
    public void testVariable() {
        Transformer transformer = new Transformer();
        transformer.setField("testField");
        transformer.setVariableName("test");
        TestEntity entity = new TestEntity();
        new SimpleTransformer(transformer).apply(entity, "test");
        assertEquals("test", entity.getTestField());
    }

    @Test
    public void testConst() {
        Transformer transformer = new Transformer();
        transformer.setField("testField");
        transformer.setConstant("test");
        TestEntity entity = new TestEntity();
        new ConstTransformer(transformer).apply(entity, null);
        assertEquals("test", entity.getTestField());
    }

    @Test
    public void testXml() {
        Transformer transformer = new Transformer();
        transformer.setField("testField");
        transformer.setXpath("/dummy/test");
        TestEntity entity = new TestEntity();
        new XmlTransformer(transformer).apply(entity, "<dummy><test>test</test></dummy>");
        assertEquals("test", entity.getTestField());
    }

    @Test
    public void testJson() {
        Transformer transformer = new Transformer();
        transformer.setField("testField");
        transformer.setJsonPath("$.dummy.test");
        TestEntity entity = new TestEntity();
        new JsonTransformer(transformer).apply(entity, "{\"dummy\":{\"test\":\"test\"}}");
        assertEquals("test", entity.getTestField());
    }

    @Test
    public void testNoop() {
        Transformer transformer = new Transformer();
        transformer.setField("testField");
        transformer.setVariableName("test");
        TestEntity entity = new TestEntity();
        new NoopTransformer(transformer).apply(entity, "test");
        assertNull(entity.getTestField());
    }
}