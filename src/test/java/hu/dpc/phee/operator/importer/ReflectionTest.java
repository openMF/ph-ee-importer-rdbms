package hu.dpc.phee.operator.importer;

import hu.dpc.phee.operator.entity.transfer.Transfer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.PropertyAccessorFactory;

public class ReflectionTest {
    @Test
    public void test() {
        Transfer transfer = new Transfer();
        PropertyAccessorFactory.forBeanPropertyAccess(transfer).setPropertyValue("clientCorrelationId", "123");
    }
}
