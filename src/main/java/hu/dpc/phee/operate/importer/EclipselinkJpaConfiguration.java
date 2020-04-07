package hu.dpc.phee.operate.importer;

import org.eclipse.persistence.config.PersistenceUnitProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.orm.jpa.JpaBaseConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.JpaProperties;
import org.springframework.boot.autoconfigure.transaction.TransactionManagerCustomizers;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.instrument.classloading.InstrumentationLoadTimeWeaver;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.vendor.AbstractJpaVendorAdapter;
import org.springframework.orm.jpa.vendor.EclipseLinkJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.jta.JtaTransactionManager;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EntityScan(basePackages = "hu.dpc.phee.operate.importer")
@EnableJpaRepositories(basePackages = "hu.dpc.phee.operate.importer")
@EnableTransactionManagement(proxyTargetClass = true)
public class EclipselinkJpaConfiguration extends JpaBaseConfiguration {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    protected EclipselinkJpaConfiguration(DataSource dataSource, JpaProperties properties, ObjectProvider<JtaTransactionManager> jtaTransactionManager, ObjectProvider<TransactionManagerCustomizers> transactionManagerCustomizers) {
        super(dataSource, properties, jtaTransactionManager);
    }

    @Override
    protected AbstractJpaVendorAdapter createJpaVendorAdapter() {
        return new EclipseLinkJpaVendorAdapter();
    }

    @Override
    protected Map<String, Object> getVendorProperties() {
        HashMap<String, Object> map = new HashMap<>();
        map.put(PersistenceUnitProperties.WEAVING, detectWeavingMode());
        map.put(PersistenceUnitProperties.DDL_GENERATION, "drop-and-create-tables");
//        map.put(PersistenceUnitProperties.DDL_GENERATION, "create-or-extend-tables");
        map.put(PersistenceUnitProperties.LOGGING_LEVEL, "INFO");
        map.put("eclipselink.jdbc.batch-writing", "JDBC");
        map.put("eclipselink.jdbc.batch-writing.size", "1000");
        map.put("eclipselink.jdbc.cache-statements", "true");

        map.put("eclipselink.logging.level.sql", "WARNING");
        map.put("eclipselink.logging.parameters", "false");
        map.put("eclipselink.logging.session", "true");
        map.put("eclipselink.logging.thread", "true");
        map.put("eclipselink.logging.timestamp", "false");
        return map;
    }

    private String detectWeavingMode() {
        String weavingMode = InstrumentationLoadTimeWeaver.isInstrumentationAvailable() ? "true" : "static";
        logger.debug("weaving mode is set to {}", weavingMode);
        return weavingMode;
    }
}
