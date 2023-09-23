package hu.dpc.phee.operator.tenants;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class TenantsService implements DisposableBean {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${datasource.common.protocol}")
    private String jdbcProtocol;

    @Value("${datasource.common.subprotocol}")
    private String jdbcSubprotocol;

    @Value("${datasource.common.driverclass_name}")
    private String driverClass;


    @Autowired
    private Environment environment;

    @Autowired
    TenantConnections tenantConnectionList;

    private Map<String, TenantConnectionProperties> tenantConnectionProperties;

    private Map<String, DataSource> tenantDataSources = new HashMap<>();


    @PostConstruct
    public void setup() {
        String[] activeProfiles = environment.getActiveProfiles();
        List<String> tenants = Stream.of(activeProfiles)
                .map(profile -> profile.startsWith("tenant-") ? profile.substring("tenant-".length()) : null)
                .filter(Objects::nonNull)
                .toList();
        logger.info("Loaded tenants from configuration: {}", tenants);

        if (tenantConnectionList.getConnections().isEmpty()) {
            throw new RuntimeException("No tenant connection properties found in configuration");
        }

        this.tenantConnectionProperties = tenantConnectionList.getConnections().stream()
                .collect(Collectors.toMap(TenantConnectionProperties::getName, it -> it));
        logger.info("loaded {} tenant config properties: {}", tenantConnectionProperties.size(), tenantConnectionProperties);

        tenantConnectionProperties.forEach((name, properties) -> {
            logger.info("Creating datasource for tenant {}", name);
            tenantDataSources.put(name, createNewDataSourceFor(properties));
        });
    }


    public DataSource getTenantDataSource(String tenantIdentifier) {
        return tenantDataSources.get(tenantIdentifier);
    }

    // for initializing JPA repositories
    public DataSource getAnyDataSource() {
        return tenantDataSources.values().iterator().next();
    }

    @Override
    public void destroy() {
        logger.info("Closing {} datasources", tenantDataSources.size());
        this.tenantDataSources.forEach((name, ds) -> {
            try {
                ((HikariDataSource) ds).close();
            } catch (Exception e) {
                logger.error("Error closing datasource for tenant {}", name, e);
            }
        });
    }


    private String createJdbcUrl(String jdbcProtocol, String jdbcSubprotocol, String hostname, int port, String dbName) {
        return jdbcProtocol + ':' + jdbcSubprotocol + "://" + hostname + ':' + port + '/' + dbName;
    }

    public DataSource createNewDataSourceFor(TenantConnectionProperties tenant) {
        HikariConfig config = new HikariConfig();
        int port = Integer.parseInt(tenant.getSchemaServerPort());
        config.setJdbcUrl(createJdbcUrl(jdbcProtocol, jdbcSubprotocol, tenant.getSchemaServer(), port, tenant.getSchemaName()));
        config.setUsername(tenant.getSchemaUsername());
        config.setPassword(tenant.getSchemaPassword());
        config.setAutoCommit(false);
        config.setConnectionInitSql("SELECT 1");
        config.setValidationTimeout(30000);
        config.setConnectionTestQuery("SELECT 1");
        config.setConnectionTimeout(30000);
        config.setDriverClassName(driverClass);
        config.setIdleTimeout(600000);
        config.setMaximumPoolSize(20);
        config.setMinimumIdle(5);
        config.setPoolName(tenant.getSchemaName() + "Pool");
        logger.info("Hikari pool created for tenant: {}", tenant);
        return new HikariDataSource(config);
    }

}
