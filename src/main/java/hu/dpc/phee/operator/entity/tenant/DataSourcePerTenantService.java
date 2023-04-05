/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package hu.dpc.phee.operator.entity.tenant;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;


@Service
public class DataSourcePerTenantService implements DisposableBean {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final Map<Long, DataSource> tenantToDataSourceMap = new HashMap<>();

    @Value("${datasource.core.port}")
    private int defaultPort;

    @Value("${datasource.core.host}")
    private String defaultHostname;

    @Value("${datasource.core.schema}")
    private String defaultSchema;

    @Value("${datasource.core.username}")
    private String defaultUsername;

    @Value("${datasource.core.password}")
    private String defaultPassword;

    @Value("${datasource.common.protocol}")
    private String jdbcProtocol;

    @Value("${datasource.common.subprotocol}")
    private String jdbcSubprotocol;

    @Value("${datasource.common.driverclass_name}")
    private String driverClass;

    public DataSource retrieveDataSource() {
        DataSource tenantDataSource;

        final TenantServerConnection tenant = ThreadLocalContextUtil.getTenant();
        logger.trace("Retrieving datasource for tenant: {}", tenant);

        if (tenant != null) {
            if (this.tenantToDataSourceMap.containsKey(tenant.getId())) {
                tenantDataSource = this.tenantToDataSourceMap.get(tenant.getId());
            } else {
                synchronized (this.tenantToDataSourceMap) {
                    if (this.tenantToDataSourceMap.containsKey(tenant.getId())) {
                        return this.tenantToDataSourceMap.get(tenant.getId());
                    } else {
                        logger.info("Creating new datasource for tenant: {}", tenant);
                        tenantDataSource = createNewDataSourceFor(tenant);
                        this.tenantToDataSourceMap.put(tenant.getId(), tenantDataSource);
                    }
                }
            }
        } else {
            long defaultConnectionKey = 0;
            if (this.tenantToDataSourceMap.containsKey(defaultConnectionKey)) {
                tenantDataSource = this.tenantToDataSourceMap.get(defaultConnectionKey);
            } else {
                synchronized (this.tenantToDataSourceMap) {
                    if (this.tenantToDataSourceMap.containsKey(defaultConnectionKey)) {
                        return this.tenantToDataSourceMap.get(tenant.getId());
                    } else {
                        logger.info("Creating new datasource for default connection");
                        TenantServerConnection defaultConnection = new TenantServerConnection();
                        defaultConnection.setSchemaServer(defaultHostname);
                        defaultConnection.setSchemaServerPort(String.valueOf(defaultPort));
                        defaultConnection.setSchemaName(defaultSchema);
                        defaultConnection.setSchemaUsername(defaultUsername);
                        defaultConnection.setSchemaPassword(defaultPassword);
                        tenantDataSource = createNewDataSourceFor(defaultConnection);
                        this.tenantToDataSourceMap.put(defaultConnectionKey, tenantDataSource);
                    }
                }
            }
        }

        return tenantDataSource;
    }

    private DataSource createNewDataSourceFor(TenantServerConnection tenant) {
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

    private String createJdbcUrl(String jdbcProtocol, String jdbcSubprotocol, String hostname, int port, String dbName) {
        return jdbcProtocol + ':' + jdbcSubprotocol + "://" + hostname + ':' + port + '/' + dbName;
    }

    @Override
    public void destroy() {
        for (Map.Entry<Long, DataSource> entry : this.tenantToDataSourceMap.entrySet()) {
            HikariDataSource ds = (HikariDataSource) entry.getValue();
            ds.close();
            logger.info("Datasource closed: {}", ds.getPoolName());
        }
    }
}
