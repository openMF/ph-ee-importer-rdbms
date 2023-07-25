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

import com.zaxxer.hikari.HikariDataSource;
import hu.dpc.phee.operator.tenants.TenantsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;


@Service
public class DataSourcePerTenantService implements DisposableBean {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final Map<Long, DataSource> tenantToDataSourceMap = new HashMap<>();

    @Autowired
    TenantsService tenantsService;


    public DataSource retrieveDataSource() {
        DataSource threadLocalDatasource = ThreadLocalContextUtil.getTenant();
        return threadLocalDatasource != null ? threadLocalDatasource : tenantsService.getAnyDataSource();
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
