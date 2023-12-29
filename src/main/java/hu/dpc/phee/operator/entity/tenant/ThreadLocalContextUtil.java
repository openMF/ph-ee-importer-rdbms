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

import org.springframework.util.Assert;
import javax.sql.DataSource;

public final class ThreadLocalContextUtil {

    private ThreadLocalContextUtil() {}

    private static final ThreadLocal<DataSource> tenantcontext = new ThreadLocal<>();

    public static void setTenant(final DataSource dataSource) {
        Assert.notNull(dataSource, "tenant dataSource cannot be null");
        tenantcontext.set(dataSource);
    }

    public static DataSource getTenant() {
        return tenantcontext.get();
    }

    public static void clear() {
        tenantcontext.remove();
    }
}
