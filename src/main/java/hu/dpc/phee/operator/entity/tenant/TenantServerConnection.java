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



import hu.dpc.phee.operator.entity.parent.AbstractPersistableCustom;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "tenant_server_connections")
@ToString
@Getter
@Setter
public class TenantServerConnection extends AbstractPersistableCustom<Long> {

    @Column(name = "schema_server")
    private String schemaServer;

    @Column(name = "schema_name")
    private String schemaName;

    @Column(name = "schema_server_port")
    private String schemaServerPort;

    @Column(name = "schema_username")
    private String schemaUsername;

    @Column(name = "schema_password")
    private String schemaPassword;

    @Column(name = "auto_update")
    private boolean autoUpdateEnabled;

    public TenantServerConnection() {}
}
