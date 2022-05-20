/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.connect.oceanbase.config;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class OceanBaseConnectConfig {

    private String username;
    private String password;
    private String tenantName;
    private String databaseName;
    private String tableName;
    private String hostname;
    private Integer port;
    private Duration connectTimeout;
    private String rsList;
    private String logProxyHost;
    private Integer logProxyPort;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getTenantName() {
        return tenantName;
    }

    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public Duration getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(Duration connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public String getRsList() {
        return rsList;
    }

    public void setRsList(String rsList) {
        this.rsList = rsList;
    }

    public String getLogProxyHost() {
        return logProxyHost;
    }

    public void setLogProxyHost(String logProxyHost) {
        this.logProxyHost = logProxyHost;
    }

    public Integer getLogProxyPort() {
        return logProxyPort;
    }

    public void setLogProxyPort(Integer logProxyPort) {
        this.logProxyPort = logProxyPort;
    }

    public static final Set<String> REQUEST_CONFIG = Collections.unmodifiableSet(new HashSet<String>() {
        {
            add("username");
            add("password");
            add("tenantName");
            add("databaseName");
            add("tableName");
            add("rsList");
            add("logProxyHost");
            add("logProxyPort");
        }
    });
}
