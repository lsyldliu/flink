/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.gateway.workflow.dolphinscheduler.entity;

/** DS materialized table parameter. */
public class MaterializedTableParameters {

    private String identifier;
    private String gatewayEndpoint;
    private boolean isPeriodic;
    private String dynamicOptions;
    private String staticPartitions;
    private String executionConfig;
    private String initConfig;
    private String statementDescription;

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public String getGatewayEndpoint() {
        return gatewayEndpoint;
    }

    public void setGatewayEndpoint(String gatewayEndpoint) {
        this.gatewayEndpoint = gatewayEndpoint;
    }

    public boolean isPeriodic() {
        return isPeriodic;
    }

    public void setPeriodic(boolean periodic) {
        isPeriodic = periodic;
    }

    public String getDynamicOptions() {
        return dynamicOptions;
    }

    public void setDynamicOptions(String dynamicOptions) {
        this.dynamicOptions = dynamicOptions;
    }

    public String getStaticPartitions() {
        return staticPartitions;
    }

    public void setStaticPartitions(String staticPartitions) {
        this.staticPartitions = staticPartitions;
    }

    public String getExecutionConfig() {
        return executionConfig;
    }

    public void setExecutionConfig(String executionConfig) {
        this.executionConfig = executionConfig;
    }

    public String getInitConfig() {
        return initConfig;
    }

    public void setInitConfig(String initConfig) {
        this.initConfig = initConfig;
    }

    public String getStatementDescription() {
        return statementDescription;
    }

    public void setStatementDescription(String statementDescription) {
        this.statementDescription = statementDescription;
    }
}
