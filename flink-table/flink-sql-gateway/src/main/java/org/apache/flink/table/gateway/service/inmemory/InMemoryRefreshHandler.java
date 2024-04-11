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

package org.apache.flink.table.gateway.service.inmemory;

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.dynamic.RefreshHandler;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/** A {@link RefreshHandler} instance for in-memory scheduler. */
public class InMemoryRefreshHandler implements RefreshHandler, Serializable {

    private final ObjectIdentifier dynamicTableIdentifier;
    private final String descriptionStatement;
    private final String workflowName;
    private final String schedulerInterval;
    private final boolean isPeriodic;
    private final Map<String, String> staticPartitions;
    private final Map<String, String> executionConfig;

    public InMemoryRefreshHandler(
            ObjectIdentifier dynamicTableIdentifier,
            String descriptionStatement,
            String workflowName,
            String schedulerInterval,
            boolean isPeriodic,
            Map<String, String> staticPartitions,
            Map<String, String> executionConfig) {
        this.dynamicTableIdentifier = dynamicTableIdentifier;
        this.descriptionStatement = descriptionStatement;
        this.workflowName = workflowName;
        this.schedulerInterval = schedulerInterval;
        this.isPeriodic = isPeriodic;
        this.staticPartitions = staticPartitions;
        this.executionConfig = executionConfig;
    }

    public ObjectIdentifier getDynamicTableIdentifier() {
        return dynamicTableIdentifier;
    }

    public String getDescriptionStatement() {
        return descriptionStatement;
    }

    public String getWorkflowName() {
        return workflowName;
    }

    public String getSchedulerInterval() {
        return schedulerInterval;
    }

    public boolean isPeriodic() {
        return isPeriodic;
    }

    public Map<String, String> getStaticPartitions() {
        return staticPartitions;
    }

    public Map<String, String> getExecutionConfig() {
        return executionConfig;
    }

    @Override
    public String asSummaryString() {
        return "{\n"
                + "dynamicTableIdentifier: "
                + dynamicTableIdentifier
                + ",\n descriptionStatement: "
                + descriptionStatement
                + ",\n workflowName: "
                + workflowName
                + ",\n schedulerInterval: "
                + schedulerInterval
                + ",\n isPeriodic: "
                + isPeriodic
                + ",\n staticPartitions: "
                + staticPartitions
                + ",\n executionConfig: "
                + executionConfig
                + "\n}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InMemoryRefreshHandler that = (InMemoryRefreshHandler) o;
        return isPeriodic == that.isPeriodic
                && Objects.equals(dynamicTableIdentifier, that.dynamicTableIdentifier)
                && Objects.equals(descriptionStatement, that.descriptionStatement)
                && Objects.equals(workflowName, that.workflowName)
                && Objects.equals(schedulerInterval, that.schedulerInterval)
                && Objects.equals(staticPartitions, that.staticPartitions)
                && Objects.equals(executionConfig, that.executionConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                dynamicTableIdentifier,
                descriptionStatement,
                workflowName,
                schedulerInterval,
                isPeriodic,
                staticPartitions,
                executionConfig);
    }
}
