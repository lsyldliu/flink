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

package org.apache.flink.table.workflow;

import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;

import java.time.Duration;
import java.util.Map;

/**
 * {@link CreateRefreshWorkflow} provides the related information to create periodic refresh
 * workflow of {@link CatalogMaterializedTable}.
 */
public class CreatePeriodicRefreshWorkflow implements CreateRefreshWorkflow {

    private final ObjectIdentifier materializedTableIdentifier;
    private final String descriptionStatement;
    private final String workflowName;
    private final String cronExpr;
    private final Duration freshness;
    private final Map<String, String> executionConfig;

    public CreatePeriodicRefreshWorkflow(
            ObjectIdentifier materializedTableIdentifier,
            String descriptionStatement,
            String workflowName,
            String cronExpr,
            Duration freshness,
            Map<String, String> executionConfig) {
        this.materializedTableIdentifier = materializedTableIdentifier;
        this.descriptionStatement = descriptionStatement;
        this.workflowName = workflowName;
        this.cronExpr = cronExpr;
        this.freshness = freshness;
        this.executionConfig = executionConfig;
    }

    public ObjectIdentifier getMaterializedTableIdentifier() {
        return materializedTableIdentifier;
    }

    public String getDescriptionStatement() {
        return descriptionStatement;
    }

    public String getWorkflowName() {
        return workflowName;
    }

    public String getCronExpr() {
        return cronExpr;
    }

    public Duration getFreshness() {
        return freshness;
    }

    public Map<String, String> getExecutionConfig() {
        return executionConfig;
    }
}
