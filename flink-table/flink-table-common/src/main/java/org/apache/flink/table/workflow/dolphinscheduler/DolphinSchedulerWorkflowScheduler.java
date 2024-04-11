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

package org.apache.flink.table.workflow.dolphinscheduler;

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.dynamic.RefreshHandlerSerializer;
import org.apache.flink.table.workflow.WorkflowScheduler;

import java.util.Map;

/** DolphinScheduler workflow scheduler implementation. */
public class DolphinSchedulerWorkflowScheduler
        implements WorkflowScheduler<DolphinSchedulerRefreshHandler> {

    private final String userName;
    private final String password;
    private final String endpoint;
    private final String projectName;

    public DolphinSchedulerWorkflowScheduler(
            String userName, String password, String endpoint, String projectName) {
        this.userName = userName;
        this.password = password;
        this.endpoint = endpoint;
        this.projectName = projectName;
    }

    @Override
    public RefreshHandlerSerializer createSerializer() {
        return DolphinSchedulerRefreshHandlerSerializer.INSTANCE;
    }

    @Override
    public DolphinSchedulerRefreshHandler createRefreshWorkflow(
            ObjectIdentifier dynamicTableIdentifier,
            String refreshStatement,
            String workflowNamePrefix,
            String refreshCron,
            boolean isPeriodic,
            String triggerUrl,
            Map<String, String> staticPartitions,
            Map<String, String> executionConf) {
        return null;
    }

    @Override
    public boolean suspendRefreshWorkflow(DolphinSchedulerRefreshHandler refreshHandler) {
        return false;
    }

    @Override
    public boolean resumeRefreshWorkflow(
            DolphinSchedulerRefreshHandler refreshHandler,
            Map<String, String> updatedExecutionConf) {
        return false;
    }

    @Override
    public boolean deleteRefreshWorkflow(DolphinSchedulerRefreshHandler refreshHandler) {
        return false;
    }
}
