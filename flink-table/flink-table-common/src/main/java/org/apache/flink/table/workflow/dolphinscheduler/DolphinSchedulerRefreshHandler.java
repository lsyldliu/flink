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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.dynamic.RefreshHandler;

/** The refresh handler for dolphin scheduler. */
@PublicEvolving
public class DolphinSchedulerRefreshHandler implements RefreshHandler {

    private final String type;
    private final String endpoint;
    private final String workflowId;

    public DolphinSchedulerRefreshHandler(String type, String endpoint, String workflowId) {
        this.type = type;
        this.endpoint = endpoint;
        this.workflowId = workflowId;
    }

    public String getSchedulerType() {
        return type;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getWorkflowId() {
        return workflowId;
    }

    @Override
    public String asSummaryString() {
        return String.format(
                "{\ntype: %s,\n endpoint: %s,\n workflowId: %s\n}", type, endpoint, workflowId);
    }
}
