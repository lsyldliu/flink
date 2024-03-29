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

package org.apache.flink.table.catalog.dynamic;

import org.apache.flink.annotation.Internal;

/** Scheduler job info. */
@Internal
public class SchedulerJobInfo {

    private final String schedulerType;
    private final String endpoint;
    private final String workflowId;

    public SchedulerJobInfo(String schedulerType, String endpoint, String workflowId) {
        this.schedulerType = schedulerType;
        this.endpoint = endpoint;
        this.workflowId = workflowId;
    }

    public String getSchedulerType() {
        return schedulerType;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getWorkflowId() {
        return workflowId;
    }
}
