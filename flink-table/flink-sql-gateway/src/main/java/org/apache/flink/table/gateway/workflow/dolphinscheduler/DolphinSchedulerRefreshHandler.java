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

package org.apache.flink.table.gateway.workflow.dolphinscheduler;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.refresh.RefreshHandler;

import java.io.Serializable;
import java.util.Objects;

/** A {@link RefreshHandler} instance for dolphinscheduler. */
@PublicEvolving
public class DolphinSchedulerRefreshHandler implements RefreshHandler, Serializable {

    private final long projectCode;
    private final long workflowCode;
    private final String workflowName;
    private final long taskCode;
    private final int schedulerId;

    public DolphinSchedulerRefreshHandler(
            long projectCode,
            long workflowCode,
            String workflowName,
            long taskCode,
            int schedulerId) {
        this.projectCode = projectCode;
        this.workflowCode = workflowCode;
        this.workflowName = workflowName;
        this.taskCode = taskCode;
        this.schedulerId = schedulerId;
    }

    public long getProjectCode() {
        return projectCode;
    }

    public long getWorkflowCode() {
        return workflowCode;
    }

    public String getWorkflowName() {
        return workflowName;
    }

    public long getTaskCode() {
        return taskCode;
    }

    public int getSchedulerId() {
        return schedulerId;
    }

    @Override
    public String asSummaryString() {
        return JSONUtils.toJsonString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DolphinSchedulerRefreshHandler that = (DolphinSchedulerRefreshHandler) o;
        return projectCode == that.projectCode
                && workflowCode == that.workflowCode
                && taskCode == that.taskCode
                && schedulerId == that.schedulerId
                && Objects.equals(workflowName, that.workflowName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(projectCode, workflowCode, workflowName, taskCode, schedulerId);
    }

    @Override
    public String toString() {
        return "DolphinSchedulerRefreshHandler{"
                + "projectCode="
                + projectCode
                + ", workflowCode="
                + workflowCode
                + ", workflowName='"
                + workflowName
                + '\''
                + ", taskCode="
                + taskCode
                + ", schedulerId="
                + schedulerId
                + '}';
    }
}
