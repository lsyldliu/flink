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

import org.apache.flink.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * This interface represents the meta information of current dynamic table background refresh job.
 * The refresh mode of background job maybe continuous or full. The format of the meta information
 * in the two modes is not consistent, so we unify it by a structured json string jobDetail.
 *
 * <p>In continuous mode, the format of the meta information is { "clusterType": "yarn",
 * "clusterId": "xxx", "jobId": "yyyy" }.
 *
 * <p>In full mode, the meta information format is { "schedulerType": "airflow", "endpoint": "xxx",
 * "workflowId": "yyy" }.
 */
@PublicEvolving
public class RefreshHandler {

    private final CatalogDynamicTable.RefreshMode actualRefreshMode;
    private final JobState jobState;
    private final @Nullable String jobDetail;

    public RefreshHandler(
            CatalogDynamicTable.RefreshMode actualRefreshMode,
            JobState jobState,
            @Nullable String jobDetail) {
        this.actualRefreshMode = actualRefreshMode;
        this.jobState = jobState;
        this.jobDetail = jobDetail;
    }

    public CatalogDynamicTable.RefreshMode getActualRefreshMode() {
        return actualRefreshMode;
    }

    public JobState getJobState() {
        return jobState;
    }

    public String getJobDetail() {
        return jobDetail;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RefreshHandler that = (RefreshHandler) o;
        return actualRefreshMode == that.actualRefreshMode
                && jobState == that.jobState
                && Objects.equals(jobDetail, that.jobDetail);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actualRefreshMode, jobState, jobDetail);
    }

    @Override
    public String toString() {
        return "RefreshJobHandler{"
                + "actualRefreshMode="
                + actualRefreshMode
                + ", jobState="
                + jobState
                + ", jobDetail='"
                + jobDetail
                + '\''
                + '}';
    }

    /** Background refresh job state. */
    @PublicEvolving
    public enum JobState {
        INITIALIZING,
        ACTIVATED,
        SUSPENDED
    }
}
