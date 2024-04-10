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

package org.apache.flink.table.gateway.rest.message.dynamic.table;

import org.apache.flink.runtime.rest.messages.RequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Map;

/** {@link RequestBody} for refresh dynamic table. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DynamicTableRefreshRequestBody implements RequestBody {

    private static final String FIELD_NAME_DYNAMIC_TABLE = "dynamicTable";
    private static final String FIELD_NAME_REFRESH_STATEMENT = "refreshStatement";
    private static final String FIELD_NAME_IS_PERIODIC = "isPeriodic";
    private static final String FIELD_NAME_SCHEDULER_TIME = "schedulerTime";
    private static final String FIELD_NAME_SCHEDULER_TIME_FORMAT = "schedulerTimeFormat";
    private static final String FIELD_NAME_EXECUTION_CONFIG = "executionConfig";

    @JsonProperty(FIELD_NAME_DYNAMIC_TABLE)
    private final String dynamicTable;

    @JsonProperty(FIELD_NAME_REFRESH_STATEMENT)
    private final String refreshStatement;

    @JsonProperty(FIELD_NAME_IS_PERIODIC)
    private final boolean isPeriodic;

    @JsonProperty(FIELD_NAME_SCHEDULER_TIME)
    @Nullable
    private final String schedulerTime;

    @JsonProperty(FIELD_NAME_SCHEDULER_TIME_FORMAT)
    @Nullable
    private final String schedulerTimeFormat;

    @JsonProperty(FIELD_NAME_EXECUTION_CONFIG)
    @Nullable
    private final Map<String, String> executionConfig;

    @JsonCreator
    public DynamicTableRefreshRequestBody(
            @JsonProperty(FIELD_NAME_DYNAMIC_TABLE) String dynamicTable,
            @JsonProperty(FIELD_NAME_REFRESH_STATEMENT) String refreshStatement,
            @JsonProperty(FIELD_NAME_IS_PERIODIC) boolean isPeriodic,
            @JsonProperty(FIELD_NAME_SCHEDULER_TIME) @Nullable String schedulerTime,
            @JsonProperty(FIELD_NAME_SCHEDULER_TIME_FORMAT) @Nullable String schedulerTimeFormat,
            @JsonProperty(FIELD_NAME_EXECUTION_CONFIG) @Nullable
                    Map<String, String> executionConfig) {
        this.dynamicTable = dynamicTable;
        this.refreshStatement = refreshStatement;
        this.isPeriodic = isPeriodic;
        this.schedulerTime = schedulerTime;
        this.schedulerTimeFormat = schedulerTimeFormat;
        this.executionConfig = executionConfig;
    }

    public String getDynamicTable() {
        return dynamicTable;
    }

    public String getRefreshStatement() {
        return refreshStatement;
    }

    public boolean isPeriodic() {
        return isPeriodic;
    }

    @Nullable
    public String getSchedulerTime() {
        return schedulerTime;
    }

    @Nullable
    public String getSchedulerTimeFormat() {
        return schedulerTimeFormat;
    }

    @Nullable
    public Map<String, String> getExecutionConfig() {
        return executionConfig;
    }
}
