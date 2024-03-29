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

package org.apache.flink.table.gateway.rest.serde;

import org.apache.flink.table.catalog.dynamic.SchedulerJobInfo;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/** Serializer to serialize {@link SchedulerJobInfo}. */
public class SchedulerJobInfoSerializer extends StdSerializer<SchedulerJobInfo> {

    private static final long serialVersionUID = 1L;

    public static final String FIELD_SCHEDULER_TYPE = "schedulerType";
    public static final String FIELD_ENDPOINT = "endpoint";
    public static final String FIELD_WORKFLOW_ID = "workflowId";

    protected SchedulerJobInfoSerializer() {
        super(SchedulerJobInfo.class);
    }

    @Override
    public void serialize(
            SchedulerJobInfo schedulerJobInfo,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField(FIELD_SCHEDULER_TYPE, schedulerJobInfo.getSchedulerType());
        jsonGenerator.writeStringField(FIELD_ENDPOINT, schedulerJobInfo.getEndpoint());
        jsonGenerator.writeStringField(FIELD_WORKFLOW_ID, schedulerJobInfo.getWorkflowId());
        jsonGenerator.writeEndObject();
    }
}
