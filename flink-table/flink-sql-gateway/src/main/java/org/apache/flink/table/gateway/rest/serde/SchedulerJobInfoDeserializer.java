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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JacksonException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

import static org.apache.flink.table.gateway.rest.serde.SchedulerJobInfoSerializer.FIELD_ENDPOINT;
import static org.apache.flink.table.gateway.rest.serde.SchedulerJobInfoSerializer.FIELD_SCHEDULER_TYPE;
import static org.apache.flink.table.gateway.rest.serde.SchedulerJobInfoSerializer.FIELD_WORKFLOW_ID;

/** Deserializer to deserialize {@link SchedulerJobInfo}. */
public class SchedulerJobInfoDeserializer extends StdDeserializer<SchedulerJobInfo> {

    private static final long serialVersionUID = 1L;

    protected SchedulerJobInfoDeserializer() {
        super(SchedulerJobInfo.class);
    }

    @Override
    public SchedulerJobInfo deserialize(
            JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException, JacksonException {
        ObjectNode jsonNode = jsonParser.readValueAsTree();
        String schedulerType = jsonNode.get(FIELD_SCHEDULER_TYPE).asText();
        String endpoint = jsonNode.get(FIELD_ENDPOINT).asText();
        String workflowId = jsonNode.get(FIELD_WORKFLOW_ID).asText();
        return new SchedulerJobInfo(schedulerType, endpoint, workflowId);
    }
}
