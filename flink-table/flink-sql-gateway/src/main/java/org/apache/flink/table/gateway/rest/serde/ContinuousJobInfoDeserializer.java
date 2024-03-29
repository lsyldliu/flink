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

import org.apache.flink.table.catalog.dynamic.ContinuousJobInfo;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JacksonException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

import static org.apache.flink.table.gateway.rest.serde.ContinuousJobInfoSerializer.FIELD_CLUSTER_ID;
import static org.apache.flink.table.gateway.rest.serde.ContinuousJobInfoSerializer.FIELD_EXECUTION_TARGET;
import static org.apache.flink.table.gateway.rest.serde.ContinuousJobInfoSerializer.FIELD_JOB_ID;

/** Deserializer to deserialize {@link ContinuousJobInfo}. */
public class ContinuousJobInfoDeserializer extends StdDeserializer<ContinuousJobInfo> {

    private static final long serialVersionUID = 1L;

    protected ContinuousJobInfoDeserializer() {
        super(ContinuousJobInfo.class);
    }

    @Override
    public ContinuousJobInfo deserialize(
            JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException, JacksonException {
        ObjectNode jsonNode = jsonParser.readValueAsTree();
        String executionTarget = jsonNode.get(FIELD_EXECUTION_TARGET).asText();
        String clusterId = jsonNode.get(FIELD_CLUSTER_ID).asText();
        String jobId = jsonNode.get(FIELD_JOB_ID).asText();
        return new ContinuousJobInfo(executionTarget, clusterId, jobId);
    }
}
