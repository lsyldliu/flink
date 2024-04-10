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
import org.apache.flink.table.catalog.dynamic.RefreshHandlerSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/** Serializer for {@link DolphinSchedulerRefreshHandler}. */
@PublicEvolving
public class DolphinSchedulerRefreshHandlerSerializer
        implements RefreshHandlerSerializer<DolphinSchedulerRefreshHandler> {

    public static final DolphinSchedulerRefreshHandlerSerializer INSTANCE =
            new DolphinSchedulerRefreshHandlerSerializer();

    @Override
    public byte[] serialize(DolphinSchedulerRefreshHandler refreshHandler) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            outputStream.writeUTF(refreshHandler.getSchedulerType());
            outputStream.writeUTF(refreshHandler.getEndpoint());
            outputStream.writeUTF(refreshHandler.getWorkflowId());
        }
        return byteArrayOutputStream.toByteArray();
    }

    @Override
    public DolphinSchedulerRefreshHandler deserialize(byte[] serializedBytes) throws IOException {
        try (ObjectInputStream inputStream =
                new ObjectInputStream(new ByteArrayInputStream(serializedBytes))) {
            String type = inputStream.readUTF();
            String endpoint = inputStream.readUTF();
            String workflowId = inputStream.readUTF();
            return new DolphinSchedulerRefreshHandler(type, endpoint, workflowId);
        }
    }
}
