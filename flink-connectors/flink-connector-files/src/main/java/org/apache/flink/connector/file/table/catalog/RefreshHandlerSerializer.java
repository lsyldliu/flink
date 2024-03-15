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

package org.apache.flink.connector.file.table.catalog;

import org.apache.flink.table.catalog.dynamic.CatalogDynamicTable;
import org.apache.flink.table.catalog.dynamic.RefreshHandler;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

import static org.apache.flink.connector.file.table.catalog.JsonSerdeUtil.traverse;

/** A {@link JsonSerializer} for {@link RefreshHandlerSerializer}. */
public class RefreshHandlerSerializer
        implements JsonSerializer<RefreshHandler>, JsonDeserializer<RefreshHandler> {

    public static final RefreshHandlerSerializer INSTANCE = new RefreshHandlerSerializer();

    @Override
    public RefreshHandler deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException {
        JsonNode jsonNode = jsonParser.readValueAsTree();

        JsonNode refreshModeNode = jsonNode.get("actualRefreshMode");
        CatalogDynamicTable.RefreshMode refreshMode =
                ctx.readValue(
                        traverse(refreshModeNode, jsonParser.getCodec()),
                        CatalogDynamicTable.RefreshMode.class);
        JsonNode stateNode = jsonNode.get("state");
        RefreshHandler.State state =
                ctx.readValue(
                        traverse(stateNode, jsonParser.getCodec()), RefreshHandler.State.class);
        JsonNode detailNode = jsonNode.get("detail");
        String detail = null;
        if (detailNode != null) {
            detail = detailNode.asText();
        }
        return new RefreshHandler(refreshMode, state, detail);
    }

    @Override
    public void serialize(
            RefreshHandler refreshHandler, JsonGenerator generator, SerializerProvider provider)
            throws IOException {
        generator.writeStartObject();
        generator.writeObjectField("actualRefreshMode", refreshHandler.getActualRefreshMode());
        generator.writeObjectField("state", refreshHandler.getState());
        if (refreshHandler.getDetail() != null) {
            generator.writeStringField("detail", refreshHandler.getDetail());
        }
        generator.writeEndObject();
    }
}
