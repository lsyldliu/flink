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

import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.dynamic.CatalogDynamicTable;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.flink.connector.file.table.catalog.JsonSerdeUtil.traverse;

/** A {@link JsonSerializer} for {@link TableSchema}. */
public class TableSchemaSerializer
        implements JsonSerializer<TableSchema>, JsonDeserializer<TableSchema> {

    public static final TableSchemaSerializer INSTANCE = new TableSchemaSerializer();

    @Override
    public TableSchema deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException {
        ObjectNode jsonNode = jsonParser.readValueAsTree();
        JsonNode tableKindNode = jsonNode.get("tableKind");
        CatalogBaseTable.TableKind tableKind =
                ctx.readValue(
                        traverse(tableKindNode, jsonParser.getCodec()),
                        CatalogBaseTable.TableKind.class);

        String schema = jsonNode.get("schema").asText();
        JsonNode commentNode = jsonNode.get("comment");
        String comment = null;
        if (commentNode != null) {
            comment = commentNode.asText();
        }
        JsonNode optionsJson = jsonNode.get("options");
        Map<String, String> options = new HashMap<>();
        Iterator<String> optionsKeys = optionsJson.fieldNames();
        while (optionsKeys.hasNext()) {
            String key = optionsKeys.next();
            options.put(key, optionsJson.get(key).asText());
        }

        JsonNode pkConstraintNameNode = jsonNode.get("pkConstraintName");
        String pkConstraintName = null;
        if (pkConstraintNameNode != null) {
            pkConstraintName = pkConstraintNameNode.asText();
        }
        JsonNode pkColumnsNode = jsonNode.get("pkColumns");
        String pkColumns = null;
        if (pkColumnsNode != null) {
            pkColumns = pkColumnsNode.asText();
        }
        JsonNode partitionColumnsNode = jsonNode.get("partitionColumns");
        String partitionColumns = null;
        if (partitionColumnsNode != null) {
            partitionColumns = partitionColumnsNode.asText();
        }

        String definitionQuery = null;
        Duration freshness = null;
        CatalogDynamicTable.LogicalRefreshMode logicalRefreshMode = null;
        CatalogDynamicTable.RefreshMode refreshMode = null;
        CatalogDynamicTable.RefreshStatus refreshStatus = null;
        String refreshHandlerDesc = null;
        byte[] serializedRefreshHandler = null;
        if (tableKind == CatalogBaseTable.TableKind.DYNAMIC_TABLE) {
            definitionQuery = jsonNode.get("definitionQuery").asText();

            JsonNode freshnessNode = jsonNode.get("freshness");
            freshness =
                    ctx.readValue(traverse(freshnessNode, jsonParser.getCodec()), Duration.class);

            JsonNode logicalRefreshModeNode = jsonNode.get("logicalRefreshMode");
            logicalRefreshMode =
                    ctx.readValue(
                            traverse(logicalRefreshModeNode, jsonParser.getCodec()),
                            CatalogDynamicTable.LogicalRefreshMode.class);

            JsonNode refreshModeNode = jsonNode.get("refreshMode");
            refreshMode =
                    ctx.readValue(
                            traverse(refreshModeNode, jsonParser.getCodec()),
                            CatalogDynamicTable.RefreshMode.class);

            JsonNode refreshStatusNode = jsonNode.get("refreshStatus");
            refreshStatus =
                    ctx.readValue(
                            traverse(refreshStatusNode, jsonParser.getCodec()),
                            CatalogDynamicTable.RefreshStatus.class);
            JsonNode refreshHandlerDescNode = jsonNode.get("refreshHandlerDescription");
            if (refreshHandlerDescNode != null) {
                refreshHandlerDesc = refreshHandlerDescNode.asText();
            }

            JsonNode serializedRefreshHandlerNode = jsonNode.get("serializedRefreshHandler");
            serializedRefreshHandler =
                    ctx.readValue(
                            traverse(serializedRefreshHandlerNode, jsonParser.getCodec()),
                            byte[].class);
        }

        return new TableSchema(
                tableKind,
                schema,
                comment,
                options,
                pkConstraintName,
                pkColumns,
                partitionColumns,
                definitionQuery,
                freshness,
                logicalRefreshMode,
                refreshMode,
                refreshStatus,
                refreshHandlerDesc,
                serializedRefreshHandler);
    }

    @Override
    public void serialize(
            TableSchema tableSchema, JsonGenerator generator, SerializerProvider provider)
            throws IOException {
        generator.writeStartObject();

        generator.writeObjectField("tableKind", tableSchema.getTableKind());
        generator.writeStringField("schema", tableSchema.getSchema());
        if (tableSchema.getComment() != null) {
            generator.writeStringField("comment", tableSchema.getComment());
        }

        generator.writeObjectFieldStart("options");
        for (Map.Entry<String, String> entry : tableSchema.getOptions().entrySet()) {
            generator.writeStringField(entry.getKey(), entry.getValue());
        }
        generator.writeEndObject();

        if (tableSchema.getPkConstraintName() != null) {
            generator.writeStringField("pkConstraintName", tableSchema.getPkConstraintName());
            generator.writeStringField("pkColumns", tableSchema.getPkColumns());
        }

        if (tableSchema.getPartitionColumns() != null) {
            generator.writeStringField("partitionColumns", tableSchema.getPartitionColumns());
        }

        if (tableSchema.getTableKind() == CatalogBaseTable.TableKind.DYNAMIC_TABLE) {
            generator.writeStringField("definitionQuery", tableSchema.getDefinitionQuery());
            generator.writeObjectField("freshness", tableSchema.getFreshness());
            generator.writeObjectField("logicalRefreshMode", tableSchema.getLogicalRefreshMode());
            generator.writeObjectField("refreshMode", tableSchema.getRefreshMode());
            generator.writeObjectField("refreshStatus", tableSchema.getRefreshStatus());
            if (tableSchema.getRefreshHandlerDescription() != null) {
                generator.writeStringField(
                        "refreshHandlerDescription", tableSchema.getRefreshHandlerDescription());
            }
            generator.writeBinaryField(
                    "serializedRefreshHandler", tableSchema.getSerializedRefreshHandler());

            generator.writeEndObject();
        }
    }
}
