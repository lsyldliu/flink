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
import org.apache.flink.table.catalog.dynamic.RefreshHandler;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;

/** Table Schema info. */
public class TableSchema implements Serializable {

    private final CatalogBaseTable.TableKind tableKind;
    private final String schema;
    private final @Nullable String comment;
    private final Map<String, String> options;
    private final @Nullable String pkConstraintName;
    private final @Nullable String pkColumns;
    private final @Nullable String partitionColumns;
    private final String definitionQuery;
    private final Duration freshness;
    private final @Nullable CatalogDynamicTable.RefreshMode refreshMode;
    private final RefreshHandler refreshHandler;

    public TableSchema(
            CatalogBaseTable.TableKind tableKind,
            String schema,
            String comment,
            Map<String, String> options,
            String pkConstraintName,
            String pkColumns,
            String partitionColumns,
            String definitionQuery,
            Duration freshness,
            CatalogDynamicTable.RefreshMode refreshMode,
            RefreshHandler refreshHandler) {
        this.tableKind = tableKind;
        this.schema = schema;
        this.comment = comment;
        this.options = options;
        this.pkConstraintName = pkConstraintName;
        this.pkColumns = pkColumns;
        this.partitionColumns = partitionColumns;
        this.definitionQuery = definitionQuery;
        this.freshness = freshness;
        this.refreshMode = refreshMode;
        this.refreshHandler = refreshHandler;
    }

    public String getSchema() {
        return schema;
    }

    public String getComment() {
        return comment;
    }

    public CatalogBaseTable.TableKind getTableKind() {
        return tableKind;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public String getPkConstraintName() {
        return pkConstraintName;
    }

    public String getPkColumns() {
        return pkColumns;
    }

    public String getPartitionColumns() {
        return partitionColumns;
    }

    public String getDefinitionQuery() {
        return definitionQuery;
    }

    public Duration getFreshness() {
        return freshness;
    }

    public CatalogDynamicTable.RefreshMode getRefreshMode() {
        return refreshMode;
    }

    public RefreshHandler getRefreshHandler() {
        return refreshHandler;
    }
}
