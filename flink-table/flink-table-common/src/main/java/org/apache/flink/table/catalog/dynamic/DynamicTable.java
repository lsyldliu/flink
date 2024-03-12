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
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Dynamic Table definition: In the context of integrated stream-batch data storage, it provides
 * full history data and incremental Changelog. By defining the data's production business logic and
 * freshness, updates are achieved through streaming incremental updates or batch-based scheduled
 * updates, while also possessing the capability for both batch and incremental consumption.
 *
 * <p>Represents the unresolved metadata of a dynamic table in a {@link Catalog}. Based on the
 * {@link CatalogTable}, the metadata for {@link DynamicTable} also includes the following three
 * parts:
 *
 * <ul>
 *   <li>Data freshness, which determines when the data is generated and becomes visible for user.
 *   <li>Data production business logic, also known as the definition query.
 *   <li>Background data refresh job, either through streaming increments or batch-based scheduled
 *       refresh job, it is initialized lazily after table is created.
 * </ul>
 *
 * <p>A catalog implementer can either use {@link #of(Schema, String, List, Map, String, String)}
 * for a basic implementation of this interface or create a custom class that allows passing
 * catalog-specific objects all the way down to the connector creation (if necessary).
 */
@PublicEvolving
public interface DynamicTable extends CatalogTable {

    /**
     * Creates an instance of {@link DynamicTable}.
     *
     * @param schema unresolved schema
     * @param comment optional comment
     * @param partitionKeys list of partition keys or an empty list if not partitioned
     * @param options options to configure the connector
     * @param definitionQuery definition sql query
     * @param freshness data freshness
     */
    static DynamicTable of(
            Schema schema,
            @Nullable String comment,
            List<String> partitionKeys,
            Map<String, String> options,
            String definitionQuery,
            String freshness) {
        return new DefaultDynamicTable(
                schema, comment, partitionKeys, options, definitionQuery, freshness);
    }

    @Override
    default TableKind getTableKind() {
        return TableKind.DYNAMIC_TABLE;
    }

    /**
     * The definition query text of dynamic table, text is expanded in contrast to the original SQL.
     * This is needed because the context such as current DB is lost after the session, in which
     * view is defined, is gone. Expanded query text takes care of this, as an example.
     *
     * <p>For example, for a dynamic table that is defined in the context of "default" database with
     * a query {@code select * from test1}, the expanded query text might become {@code select
     * `test1`.`name`, `test1`.`value` from `default`.`test1`}, where table test1 resides in
     * database "default" and has two columns ("name" and "value").
     *
     * @return the dynamic table definition in expanded text.
     */
    String getDefinitionQuery();

    /** Get the freshness of dynamic table which is used to determine the data refresh mode. */
    String getFreshness();

    /**
     * Return the background refresh job info of current dynamic table. Note the refresh job is
     * initialized lazily after table is created.
     */
    RefreshHandler getRefreshJobHandler();

    /**
     * Update the background refresh job info of current dynamic table. Note the refresh job is
     * initialized lazily after table is created.
     */
    void setRefreshJobHandler(RefreshHandler refreshJobInfo);
}
