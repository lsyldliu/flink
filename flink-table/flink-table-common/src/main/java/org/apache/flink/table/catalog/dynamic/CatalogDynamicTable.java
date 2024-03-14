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
import org.apache.flink.table.catalog.CatalogBaseTable;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Represents the unresolved metadata of a dynamic table in a {@link Catalog}.
 *
 * <p>Dynamic Table definition: In the context of integrated stream-batch data storage, it provides
 * full history data and incremental changelog. By defining the data's production business logic and
 * freshness, data update is achieved through continuous or full refresh mode, while also possessing
 * the capability for both batch and incremental consumption.
 *
 * <p>The metadata for {@link CatalogDynamicTable} also includes the following four main parts:
 *
 * <ul>
 *   <li>Schema, comments, options and partition keys.
 *   <li>Data freshness, which determines when the data is generated and becomes visible for user.
 *   <li>Data production business logic, also known as the definition query.
 *   <li>Background data refresh job, either through a flink streaming or scheduled batch job, it is
 *       initialized after dynamic table is created.
 * </ul>
 *
 * <p>A catalog implementer can either use {@link #of(Schema, String, List, Map, Long, String,
 * Duration, RefreshMode, RefreshHandler)} for a basic implementation of this interface or create a
 * custom class that allows passing catalog-specific objects all the way down to the connector
 * creation (if necessary).
 */
@PublicEvolving
public interface CatalogDynamicTable extends CatalogBaseTable {

    /**
     * Creates an instance of {@link CatalogDynamicTable}.
     *
     * @param schema unresolved schema
     * @param comment optional comment
     * @param partitionKeys list of partition keys or an empty list if not partitioned
     * @param options options to configure the connector
     * @param snapshot table snapshot of the table
     * @param definitionQuery definition sql query
     * @param freshness data freshness
     * @param refreshMode data refresh mode
     * @param refreshJobHandler refresh job handler
     */
    static CatalogDynamicTable of(
            Schema schema,
            @Nullable String comment,
            List<String> partitionKeys,
            Map<String, String> options,
            @Nullable Long snapshot,
            String definitionQuery,
            Duration freshness,
            @Nullable RefreshMode refreshMode,
            RefreshHandler refreshJobHandler) {
        return new DefaultCatalogDynamicTable(
                schema,
                comment,
                partitionKeys,
                options,
                snapshot,
                definitionQuery,
                freshness,
                refreshMode,
                refreshJobHandler);
    }

    @Override
    default TableKind getTableKind() {
        return TableKind.DYNAMIC_TABLE;
    }

    /**
     * Check if the table is partitioned or not.
     *
     * @return true if the table is partitioned; otherwise, false
     */
    boolean isPartitioned();

    /**
     * Get the partition keys of the table. This will be an empty set if the table is not
     * partitioned.
     *
     * @return partition keys of the table
     */
    List<String> getPartitionKeys();

    /**
     * Returns a copy of this {@code CatalogDynamicTable} with given table options {@code options}.
     *
     * @return a new copy of this table with replaced table options
     */
    CatalogDynamicTable copy(Map<String, String> options);

    /** Return the snapshot specified for the table. Return Optional.empty() if not specified. */
    default Optional<Long> getSnapshot() {
        return Optional.empty();
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
    Duration getFreshness();

    /** Get the refresh mode of dynamic table. */
    Optional<RefreshMode> getRefreshMode();

    /** Get the refresh job handler of dynamic table. */
    RefreshHandler getRefreshJobHandler();

    /** The refresh mode of dynamic table. */
    @PublicEvolving
    enum RefreshMode {
        CONTINUOUS,
        FULL
    }
}
