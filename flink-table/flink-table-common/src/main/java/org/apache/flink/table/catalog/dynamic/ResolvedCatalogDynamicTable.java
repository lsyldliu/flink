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
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A validated {@link CatalogDynamicTable} that is backed by the original metadata coming from the
 * {@link Catalog} but resolved by the framework.
 *
 * <p>Note: This will be converted to {@link ResolvedCatalogTable} by framework during planner
 * optimize query phase.
 */
@PublicEvolving
public class ResolvedCatalogDynamicTable
        implements ResolvedCatalogBaseTable<CatalogDynamicTable>, CatalogDynamicTable {

    private final CatalogDynamicTable origin;

    private final ResolvedSchema resolvedSchema;

    public ResolvedCatalogDynamicTable(CatalogDynamicTable origin, ResolvedSchema resolvedSchema) {
        this.origin =
                Preconditions.checkNotNull(
                        origin, "Original catalog dynamic table must not be null.");
        this.resolvedSchema =
                Preconditions.checkNotNull(resolvedSchema, "Resolved schema must not be null.");
    }

    @Override
    public Map<String, String> getOptions() {
        return origin.getOptions();
    }

    @Override
    public String getComment() {
        return origin.getComment();
    }

    @Override
    public CatalogBaseTable copy() {
        return new ResolvedCatalogDynamicTable((CatalogDynamicTable) origin.copy(), resolvedSchema);
    }

    @Override
    public Optional<String> getDescription() {
        return origin.getDescription();
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return origin.getDetailedDescription();
    }

    @Override
    public boolean isPartitioned() {
        return origin.isPartitioned();
    }

    @Override
    public List<String> getPartitionKeys() {
        return origin.getPartitionKeys();
    }

    @Override
    public ResolvedCatalogDynamicTable copy(Map<String, String> options) {
        return new ResolvedCatalogDynamicTable(origin.copy(options), resolvedSchema);
    }

    @Override
    public CatalogDynamicTable getOrigin() {
        return origin;
    }

    @Override
    public ResolvedSchema getResolvedSchema() {
        return resolvedSchema;
    }

    @Override
    public String getDefinitionQuery() {
        return origin.getDefinitionQuery();
    }

    @Override
    public Duration getFreshness() {
        return origin.getFreshness();
    }

    @Override
    public Optional<RefreshMode> getRefreshMode() {
        return origin.getRefreshMode();
    }

    @Override
    public RefreshHandler getRefreshJobHandler() {
        return origin.getRefreshJobHandler();
    }

    public ResolvedCatalogTable toResolvedCatalogTable() {
        return new ResolvedCatalogTable(
                CatalogTable.of(
                        getUnresolvedSchema(),
                        getComment(),
                        getPartitionKeys(),
                        getOptions(),
                        null),
                getResolvedSchema());
    }
}
