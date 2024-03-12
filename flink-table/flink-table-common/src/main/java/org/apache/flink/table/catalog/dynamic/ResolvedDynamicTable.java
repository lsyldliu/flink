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
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A validated {@link DynamicTable} that is backed by the original metadata coming from the {@link
 * Catalog} but resolved by the framework.
 */
@PublicEvolving
public final class ResolvedDynamicTable
        implements ResolvedCatalogBaseTable<DynamicTable>, DynamicTable {

    private final DynamicTable origin;

    private final ResolvedSchema resolvedSchema;

    public ResolvedDynamicTable(DynamicTable origin, ResolvedSchema resolvedSchema) {
        this.origin =
                Preconditions.checkNotNull(origin, "Original catalog table must not be null.");
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
        return new ResolvedDynamicTable((DynamicTable) origin.copy(), resolvedSchema);
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
    public CatalogTable copy(Map<String, String> options) {
        return new ResolvedDynamicTable((DynamicTable) origin.copy(options), resolvedSchema);
    }

    @Override
    public DynamicTable getOrigin() {
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
    public String getFreshness() {
        return origin.getFreshness();
    }

    @Override
    public RefreshHandler getRefreshJobHandler() {
        return origin.getRefreshJobHandler();
    }

    @Override
    public void setRefreshJobHandler(RefreshHandler refreshJobHandler) {
        origin.setRefreshJobHandler(refreshJobHandler);
    }

    @Override
    public Map<String, String> toProperties() {
        return Collections.emptyMap();
    }
}
