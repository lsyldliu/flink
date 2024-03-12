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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.DefaultCatalogTable;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of a {@link DynamicTable}. */
@Internal
public class DefaultDynamicTable extends DefaultCatalogTable implements DynamicTable {

    private final String definitionQuery;
    private final String freshness;

    // lazily initialization after the background is created.
    private RefreshHandler refreshJobHandler;

    public DefaultDynamicTable(
            Schema schema,
            @Nullable String comment,
            List<String> partitionKeys,
            Map<String, String> options,
            String definitionQuery,
            String freshness) {
        super(schema, comment, partitionKeys, options);
        this.definitionQuery = checkNotNull(definitionQuery, "definitionQuery must not be null.");
        this.freshness = checkNotNull(freshness, "definitionQuery must not be null.");
    }

    @Override
    public String getDefinitionQuery() {
        return definitionQuery;
    }

    @Override
    public String getFreshness() {
        return freshness;
    }

    @Override
    public RefreshHandler getRefreshJobHandler() {
        return refreshJobHandler;
    }

    @Override
    public void setRefreshJobHandler(RefreshHandler refreshJobHandler) {
        this.refreshJobHandler = refreshJobHandler;
    }
}
