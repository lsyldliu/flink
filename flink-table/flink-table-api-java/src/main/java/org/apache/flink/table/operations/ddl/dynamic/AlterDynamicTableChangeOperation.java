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

package org.apache.flink.table.operations.ddl.dynamic;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.dynamic.CatalogDynamicTable;

import java.util.List;

/** Alter dynamic table with new table definition and table changes represents the modification. */
@Internal
public class AlterDynamicTableChangeOperation extends AlterDynamicTableOperation {

    private final List<TableChange> tableChanges;
    private final CatalogDynamicTable newDynamicTable;

    public AlterDynamicTableChangeOperation(
            ObjectIdentifier tableIdentifier,
            List<TableChange> tableChanges,
            CatalogDynamicTable newDynamicTable) {
        super(tableIdentifier);
        this.tableChanges = tableChanges;
        this.newDynamicTable = newDynamicTable;
    }

    public List<TableChange> getTableChanges() {
        return tableChanges;
    }

    public CatalogDynamicTable getNewDynamicTable() {
        return newDynamicTable;
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        ctx.getCatalogManager()
                .alterTable(getNewDynamicTable(), getTableChanges(), getTableIdentifier(), false);
        return TableResultImpl.TABLE_RESULT_OK;
    }

    @Override
    public String asSummaryString() {
        return String.format("ALTER DYNAMIC TABLE %s\n", tableIdentifier.asSummaryString());
    }
}
