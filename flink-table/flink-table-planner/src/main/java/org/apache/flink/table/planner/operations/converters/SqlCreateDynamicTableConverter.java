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

package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.ddl.dynamic.SqlCreateDynamicTable;
import org.apache.flink.sql.parser.ddl.dynamic.SqlRefreshMode;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.catalog.dynamic.CatalogDynamicTable;
import org.apache.flink.table.catalog.dynamic.RefreshHandler;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.ddl.dynamic.CreateDynamicTableOperation;
import org.apache.flink.table.planner.operations.DynamicTableUtil;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.utils.OperationConverterUtils;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** A converter for {@link SqlCreateDynamicTable}. */
public class SqlCreateDynamicTableConverter implements SqlNodeConverter<SqlCreateDynamicTable> {

    @Override
    public Operation convertSqlNode(
            SqlCreateDynamicTable sqlCreateDynamicTable, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlCreateDynamicTable.fullTableName());
        CatalogManager catalogManager = context.getCatalogManager();
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

        // get table comment
        String tableComment =
                OperationConverterUtils.getTableComment(sqlCreateDynamicTable.getComment());

        // get partition key
        List<String> partitionKeys =
                sqlCreateDynamicTable.getPartitionKeyList().getList().stream()
                        .map(p -> ((SqlIdentifier) p).getSimple())
                        .collect(Collectors.toList());

        // get table properties
        Map<String, String> properties = new HashMap<>();
        sqlCreateDynamicTable
                .getPropertyList()
                .getList()
                .forEach(
                        p ->
                                properties.put(
                                        ((SqlTableOption) p).getKeyString(),
                                        ((SqlTableOption) p).getValueString()));

        // get freshness
        Duration freshness =
                DynamicTableUtil.getDynamicTableFreshness(sqlCreateDynamicTable.getFreshness());

        // get refresh mode
        CatalogDynamicTable.RefreshMode refreshMode = null;
        if (sqlCreateDynamicTable.getRefreshMode().isPresent()) {
            SqlRefreshMode sqlRefreshMode =
                    sqlCreateDynamicTable.getRefreshMode().get().getValueAs(SqlRefreshMode.class);
            refreshMode = DynamicTableUtil.getRefreshMode(sqlRefreshMode);
        }

        // get query schema
        SqlNode validateQuery =
                context.getSqlValidator().validate(sqlCreateDynamicTable.getAsQuery());
        String expandedQuery =
                context.expandSqlIdentifiers(
                        context.toQuotedSqlString(sqlCreateDynamicTable.getAsQuery()));

        QueryOperation operation =
                new PlannerQueryOperation(
                        context.toRelRoot(validateQuery).project(),
                        () -> context.toQuotedSqlString(validateQuery));

        Schema.Builder builder =
                Schema.newBuilder().fromResolvedSchema(operation.getResolvedSchema());
        // get primary key
        Optional<SqlTableConstraint> sqlTableConstraint =
                sqlCreateDynamicTable.getTableConstraint();
        if (sqlTableConstraint.isPresent()) {
            List<String> primaryKeyColumns =
                    Arrays.asList(sqlTableConstraint.get().getColumnNames());
            String constraintName =
                    sqlTableConstraint
                            .get()
                            .getConstraintName()
                            .orElseGet(
                                    () ->
                                            primaryKeyColumns.stream()
                                                    .collect(Collectors.joining("_", "PK_", "")));
            builder.primaryKeyNamed(constraintName, primaryKeyColumns);
        }

        CatalogDynamicTable dynamicTable =
                CatalogDynamicTable.of(
                        builder.build(),
                        tableComment,
                        partitionKeys,
                        properties,
                        null,
                        expandedQuery,
                        freshness,
                        refreshMode,
                        new RefreshHandler(
                                CatalogDynamicTable.RefreshMode.CONTINUOUS,
                                RefreshHandler.State.INITIALIZING,
                                null));

        return new CreateDynamicTableOperation(identifier, dynamicTable);
    }
}
