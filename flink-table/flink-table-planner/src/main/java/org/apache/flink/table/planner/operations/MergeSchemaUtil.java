/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.table.planner.operations;

import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.ddl.position.SqlTableColumnPosition;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/** A utility class with logic for handling the {@code ALTER TABLE ... ADD/MODIFY ...} clause. */
public class MergeSchemaUtil {

    private final SqlValidator sqlValidator;
    private final Function<SqlNode, String> escapeExpression;
    private final Consumer<SqlTableConstraint> validateTableConstraint;

    public MergeSchemaUtil(
            SqlValidator sqlValidator,
            Function<SqlNode, String> escapeExpression,
            Consumer<SqlTableConstraint> validateTableConstraint) {
        this.sqlValidator = sqlValidator;
        this.escapeExpression = escapeExpression;
        this.validateTableConstraint = validateTableConstraint;
    }

    /**
     * Merge source {@link Schema} and modified columns/constraint/watermark to a new {@link Schema}
     * in {@code ALTER TABLE ... MODIFY ...} clause.
     */
    public Schema mergeSchema(
            ObjectIdentifier tableIdentifier,
            Schema originSchema,
            List<SqlTableColumnPosition> modifiedColumnPositions,
            @Nullable SqlWatermark watermark,
            List<SqlTableConstraint> constraints) {
        // construct column name and itself mapping
        Map<String, Schema.UnresolvedColumn> originColumns =
                originSchema.getColumns().stream()
                        .collect(
                                Collectors.toMap(
                                        Schema.UnresolvedColumn::getName, Function.identity()));

        // get all column names of origin table
        List<String> originColumnNames =
                originSchema.getColumns().stream()
                        .map(Schema.UnresolvedColumn::getName)
                        .collect(Collectors.toList());

        // ensure the modified column is existed in table
        modifiedColumnPositions.forEach(
                sqlTableColumnPosition -> {
                    String columnName = sqlTableColumnPosition.getColumn().getName().getSimple();
                    if (!originColumnNames.contains(columnName)) {
                        throw new ValidationException(
                                String.format(
                                        "Column %s was not found in table %s.",
                                        columnName, tableIdentifier));
                    }
                });

        // reorder all columns by the modified columns and its position
        Map<String, SqlTableColumn> modifiedColumns =
                reorderByModifiedColumn(
                        tableIdentifier, originColumnNames, modifiedColumnPositions);

        Schema.Builder builder = Schema.newBuilder();
        // merge original schema and modified columns
        originColumnNames.forEach(
                columnName -> {
                    if (modifiedColumns.containsKey(columnName)) {
                        addSqlTableColumn(builder, modifiedColumns.get(columnName));
                    } else {
                        addUnresolvedColumn(builder, originColumns.get(columnName));
                    }
                });

        // add watermark
        if (watermark != null) {
            String rowtimeAttribute = watermark.getEventTimeColumnName().toString();
            // validate the origin table exists watermark firstly
            validateWatermark(tableIdentifier, rowtimeAttribute, originSchema.getWatermarkSpecs());

            builder.watermark(
                    rowtimeAttribute, escapeExpression.apply(watermark.getWatermarkStrategy()));
        }

        // Sanity check for constraint.
        constraints.forEach(validateTableConstraint);
        // add primary key
        Optional<SqlTableConstraint> primaryKey =
                constraints.stream().filter(SqlTableConstraint::isPrimaryKey).findAny();
        if (primaryKey.isPresent()) {
            // ensure primary key exists in table
            if (!originSchema.getPrimaryKey().isPresent()) {
                throw new ValidationException(
                        String.format(
                                "Table %s does not define primary key, currently not allowed to modify "
                                        + "primary key which was not found in table.",
                                tableIdentifier));
            }

            if (primaryKey.get().getConstraintName().isPresent()) {
                builder.primaryKeyNamed(
                        primaryKey.get().getConstraintName().get(),
                        primaryKey.get().getColumnNames());
            } else {
                builder.primaryKey(primaryKey.get().getColumnNames());
            }
        }
        return builder.build();
    }

    private void validateWatermark(
            ObjectIdentifier tableIdentifier,
            String modifiedRowtimeAttribute,
            List<Schema.UnresolvedWatermarkSpec> originWatermarkSpecs) {
        if (originWatermarkSpecs.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Table %s does not define watermark, currently not allowed to "
                                    + "modify watermark %s which was not found in table.",
                            tableIdentifier, modifiedRowtimeAttribute));
        } else {
            originWatermarkSpecs.forEach(
                    unresolvedWatermarkSpec -> {
                        String originRowtimeAttribute = unresolvedWatermarkSpec.getColumnName();
                        if (!originRowtimeAttribute.equals(modifiedRowtimeAttribute)) {
                            throw new ValidationException(
                                    String.format(
                                            "Currently not allowed to modify watermark `%s` which is inconsistent with "
                                                    + "table original watermark `%s`.",
                                            modifiedRowtimeAttribute, originRowtimeAttribute));
                        }
                    });
        }
    }

    private void addSqlTableColumn(Schema.Builder builder, SqlTableColumn sqlTableColumn) {
        final String name = sqlTableColumn.getName().getSimple();
        if (sqlTableColumn instanceof SqlTableColumn.SqlRegularColumn) {
            final SqlTableColumn.SqlRegularColumn regularColumn =
                    (SqlTableColumn.SqlRegularColumn) sqlTableColumn;
            SqlDataTypeSpec type = regularColumn.getType();
            boolean nullable = type.getNullable() == null ? true : type.getNullable();
            RelDataType relType = type.deriveType(sqlValidator, nullable);
            builder.column(name, fromLogicalToDataType(toLogicalType(relType)));
        } else if (sqlTableColumn instanceof SqlTableColumn.SqlComputedColumn) {
            final SqlTableColumn.SqlComputedColumn computedColumn =
                    (SqlTableColumn.SqlComputedColumn) sqlTableColumn;
            builder.columnByExpression(name, escapeExpression.apply(computedColumn.getExpr()));
        } else if (sqlTableColumn instanceof SqlTableColumn.SqlMetadataColumn) {
            final SqlTableColumn.SqlMetadataColumn metadataColumn =
                    (SqlTableColumn.SqlMetadataColumn) sqlTableColumn;
            SqlDataTypeSpec type = metadataColumn.getType();
            boolean nullable = type.getNullable() == null ? true : type.getNullable();
            RelDataType relType = type.deriveType(sqlValidator, nullable);
            builder.columnByMetadata(
                    name,
                    fromLogicalToDataType(toLogicalType(relType)),
                    metadataColumn.getMetadataAlias().orElse(null),
                    metadataColumn.isVirtual());
        } else {
            throw new ValidationException("Unsupported column type: " + sqlTableColumn);
        }
        // parse column comment
        sqlTableColumn
                .getComment()
                .map(SqlCharStringLiteral.class::cast)
                .map(comment -> builder.withComment(comment.getValueAs(String.class)))
                .orElse(null);
    }

    private void addUnresolvedColumn(
            Schema.Builder builder, Schema.UnresolvedColumn unresolvedColumn) {
        if (unresolvedColumn instanceof Schema.UnresolvedPhysicalColumn) {
            builder.column(
                    unresolvedColumn.getName(),
                    ((Schema.UnresolvedPhysicalColumn) unresolvedColumn).getDataType());
        } else if (unresolvedColumn instanceof Schema.UnresolvedComputedColumn) {
            builder.columnByExpression(
                    unresolvedColumn.getName(),
                    ((Schema.UnresolvedComputedColumn) unresolvedColumn).getExpression());
        } else if (unresolvedColumn instanceof Schema.UnresolvedMetadataColumn) {
            Schema.UnresolvedMetadataColumn unresolvedMetadataColumn =
                    (Schema.UnresolvedMetadataColumn) unresolvedColumn;
            builder.columnByMetadata(
                    unresolvedColumn.getName(),
                    unresolvedMetadataColumn.getDataType(),
                    unresolvedMetadataColumn.getMetadataKey(),
                    unresolvedMetadataColumn.isVirtual());
        }
        // add comment
        builder.withComment(unresolvedColumn.getComment().orElse(null));
    }

    /** Reorder the column according to the original columns and new modified columns. */
    private Map<String, SqlTableColumn> reorderByModifiedColumn(
            ObjectIdentifier tableIdentifier,
            List<String> originColumnNames,
            List<SqlTableColumnPosition> modifiedColumnPositions) {
        Map<String, SqlTableColumn> modifiedColumns = new HashMap<>();
        modifiedColumnPositions.forEach(
                columnPosition -> {
                    String columnName = columnPosition.getColumn().getName().getSimple();
                    if (columnPosition.isFirstColumn()) {
                        // remove column firstly
                        originColumnNames.remove(columnName);
                        // add the column to first index
                        originColumnNames.add(0, columnName);
                    } else if (columnPosition.isReferencedColumn()) {
                        // remove column firstly
                        originColumnNames.remove(columnName);
                        // get referenced column index secondly
                        String referencedColumnName =
                                columnPosition.getReferencedColumn().getSimple();
                        int afterIndex = originColumnNames.indexOf(referencedColumnName);
                        if (afterIndex < 0) {
                            throw new ValidationException(
                                    String.format(
                                            "The column '%s' referenced by column '%s' was not found in table %s.",
                                            referencedColumnName, columnName, tableIdentifier));
                        }
                        // add the column to new position
                        originColumnNames.add(afterIndex + 1, columnName);
                    }
                    // Otherwise, keep the original column position order

                    // add the modified column to map that is used to construct new column
                    // definition in next step
                    modifiedColumns.put(columnName, columnPosition.getColumn());
                });
        return modifiedColumns;
    }
}
