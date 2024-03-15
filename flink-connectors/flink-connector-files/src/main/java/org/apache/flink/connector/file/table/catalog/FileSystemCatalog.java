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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.table.FileSystemTableFactory;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.dynamic.CatalogDynamicTable;
import org.apache.flink.table.catalog.dynamic.RefreshHandler;
import org.apache.flink.table.catalog.dynamic.ResolvedCatalogDynamicTable;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.StringUtils;

import org.apache.avro.Schema;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

/** A catalog implementation for {@link FileSystem}. */
public class FileSystemCatalog extends AbstractCatalog {

    static final String SCHEMA_EXTENSION = ".json";

    private final String catalogPathStr;
    private final Path path;

    public FileSystemCatalog(String pathStr, String name, String defaultDatabase) {
        super(name, defaultDatabase);
        this.catalogPathStr = pathStr;
        this.path = new Path(pathStr);
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new FileSystemTableFactory());
    }

    @Override
    public void open() throws CatalogException {
        try {
            FileSystem fs = path.getFileSystem();
            if (!fs.exists(path)) {
                throw new CatalogException(
                        String.format("Catalog %s path %s does not exist.", getName(), path));
            }
            if (!fs.getFileStatus(path).isDir()) {
                throw new CatalogException(
                        String.format(
                                "Failed to open catalog path. The given path %s is not a directory.",
                                path));
            }
        } catch (IOException e) {
            throw new CatalogException(
                    String.format("Checking catalog path %s exists exception.", path), e);
        }
    }

    @Override
    public void close() throws CatalogException {}

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            FileSystem fs = path.getFileSystem();
            FileStatus[] fileStatuses = fs.listStatus(path);
            return Arrays.stream(fileStatuses)
                    .filter(FileStatus::isDir)
                    .map(fileStatus -> fileStatus.getPath().getName())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new CatalogException("Listing database exception.", e);
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        if (databaseExists(databaseName)) {
            return new CatalogDatabaseImpl(Collections.emptyMap(), null);
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));

        return listDatabases().contains(databaseName);
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        if (databaseExists(name)) {
            if (ignoreIfExists) {
                return;
            } else {
                throw new DatabaseAlreadyExistException(getName(), name);
            }
        }

        if (!CollectionUtil.isNullOrEmpty(database.getProperties())) {
            throw new CatalogException(
                    "Hudi catalog doesn't support to create database with options.");
        }

        Path dbPath = new Path(path, name);
        try {
            FileSystem fs = dbPath.getFileSystem();
            fs.mkdirs(dbPath);
        } catch (IOException e) {
            throw new CatalogException(String.format("Creating database %s exception.", name), e);
        }
    }

    @Override
    public void dropDatabase(String databaseName, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        if (!databaseExists(databaseName)) {
            if (ignoreIfNotExists) {
                return;
            } else {
                throw new DatabaseNotExistException(getName(), databaseName);
            }
        }

        List<String> tables = listTables(databaseName);
        if (!tables.isEmpty() && !cascade) {
            throw new DatabaseNotEmptyException(getName(), databaseName);
        }

        if (databaseName.equals(getDefaultDatabase())) {
            throw new IllegalArgumentException(
                    "Hudi catalog doesn't support to drop the default database.");
        }

        Path dbPath = new Path(path, databaseName);
        try {
            FileSystem fs = dbPath.getFileSystem();
            fs.delete(dbPath, true);
        } catch (IOException e) {
            throw new CatalogException(
                    String.format("Dropping database %s exception.", databaseName), e);
        }
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("Altering database is not implemented.");
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        Path dbPath = new Path(path, databaseName);
        try {
            FileSystem fs = dbPath.getFileSystem();
            return Arrays.stream(fs.listStatus(dbPath))
                    .filter(FileStatus::isDir)
                    .map(fileStatus -> fileStatus.getPath().getName())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new CatalogException(
                    String.format("Listing table in database %s exception.", dbPath), e);
        }
    }

    @Override
    public List<String> listViews(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        final Path tableSchemaPath =
                tableSchemaPath(
                        new Path(inferTablePath(catalogPathStr, tablePath)),
                        tablePath.getObjectName());
        try {
            TableSchema tableSchema =
                    JsonSerdeUtil.fromJson(readFileUtf8(tableSchemaPath), TableSchema.class);
            DataType rowType =
                    AvroSchemaConverter.convertToDataType(
                            new Schema.Parser().parse(tableSchema.getSchema()));
            org.apache.flink.table.api.Schema.Builder builder =
                    org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(rowType);
            CatalogBaseTable.TableKind tableKind = tableSchema.getTableKind();
            String comment = tableSchema.getComment();
            Map<String, String> options = tableSchema.getOptions();
            String pkConstraintName = tableSchema.getPkConstraintName();
            List<String> pkColumns = getColumns(tableSchema.getPkColumns());
            if (pkConstraintName != null) {
                builder.primaryKeyNamed(pkConstraintName, pkColumns);
            }
            List<String> partitionColumns = getColumns(tableSchema.getPartitionColumns());
            if (tableKind == CatalogBaseTable.TableKind.TABLE) {
                return CatalogTable.of(builder.build(), comment, partitionColumns, options);
            } else {
                return CatalogDynamicTable.of(
                        builder.build(),
                        comment,
                        partitionColumns,
                        options,
                        null,
                        tableSchema.getDefinitionQuery(),
                        tableSchema.getFreshness(),
                        tableSchema.getRefreshMode(),
                        tableSchema.getRefreshHandler());
            }
        } catch (IOException e) {
            throw new CatalogException(
                    String.format("Get table %s occur exception.", tablePath), e);
        }
    }

    private List<String> getColumns(String columnStr) {
        if (columnStr != null) {
            return Arrays.stream(columnStr.split(",")).collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    /** Read file to UTF_8 decoding. */
    String readFileUtf8(Path path) throws IOException {
        try (FSDataInputStream in = path.getFileSystem().open(path)) {
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
            }
            return builder.toString();
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        Path tablePathDir = new Path(inferTablePath(catalogPathStr, tablePath));
        try {
            FileSystem fs = tablePathDir.getFileSystem();
            return fs.exists(tablePathDir);
        } catch (IOException e) {
            throw new CatalogException(
                    String.format("Check table %s exists occur exception.", tablePath), e);
        }
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            } else {
                throw new TableNotExistException(getName(), tablePath);
            }
        }

        Path path = new Path(inferTablePath(catalogPathStr, tablePath));
        try {
            this.path.getFileSystem().delete(path, true);
        } catch (IOException e) {
            throw new CatalogException(String.format("Dropping table %s exception.", tablePath), e);
        }
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException("renameTable is not implemented.");
    }

    @Override
    public void createTable(
            ObjectPath tablePath, CatalogBaseTable catalogTable, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(getName(), tablePath.getDatabaseName());
        }
        if (tableExists(tablePath)) {
            if (ignoreIfExists) {
                return;
            } else {
                throw new TableAlreadyExistException(getName(), tablePath);
            }
        }

        if (catalogTable instanceof CatalogView) {
            throw new UnsupportedOperationException(
                    "FileSystem catalog doesn't support to CREATE VIEW.");
        }
        Tuple2<String, String> tableSchemaTuple = getJsonTableSchema(tablePath, catalogTable);
        Path tableDir = new Path(tableSchemaTuple.f0);
        String jsonSchema = tableSchemaTuple.f1;
        try {
            FileSystem fs = tableDir.getFileSystem();
            if (!fs.exists(tableDir)) {
                fs.mkdirs(tableDir);
            }
            // write table schema
            Path tableSchemaPath = tableSchemaPath(tableDir, tablePath.getObjectName());
            try (FSDataOutputStream os =
                    tableSchemaPath
                            .getFileSystem()
                            .create(tableSchemaPath, FileSystem.WriteMode.NO_OVERWRITE)) {
                os.write(jsonSchema.getBytes(StandardCharsets.UTF_8));
            }

        } catch (IOException e) {
            throw new CatalogException(String.format("Create table %s exception.", tablePath), e);
        }
    }

    private Tuple2<String, String> getJsonTableSchema(
            ObjectPath tablePath, CatalogBaseTable catalogTable) {
        ResolvedCatalogBaseTable resolvedCatalogBaseTable = (ResolvedCatalogBaseTable) catalogTable;
        CatalogBaseTable.TableKind tableKind = catalogTable.getTableKind();
        ResolvedSchema resolvedSchema = resolvedCatalogBaseTable.getResolvedSchema();
        final String avroSchema =
                AvroSchemaConverter.convertToSchema(
                                resolvedSchema.toPhysicalRowDataType().getLogicalType())
                        .toString();
        String comment = catalogTable.getComment();
        Map<String, String> options = catalogTable.getOptions();
        String pkConstraintName = null;
        String pkColumns = null;
        if (resolvedSchema.getPrimaryKey().isPresent()) {
            pkConstraintName = resolvedSchema.getPrimaryKey().get().getName();
            pkColumns = String.join(",", resolvedSchema.getPrimaryKey().get().getColumns());
        }
        String partitionColumns = null;
        String definitionQuery = null;
        Duration freshness = null;
        CatalogDynamicTable.RefreshMode refreshMode = null;
        RefreshHandler refreshHandler = null;
        // CatalogBase Table convert to json string, then write
        if (catalogTable.getTableKind() == CatalogBaseTable.TableKind.TABLE) {
            ResolvedCatalogTable resolvedTable = (ResolvedCatalogTable) catalogTable;
            if (resolvedTable.isPartitioned()) {
                partitionColumns = String.join(",", resolvedTable.getPartitionKeys());
            }
        } else {
            ResolvedCatalogDynamicTable resolvedCatalogDynamicTable =
                    (ResolvedCatalogDynamicTable) catalogTable;
            if (resolvedCatalogDynamicTable.isPartitioned()) {
                partitionColumns = String.join(",", resolvedCatalogDynamicTable.getPartitionKeys());
            }
            definitionQuery = resolvedCatalogDynamicTable.getDefinitionQuery();
            freshness = resolvedCatalogDynamicTable.getFreshness();
            if (resolvedCatalogDynamicTable.getRefreshMode().isPresent()) {
                refreshMode = resolvedCatalogDynamicTable.getRefreshMode().get();
            }
            refreshHandler = resolvedCatalogDynamicTable.getRefreshJobHandler();
        }

        TableSchema tableSchema =
                new TableSchema(
                        tableKind,
                        avroSchema,
                        comment,
                        options,
                        pkConstraintName,
                        pkColumns,
                        partitionColumns,
                        definitionQuery,
                        freshness,
                        refreshMode,
                        refreshHandler);

        String jsonSchema = JsonSerdeUtil.toJson(tableSchema);
        final String tablePathStr = inferTablePath(catalogPathStr, tablePath);
        return Tuple2.of(tablePathStr, jsonSchema);
    }

    @Override
    public void alterTable(
            ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {}

    @Override
    public void alterTable(
            ObjectPath tablePath,
            CatalogBaseTable newTable,
            List<TableChange> tableChanges,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (ignoreIfNotExists && !tableExists(tablePath)) {
            return;
        }

        Tuple2<String, String> tableSchemaTuple = getJsonTableSchema(tablePath, newTable);
        Path tableDir = new Path(tableSchemaTuple.f0);
        String jsonSchema = tableSchemaTuple.f1;
        try {
            FileSystem fs = tableDir.getFileSystem();
            if (!fs.exists(tableDir)) {
                throw new CatalogException(
                        String.format("Table schema %s doesn't exists.", tablePath));
            }
            // write new table schema
            Path tableSchemaPath = tableSchemaPath(tableDir, tablePath.getObjectName());
            try (FSDataOutputStream os =
                    tableSchemaPath
                            .getFileSystem()
                            .create(tableSchemaPath, FileSystem.WriteMode.OVERWRITE)) {
                os.write(jsonSchema.getBytes(StandardCharsets.UTF_8));
            }

        } catch (IOException e) {
            throw new CatalogException(String.format("Create table %s exception.", tablePath), e);
        }
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return null;
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, TableNotPartitionedException,
                    PartitionSpecInvalidException, CatalogException {
        return null;
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return null;
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return null;
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        return false;
    }

    @Override
    public void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists)
            throws TableNotExistException, TableNotPartitionedException,
                    PartitionSpecInvalidException, PartitionAlreadyExistsException,
                    CatalogException {}

    @Override
    public void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {}

    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("alterPartition is not implemented.");
    }

    @Override
    public List<String> listFunctions(String dbName)
            throws DatabaseNotExistException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath)
            throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(
            ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("createFunction is not implemented.");
    }

    @Override
    public void alterFunction(
            ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException("alterFunction is not implemented.");
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException("dropFunction is not implemented.");
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(
            ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("alterTableStatistics is not implemented.");
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException, TablePartitionedException {
        throw new UnsupportedOperationException("alterTableColumnStatistics is not implemented.");
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("alterPartitionStatistics is not implemented.");
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException(
                "alterPartitionColumnStatistics is not implemented.");
    }

    protected String inferTablePath(String catalogPath, ObjectPath tablePath) {
        return String.format(
                "%s/%s/%s", catalogPath, tablePath.getDatabaseName(), tablePath.getObjectName());
    }

    private Path tableSchemaPath(Path tableDir, String tableName) {
        return new Path(tableDir, tableName + "_schema" + SCHEMA_EXTENSION);
    }
}
