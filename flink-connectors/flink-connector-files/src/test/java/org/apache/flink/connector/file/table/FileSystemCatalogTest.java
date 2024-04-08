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

package org.apache.flink.connector.file.table;

import org.apache.flink.connector.file.table.catalog.FileSystemCatalog;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.dynamic.CatalogDynamicTable;
import org.apache.flink.table.catalog.dynamic.RefreshHandler;
import org.apache.flink.table.catalog.dynamic.ResolvedCatalogDynamicTable;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for FileSystemCatalog. */
public class FileSystemCatalogTest {

    @TempDir private java.nio.file.Path tmpDir;
    private FileSystemCatalog fileSystemCatalog;

    @BeforeEach
    void before() throws IOException {
        java.nio.file.Path catalogPath = tmpDir.resolve("tmp");
        catalogPath.toFile().mkdir();
        catalogPath.resolve("myDb").toFile().mkdir();

        String catalogPathStr = catalogPath.toUri().getPath();
        fileSystemCatalog = new FileSystemCatalog(catalogPathStr, "myCat", "myDb");
        fileSystemCatalog.open();
    }

    @AfterEach
    void close() throws IOException {
        fileSystemCatalog.close();
    }

    @Test
    public void testCreateCatalogDynamicTable()
            throws TableAlreadyExistException, DatabaseNotExistException, TableNotExistException {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.BIGINT())
                        .column("c", DataTypes.DECIMAL(38, 18))
                        .column("d", DataTypes.TIMESTAMP())
                        .column("e", DataTypes.DOUBLE())
                        .column("f", DataTypes.STRING())
                        .column("p", DataTypes.STRING())
                        .primaryKeyNamed("pk_constraint", Arrays.asList("a", "b", "p"))
                        .build();
        String comment = "table_comment";
        List<String> partitionKeys = Collections.singletonList("p");
        Map<String, String> options = new HashMap<>();
        options.put("key1", "val1");
        options.put("key2", "val2");

        String definitionQuery = "SELECT * FROM T";
        Duration freshness = Duration.ofSeconds(10);

        CatalogDynamicTable.LogicalRefreshMode logicalRefreshMode =
                CatalogDynamicTable.LogicalRefreshMode.CONTINUOUS;
        CatalogDynamicTable.RefreshMode refreshMode = CatalogDynamicTable.RefreshMode.CONTINUOUS;

        List<Column> columns = new ArrayList<>();
        columns.add(Column.physical("a", DataTypes.INT()));
        columns.add(Column.physical("b", DataTypes.BIGINT()));
        columns.add(Column.physical("c", DataTypes.DECIMAL(38, 18)));
        columns.add(Column.physical("d", DataTypes.TIMESTAMP()));
        columns.add(Column.physical("e", DataTypes.DOUBLE()));
        columns.add(Column.physical("f", DataTypes.STRING()));
        columns.add(Column.physical("p", DataTypes.STRING()));

        UniqueConstraint constraint =
                UniqueConstraint.primaryKey("pk_constraint", Arrays.asList("a", "b", "p"));
        ResolvedSchema resolvedSchema =
                new ResolvedSchema(columns, Collections.emptyList(), constraint);

        ResolvedCatalogDynamicTable resolvedCatalogDynamicTable =
                new ResolvedCatalogDynamicTable(
                        CatalogDynamicTable.of(
                                schema,
                                comment,
                                partitionKeys,
                                options,
                                null,
                                definitionQuery,
                                freshness,
                                logicalRefreshMode,
                                refreshMode,
                                CatalogDynamicTable.RefreshStatus.INITIALIZING,
                                null,
                                new byte[0]),
                        resolvedSchema);

        ObjectPath tablePath = new ObjectPath("myDb", "myTable");
        fileSystemCatalog.createTable(tablePath, resolvedCatalogDynamicTable, false);

        boolean exists = fileSystemCatalog.tableExists(tablePath);
        assertTrue(exists);

        CatalogBaseTable catalogBaseTable = fileSystemCatalog.getTable(tablePath);

        TestRefreshHandler refreshHandler = new TestRefreshHandler("clusterId&jobId");
        ResolvedCatalogDynamicTable resolvedCatalogDynamicTable1 =
                new ResolvedCatalogDynamicTable(
                        CatalogDynamicTable.of(
                                schema,
                                comment,
                                partitionKeys,
                                options,
                                null,
                                definitionQuery,
                                freshness,
                                logicalRefreshMode,
                                refreshMode,
                                CatalogDynamicTable.RefreshStatus.ACTIVATED,
                                refreshHandler.asSummaryString(),
                                refreshHandler.toBytes()),
                        resolvedSchema);
        fileSystemCatalog.alterTable(
                tablePath, resolvedCatalogDynamicTable1, Collections.emptyList(), false);

        CatalogBaseTable catalogBaseTable1 = fileSystemCatalog.getTable(tablePath);

        fileSystemCatalog.dropTable(tablePath, false);

        assertFalse(fileSystemCatalog.tableExists(tablePath));
    }

    private static class TestRefreshHandler implements RefreshHandler {

        private final String handlerString;

        public TestRefreshHandler(String handlerString) {
            this.handlerString = handlerString;
        }

        @Override
        public String asSummaryString() {
            return "test refresh handler";
        }

        public byte[] toBytes() {
            return handlerString.getBytes();
        }
    }
}
