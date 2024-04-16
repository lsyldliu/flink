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

package org.apache.flink.table.gateway.service;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.table.catalog.FileSystemCatalog;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.MockedEndpointVersion;
import org.apache.flink.table.gateway.service.session.SessionManagerImpl;
import org.apache.flink.table.gateway.service.utils.IgnoreExceptionHandler;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.table.api.config.DynamicTableConfigOptions.DATE_FORMATTER;
import static org.apache.flink.table.api.config.DynamicTableConfigOptions.PARTITION_FIELDS;
import static org.apache.flink.table.utils.DateTimeUtils.LOCAL_TZ;
import static org.apache.flink.table.utils.DateTimeUtils.formatTimestampString;

public class DynamicTableStatementITCase {

    @RegisterExtension
    @Order(1)
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .build());

    @RegisterExtension
    @Order(2)
    static final SqlGatewayServiceExtension SQL_GATEWAY_SERVICE_EXTENSION =
            new SqlGatewayServiceExtension(MINI_CLUSTER::getClientConfiguration);

    @RegisterExtension
    @Order(3)
    static final TestExecutorExtension<ExecutorService> EXECUTOR_EXTENSION =
            new TestExecutorExtension<>(
                    () ->
                            Executors.newCachedThreadPool(
                                    new ExecutorThreadFactory(
                                            "SqlGatewayService Test Pool",
                                            IgnoreExceptionHandler.INSTANCE)));

    @TempDir private static Path path;

    private static SessionManagerImpl sessionManager;
    private static SqlGatewayServiceImpl service;

    private static SessionEnvironment defaultSessionEnvironment;

    @BeforeAll
    static void setUp() {
        sessionManager = (SessionManagerImpl) SQL_GATEWAY_SERVICE_EXTENSION.getSessionManager();
        service = (SqlGatewayServiceImpl) SQL_GATEWAY_SERVICE_EXTENSION.getService();

        File myDb = new File(path.toFile(), "mydb");
        myDb.mkdir();
        defaultSessionEnvironment =
                SessionEnvironment.newBuilder()
                        .setSessionEndpointVersion(MockedEndpointVersion.V1)
                        .registerCatalog("cat", new GenericInMemoryCatalog("cat"))
                        .registerCatalog(
                                "myFileCat",
                                new FileSystemCatalog(
                                        path.toAbsolutePath().toString(), "myFileCat", "mydb"))
                        .setDefaultCatalog("cat")
                        .build();
    }

    @Test
    void testCreateDynamicTableWithContinuousMode() throws Exception {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);
        String dataGenSource =
                "CREATE TABLE source (\n"
                        + "  order_id BIGINT,\n"
                        + "  order_number VARCHAR(20),\n"
                        + "  user_id BIGINT,\n"
                        + "  shop_id BIGINT,\n"
                        + "  product_id BIGINT,\n"
                        + "  status BIGINT,\n"
                        + "  order_type BIGINT,\n"
                        + "  order_created_at TIMESTAMP,\n"
                        + "  ds BIGINT,\n"
                        + "  order_created_at_hour VARCHAR(2),\n"
                        + "  product_code VARCHAR(20),\n"
                        + "  product_name VARCHAR(20),\n"
                        + "  product_created_at TIMESTAMP,\n"
                        + "  product_updated_at TIMESTAMP,\n"
                        + "  payment_id BIGINT,\n"
                        + "  payment_number VARCHAR(20),\n"
                        + "  payment_mode BIGINT,\n"
                        + "  payment_source VARCHAR(20),\n"
                        + "  payment_processed_at TIMESTAMP,\n"
                        + "  payment_processed_at_day VARCHAR(10),\n"
                        + "  payment_amount_cents BIGINT\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'fields.ds.kind' = 'sequence',\n"
                        + "  'fields.ds.start' = '1711415942',\n"
                        + "  'fields.ds.end' = '1711502342'\n"
                        + ")";
        service.executeStatement(sessionHandle, dataGenSource, -1, new Configuration());

        String fileSystemSource =
                "CREATE TABLE c_source (\n"
                        + "  order_id BIGINT,\n"
                        + "  order_number VARCHAR(20),\n"
                        + "  user_id BIGINT,\n"
                        + "  shop_id BIGINT,\n"
                        + "  product_id BIGINT,\n"
                        + "  status BIGINT,\n"
                        + "  order_type BIGINT,\n"
                        + "  order_created_at TIMESTAMP,\n"
                        + "  ds BIGINT,\n"
                        + "  order_created_at_hour VARCHAR(2),\n"
                        + "  product_code VARCHAR(20),\n"
                        + "  product_name VARCHAR(20),\n"
                        + "  product_created_at TIMESTAMP,\n"
                        + "  product_updated_at TIMESTAMP,\n"
                        + "  payment_id BIGINT,\n"
                        + "  payment_number VARCHAR(20),\n"
                        + "  payment_mode BIGINT,\n"
                        + "  payment_source VARCHAR(20),\n"
                        + "  payment_processed_at TIMESTAMP,\n"
                        + "  payment_processed_at_day VARCHAR(10),\n"
                        + "  payment_amount_cents BIGINT\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'filesystem',\n"
                        + "  'source.monitor-interval' = '10S',\n"
                        + "  'path' = '/tmp/dt_demo/file_cat/mydb/file_source',\n"
                        + "  'format' = 'csv'"
                        + ")";
        service.executeStatement(sessionHandle, fileSystemSource, -1, new Configuration());

        String dynamicTable =
                "CREATE DYNAMIC TABLE myFileCat.mydb.dwm_users_shops"
                        + " PARTITIONED BY (ds)\n"
                        + " WITH(\n"
                        + "   'format' = 'debezium-json'\n"
                        + " )\n"
                        + " FRESHNESS = INTERVAL '3' MINUTE\n"
                        + " AS SELECT \n"
                        + "  user_id,\n"
                        + "  shop_id,\n"
                        + "  ds,\n"
                        + "  SUM (payment_amount_cents) AS payed_buy_fee_sum,\n"
                        + "  SUM (1) AS pv\n"
                        + "FROM (SELECT user_id, shop_id, FROM_UNIXTIME(ds, 'yyyy-MM-dd') AS ds, payment_amount_cents FROM c_source) as tmp\n"
                        + "GROUP BY (user_id, shop_id, ds)";
        OperationHandle dynamicTableOperationHandle =
                service.executeStatement(sessionHandle, dynamicTable, -1, new Configuration());
        System.out.println(path.toString());
        Thread.sleep(500000);
        System.out.println();
    }

    @Test
    void testCreateDynamicTableWithFullMode() throws Exception {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);
        String dataGenSource =
                "CREATE TABLE source (\n"
                        + "  order_id BIGINT,\n"
                        + "  order_number VARCHAR(20),\n"
                        + "  user_id BIGINT,\n"
                        + "  shop_id BIGINT,\n"
                        + "  product_id BIGINT,\n"
                        + "  status BIGINT,\n"
                        + "  order_type BIGINT,\n"
                        + "  order_created_at TIMESTAMP,\n"
                        + "  ds BIGINT,\n"
                        + "  order_created_at_hour VARCHAR(2),\n"
                        + "  product_code VARCHAR(20),\n"
                        + "  product_name VARCHAR(20),\n"
                        + "  product_created_at TIMESTAMP,\n"
                        + "  product_updated_at TIMESTAMP,\n"
                        + "  payment_id BIGINT,\n"
                        + "  payment_number VARCHAR(20),\n"
                        + "  payment_mode BIGINT,\n"
                        + "  payment_source VARCHAR(20),\n"
                        + "  payment_processed_at TIMESTAMP,\n"
                        + "  payment_processed_at_day VARCHAR(10),\n"
                        + "  payment_amount_cents BIGINT\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'fields.ds.kind' = 'sequence',\n"
                        + "  'fields.ds.start' = '1711415942',\n"
                        + "  'fields.ds.end' = '1711502342'\n"
                        + ")";
        service.executeStatement(sessionHandle, dataGenSource, -1, new Configuration());

        String fileSystemSource =
                "CREATE TABLE myFileCat.mydb.file_source (\n"
                        + "  order_id BIGINT,\n"
                        + "  order_number VARCHAR(20),\n"
                        + "  user_id BIGINT,\n"
                        + "  shop_id BIGINT,\n"
                        + "  product_id BIGINT,\n"
                        + "  status BIGINT,\n"
                        + "  order_type BIGINT,\n"
                        + "  order_created_at TIMESTAMP,\n"
                        + "  ds BIGINT,\n"
                        + "  order_created_at_hour VARCHAR(2),\n"
                        + "  product_code VARCHAR(20),\n"
                        + "  product_name VARCHAR(20),\n"
                        + "  product_created_at TIMESTAMP,\n"
                        + "  product_updated_at TIMESTAMP,\n"
                        + "  payment_id BIGINT,\n"
                        + "  payment_number VARCHAR(20),\n"
                        + "  payment_mode BIGINT,\n"
                        + "  payment_source VARCHAR(20),\n"
                        + "  payment_processed_at TIMESTAMP,\n"
                        + "  payment_processed_at_day VARCHAR(10),\n"
                        + "  payment_amount_cents BIGINT\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'filesystem',\n"
                        + "  'format' = 'json'"
                        + ")";
        service.executeStatement(sessionHandle, fileSystemSource, -1, new Configuration());

        service.executeStatement(
                sessionHandle,
                "INSERT INTO myFileCat.mydb.file_source SELECT * FROM source",
                -1,
                new Configuration());

        String dynamicTable =
                "CREATE DYNAMIC TABLE myFileCat.mydb.dwm_users_shops"
                        + " PARTITIONED BY (ds)\n"
                        + " WITH(\n"
                        + "   'format' = 'debezium-json'\n"
                        + " )\n"
                        + " FRESHNESS = INTERVAL '1' HOUR\n"
                        + " AS SELECT \n"
                        + "  user_id,\n"
                        + "  shop_id,\n"
                        + "  ds,\n"
                        + "  SUM (payment_amount_cents) AS payed_buy_fee_sum,\n"
                        + "  SUM (1) AS pv\n"
                        + "FROM (SELECT user_id, shop_id, FROM_UNIXTIME(ds, 'yyyy-MM-dd') AS ds, payment_amount_cents FROM myFileCat.mydb.file_source) as tmp\n"
                        + "GROUP BY (user_id, shop_id, ds)";
        OperationHandle dynamicTableOperationHandle =
                service.executeStatement(sessionHandle, dynamicTable, -1, new Configuration());
        System.out.println(path.toString());

        Thread.sleep(5000);

        service.executeStatement(
                sessionHandle,
                "ALTER DYNAMIC TABLE myFileCat.mydb.dwm_users_shops SUSPEND",
                -1,
                new Configuration());

        Thread.sleep(5000);

        service.executeStatement(
                sessionHandle,
                "ALTER DYNAMIC TABLE myFileCat.mydb.dwm_users_shops REFRESH",
                -1,
                new Configuration());

        service.executeStatement(
                sessionHandle,
                "DROP DYNAMIC TABLE myFileCat.mydb.dwm_users_shops",
                -1,
                new Configuration());
        Thread.sleep(500000);
        System.out.println();
    }

    @Test
    void testInsertIntoWithCTE() throws Exception {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);
        String dataGenSource =
                "CREATE TABLE c_source (\n"
                        + "  order_id BIGINT,\n"
                        + "  order_number VARCHAR(20),\n"
                        + "  user_id BIGINT,\n"
                        + "  shop_id BIGINT,\n"
                        + "  product_id BIGINT,\n"
                        + "  status BIGINT,\n"
                        + "  order_type BIGINT,\n"
                        + "  order_created_at TIMESTAMP,\n"
                        + "  ds BIGINT,\n"
                        + "  order_created_at_hour VARCHAR(2),\n"
                        + "  product_code VARCHAR(20),\n"
                        + "  product_name VARCHAR(20),\n"
                        + "  product_created_at TIMESTAMP,\n"
                        + "  product_updated_at TIMESTAMP,\n"
                        + "  payment_id BIGINT,\n"
                        + "  payment_number VARCHAR(20),\n"
                        + "  payment_mode BIGINT,\n"
                        + "  payment_source VARCHAR(20),\n"
                        + "  payment_processed_at TIMESTAMP,\n"
                        + "  payment_processed_at_day VARCHAR(10),\n"
                        + "  payment_amount_cents BIGINT\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'fields.ds.kind' = 'sequence',\n"
                        + "  'fields.ds.start' = '1711415942',\n"
                        + "  'fields.ds.end' = '1711502342'\n"
                        + ")";
        service.executeStatement(sessionHandle, dataGenSource, -1, new Configuration());

        String printSink =
                "CREATE TABLE print_sink (\n"
                        + "user_id BIGINT,\n"
                        + "shop_id BIGINT,\n"
                        + "ds STRING,\n"
                        + "payed_buy_fee_sum BIGINT,\n"
                        + "pv BIGINT"
                        + ") WITH (\n"
                        + "  'connector' = 'print'\n"
                        + ")";
        service.executeStatement(sessionHandle, printSink, -1, new Configuration());

        String dynamicTable =
                "INSERT INTO print_sink\n"
                        + "WITH cte_source AS (\n"
                        + "  SELECT "
                        + "  user_id,\n"
                        + "  shop_id,\n"
                        + "  FROM_UNIXTIME(ds, 'yyyy-MM-dd') AS ds,\n"
                        + "  SUM (payment_amount_cents) AS payed_buy_fee_sum,\n"
                        + "  SUM (1) AS pv\n"
                        + "FROM c_source\n"
                        + "GROUP BY user_id, shop_id, FROM_UNIXTIME(ds, 'yyyy-MM-dd')\n"
                        + ")\n"
                        + "SELECT * FROM cte_source";
        OperationHandle dynamicTableOperationHandle =
                service.executeStatement(sessionHandle, dynamicTable, -1, new Configuration());
        System.out.println(path.toString());
        Thread.sleep(500000);

        System.out.println();
    }

    @Test
    void testCreateDynamicTableWithCTE() throws Exception {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);
        String dataGenSource =
                "CREATE TABLE c_source (\n"
                        + "  order_id BIGINT,\n"
                        + "  order_number VARCHAR(20),\n"
                        + "  user_id BIGINT,\n"
                        + "  shop_id BIGINT,\n"
                        + "  product_id BIGINT,\n"
                        + "  status BIGINT,\n"
                        + "  order_type BIGINT,\n"
                        + "  order_created_at TIMESTAMP,\n"
                        + "  ds BIGINT,\n"
                        + "  order_created_at_hour VARCHAR(2),\n"
                        + "  product_code VARCHAR(20),\n"
                        + "  product_name VARCHAR(20),\n"
                        + "  product_created_at TIMESTAMP,\n"
                        + "  product_updated_at TIMESTAMP,\n"
                        + "  payment_id BIGINT,\n"
                        + "  payment_number VARCHAR(20),\n"
                        + "  payment_mode BIGINT,\n"
                        + "  payment_source VARCHAR(20),\n"
                        + "  payment_processed_at TIMESTAMP,\n"
                        + "  payment_processed_at_day VARCHAR(10),\n"
                        + "  payment_amount_cents BIGINT\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'fields.ds.kind' = 'sequence',\n"
                        + "  'fields.ds.start' = '1711415942',\n"
                        + "  'fields.ds.end' = '1711502342'\n"
                        + ")";
        service.executeStatement(sessionHandle, dataGenSource, -1, new Configuration());

        String dynamicTable =
                "CREATE DYNAMIC TABLE myFileCat.mydb.dwm_users_shops"
                        + " PARTITIONED BY (ds)\n"
                        + " WITH(\n"
                        + "   'format' = 'debezium-json'\n"
                        + " )\n"
                        + " FRESHNESS = INTERVAL '3' MINUTE\n"
                        + " AS\n"
                        + "WITH cte_source AS (\n"
                        + "  SELECT "
                        + "  user_id,\n"
                        + "  shop_id,\n"
                        + "  FROM_UNIXTIME(ds, 'yyyy-MM-dd') AS ds,\n"
                        + "  SUM (payment_amount_cents) AS payed_buy_fee_sum,\n"
                        + "  SUM (1) AS pv\n"
                        + "FROM c_source\n"
                        + "GROUP BY user_id, shop_id, FROM_UNIXTIME(ds, 'yyyy-MM-dd')\n"
                        + ")\n"
                        + "SELECT * FROM cte_source";
        OperationHandle dynamicTableOperationHandle =
                service.executeStatement(sessionHandle, dynamicTable, -1, new Configuration());
        System.out.println(path.toString());
        Thread.sleep(500000);

        System.out.println();
    }

    @Test
    void testCreateDynamicTableWithTemporaryView() throws Exception {
        String partKey = "partition.fields.ds.date-formatter";
        String partField =
                partKey.substring(
                        PARTITION_FIELDS.length() + 1,
                        partKey.length() - (DATE_FORMATTER.length() + 1));
        String partFieldFormatter = "yyyyMMdd";
        String scheduleTime = "2024-05-02 13:12:59";
        String scheduleTimeFormatter = "yyyy-MM-dd HH:mm:ss";
        String partFiledValue =
                formatTimestampString(
                        scheduleTime, scheduleTimeFormatter, partFieldFormatter, LOCAL_TZ);
        System.out.println(partField);
        System.out.println(partFiledValue);
        System.out.println(
                formatTimestampString(scheduleTime, scheduleTimeFormatter, "HH", LOCAL_TZ));

        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);
        String dataGenSource =
                "CREATE TABLE c_source (\n"
                        + "  order_id BIGINT,\n"
                        + "  order_number VARCHAR(20),\n"
                        + "  user_id BIGINT,\n"
                        + "  shop_id BIGINT,\n"
                        + "  product_id BIGINT,\n"
                        + "  status BIGINT,\n"
                        + "  order_type BIGINT,\n"
                        + "  order_created_at TIMESTAMP,\n"
                        + "  ds BIGINT,\n"
                        + "  order_created_at_hour VARCHAR(2),\n"
                        + "  product_code VARCHAR(20),\n"
                        + "  product_name VARCHAR(20),\n"
                        + "  product_created_at TIMESTAMP,\n"
                        + "  product_updated_at TIMESTAMP,\n"
                        + "  payment_id BIGINT,\n"
                        + "  payment_number VARCHAR(20),\n"
                        + "  payment_mode BIGINT,\n"
                        + "  payment_source VARCHAR(20),\n"
                        + "  payment_processed_at TIMESTAMP,\n"
                        + "  payment_processed_at_day VARCHAR(10),\n"
                        + "  payment_amount_cents BIGINT\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'fields.ds.kind' = 'sequence',\n"
                        + "  'fields.ds.start' = '1711415942',\n"
                        + "  'fields.ds.end' = '1711502342'\n"
                        + ")";
        service.executeStatement(sessionHandle, dataGenSource, -1, new Configuration());

        String tempView =
                "CREATE TEMPORARY VIEW view_source AS\n"
                        + "  SELECT "
                        + "  user_id,\n"
                        + "  shop_id,\n"
                        + "  FROM_UNIXTIME(ds, 'yyyy-MM-dd') AS ds,\n"
                        + "  SUM (payment_amount_cents) AS payed_buy_fee_sum,\n"
                        + "  SUM (1) AS pv\n"
                        + "FROM c_source\n"
                        + "GROUP BY user_id, shop_id, FROM_UNIXTIME(ds, 'yyyy-MM-dd')";
        service.executeStatement(sessionHandle, tempView, -1, new Configuration());

        String dynamicTable =
                "CREATE DYNAMIC TABLE myFileCat.mydb.dwm_users_shops"
                        + " PARTITIONED BY (ds)\n"
                        + " WITH(\n"
                        + "   'format' = 'debezium-json'\n"
                        + " )\n"
                        + " FRESHNESS = INTERVAL '3' MINUTE\n"
                        + " AS SELECT * FROM view_source";
        OperationHandle dynamicTableOperationHandle =
                service.executeStatement(sessionHandle, dynamicTable, -1, new Configuration());
        System.out.println(path.toString());
        Thread.sleep(500000);

        System.out.println();
    }

    private List<RowData> fetchAllResults(
            SessionHandle sessionHandle, OperationHandle operationHandle) {
        Long token = 0L;
        List<RowData> results = new ArrayList<>();
        while (token != null) {
            ResultSet result =
                    service.fetchResults(sessionHandle, operationHandle, token, Integer.MAX_VALUE);
            results.addAll(result.getData());
            token = result.getNextToken();
        }
        return results;
    }
}
