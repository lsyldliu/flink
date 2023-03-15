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

package org.apache.flink.connectors.hive;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FileUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests the dynamic partition pruning optimization on Hive sources. */
@ExtendWith(ParameterizedTestExtension.class)
public class HiveDynamicPartitionPruningITCase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    @Parameter public boolean enableAdaptiveBatchScheduler;

    @Parameters(name = "enableAdaptiveBatchScheduler={0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false);
    }

    private TableEnvironment tableEnv;
    private HiveCatalog hiveCatalog;
    private String warehouse;

    @BeforeEach
    public void setup() {
        hiveCatalog = HiveTestUtils.createHiveCatalog();
        hiveCatalog
                .getHiveConf()
                .setBoolVar(
                        HiveConf.ConfVars.METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES, false);
        hiveCatalog.open();
        warehouse = hiveCatalog.getHiveConf().getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);

        if (enableAdaptiveBatchScheduler) {
            tableEnv = HiveTestUtils.createTableEnvInBatchModeWithAdaptiveScheduler();
        } else {
            tableEnv = HiveTestUtils.createTableEnvInBatchMode();
        }

        tableEnv.getConfig().getConfiguration().set(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tableEnv.useCatalog(hiveCatalog.getName());
    }

    @AfterEach
    public void tearDown() {
        if (hiveCatalog != null) {
            hiveCatalog.close();
        }
        if (warehouse != null) {
            FileUtils.deleteDirectoryQuietly(new File(warehouse));
        }
    }

    @TestTemplate
    public void testMultiInputBHJ() throws Exception {
        // src table
        tableEnv.executeSql(
                "CREATE TABLE `date_dim`(\n"
                        + "  `d_date_sk` bigint,\n"
                        + "  `d_date_id` string,\n"
                        + "  `d_date` date,\n"
                        + "  `d_month_seq` int,\n"
                        + "  `d_week_seq` int,\n"
                        + "  `d_quarter_seq` int,\n"
                        + "  `d_year` int,\n"
                        + "  `d_dow` int,\n"
                        + "  `d_moy` int,\n"
                        + "  `d_dom` int,\n"
                        + "  `d_qoy` int,\n"
                        + "  `d_fy_year` int,\n"
                        + "  `d_fy_quarter_seq` int,\n"
                        + "  `d_fy_week_seq` int,\n"
                        + "  `d_day_name` string,\n"
                        + "  `d_quarter_name` string,\n"
                        + "  `d_holiday` string,\n"
                        + "  `d_weekend` string,\n"
                        + "  `d_following_holiday` string,\n"
                        + "  `d_first_dom` int,\n"
                        + "  `d_last_dom` int,\n"
                        + "  `d_same_day_ly` int,\n"
                        + "  `d_same_day_lq` int,\n"
                        + "  `d_current_day` string,\n"
                        + "  `d_current_week` string,\n"
                        + "  `d_current_month` string,\n"
                        + "  `d_current_quarter` string,\n"
                        + "  `d_current_year` string)\n"
                        + " stored as orc");
        tableEnv.executeSql(
                        "insert into date_dim values "
                                + "(2451911, 'AAAAAAAAHMJGFCAA', '2001-01-01', 2451911, 5270, 405, 2001, 1, 1, 1, 1, 2001, 405, 5270, 'Monday', '2001Q1', 'Y', 'N', 'Y', 2451911, 2451910, 2451545, 2451819, 'N', 'N', 'N', 'N', 'N'),"
                                + "(2451967, 'AAAAAAAAPPJGFCAA', '2001-02-26', 1213, 5278, 405, 2001, 1, 2, 26, 1, 2001, 405, 5278, 'Monday', '2001Q1', 'N', 'N', 'N', 2451942, 2451972, 2451601, 2451875, 'N', 'N', 'N', 'N', 'N'),"
                                + "(2452263, 'AAAAAAAAHCLGFCAA', '2001-12-19', 1223, 5321, 409, 2001, 3, 12, 19, 4, 2001, 409, 5321, 'Wednesday', '2001Q4', 'N', 'N', 'N', 2452245, 2452578, 2451898, 2452171, 'N', 'N', 'N', 'N', 'N')")
                .await();

        // partitioned dest table
        tableEnv.executeSql(
                "CREATE TABLE `store_sales`(\n"
                        + "  `ss_sold_time_sk` bigint,\n"
                        + "  `ss_item_sk` bigint,\n"
                        + "  `ss_customer_sk` bigint,\n"
                        + "  `ss_cdemo_sk` bigint,\n"
                        + "  `ss_hdemo_sk` bigint,\n"
                        + "  `ss_addr_sk` bigint,\n"
                        + "  `ss_store_sk` bigint,\n"
                        + "  `ss_promo_sk` bigint,\n"
                        + "  `ss_ticket_number` bigint,\n"
                        + "  `ss_quantity` int,\n"
                        + "  `ss_wholesale_cost` double,\n"
                        + "  `ss_list_price` double,\n"
                        + "  `ss_sales_price` double,\n"
                        + "  `ss_ext_discount_amt` double,\n"
                        + "  `ss_ext_sales_price` double,\n"
                        + "  `ss_ext_wholesale_cost` double,\n"
                        + "  `ss_ext_list_price` double,\n"
                        + "  `ss_ext_tax` double,\n"
                        + "  `ss_coupon_amt` double,\n"
                        + "  `ss_net_paid` double,\n"
                        + "  `ss_net_paid_inc_tax` double,\n"
                        + "  `ss_net_profit` double)\n"
                        /*                        + "PARTITIONED BY (\n"
                        + "  `ss_sold_date_sk` bigint)\n"*/
                        + "stored as orc");

        // partition (ss_sold_date_sk=2451911)
        tableEnv.executeSql(
                        "insert into store_sales values (1, 2451911, 2451911, 1029361, 1176, 4603838, 805, 1944, 1914078841, 84, 57.31, 111.18, 27.79, 0.0, 2334.36, 4814.04, 9339.12, 23.34, 0.0, 2334.36, 2357.7, -2479.68),"
                                + "(2, 104379, 62073099, 1029361, 1176, 4603838, 805, 155, 1914078841, 94, 82.88, 106.08, 67.89, 510.53, 6381.66, 7790.72, 9971.52, 410.97, 510.53, 5871.13, 6282.1, -1919.59),"
                                + "(3, 2451911, 2451911, 832645, 6647, 12131496, 388, 1105, 743661365, 92, 32.57, 49.83, 37.87, 2926.59, 3484.04, 2996.44, 4584.36, 39.02,2926.59, 557.45, 596.47, -2438.99),"
                                + "(4, 2451911, 2451911, 515348, 3329, 22558157, 1174, 183, 731665549, 10, 77.96, 84.19, 60.61, 0.0, 606.1, 779.6, 841.9, 6.06, 0.0, 606.1, 612.16, -173.5),"
                                + "(5, 2451911, 2451911, 832645, 6647, 12131496, 388, 1105, 743661365, 92, 32.57, 49.83, 37.87, 2926.59, 3484.04, 2996.44, 4584.36, 39.02,2926.59, 557.45, 596.47, -2438.99),"
                                + "(6, 2451911, 2451911, 515348, 3329, 22558157, 1174, 183, 731665549, 10, 77.96, 84.19, 60.61, 0.0, 606.1, 779.6, 841.9, 6.06, 0.0, 606.1, 612.16, -173.5)")
                .await();
        /*tableEnv.executeSql(
                        "insert into store_sales values (38660, 2451911, 2451911, 832645, 6647, 12131496, 388, 1105, 743661365, 92, 32.57, 49.83, 37.87, 2926.59, 3484.04, 2996.44, 4584.36, 39.02,2926.59, 557.45, 596.47, -2438.99)")
                .await();
        tableEnv.executeSql(
                        "insert into store_sales values (48831, 2451911, 2451911, 515348, 3329, 22558157, 1174, 183, 731665549, 10, 77.96, 84.19, 60.61, 0.0, 606.1, 779.6, 841.9, 6.06, 0.0, 606.1, 612.16, -173.5)")
                .await();*/

        tableEnv.getConfig().set(HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM, false);

        String sql =
                "select ss_customer_sk, ss_item_sk, ss_sold_time_sk, d_date_sk\n"
                        + "from store_sales, date_dim\n"
                        + "where store_sales.ss_customer_sk = date_dim.d_date_sk and store_sales.ss_item_sk = date_dim.d_month_seq";

        String expected =
                "[+I[2451911, 2451911, 48831, 2451911], +I[2451911, 2451911, 30443, 2451911], +I[2451911, 2451911, 38660, 2451911]]";

        tableEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD,
                        Long.MAX_VALUE);
        tableEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
                        "ShuffleHashJoin, NestedLoopJoin, SortMergeJoin");
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        // Check dynamic partition pruning is working
        String plan = tableEnv.explainSql(sql);
        System.out.println(plan);

        List<Row> results = queryResult(tableEnv.sqlQuery(sql));
        assertThat(results.toString()).isEqualTo(expected);
    }

    @TestTemplate
    public void testMultiInput() throws Exception {
        // src table
        tableEnv.executeSql(
                "CREATE TABLE `date_dim`(\n"
                        + "  `d_date_sk` bigint,\n"
                        + "  `d_date_id` string,\n"
                        + "  `d_date` date,\n"
                        + "  `d_month_seq` int,\n"
                        + "  `d_week_seq` int,\n"
                        + "  `d_quarter_seq` int,\n"
                        + "  `d_year` int,\n"
                        + "  `d_dow` int,\n"
                        + "  `d_moy` int,\n"
                        + "  `d_dom` int,\n"
                        + "  `d_qoy` int,\n"
                        + "  `d_fy_year` int,\n"
                        + "  `d_fy_quarter_seq` int,\n"
                        + "  `d_fy_week_seq` int,\n"
                        + "  `d_day_name` string,\n"
                        + "  `d_quarter_name` string,\n"
                        + "  `d_holiday` string,\n"
                        + "  `d_weekend` string,\n"
                        + "  `d_following_holiday` string,\n"
                        + "  `d_first_dom` int,\n"
                        + "  `d_last_dom` int,\n"
                        + "  `d_same_day_ly` int,\n"
                        + "  `d_same_day_lq` int,\n"
                        + "  `d_current_day` string,\n"
                        + "  `d_current_week` string,\n"
                        + "  `d_current_month` string,\n"
                        + "  `d_current_quarter` string,\n"
                        + "  `d_current_year` string)\n"
                        + " stored as orc");
        /*        tableEnv.executeSql(
                "insert into date_dim values "
                        + "(2451911, 'AAAAAAAAHMJGFCAA', '2001-01-01', 1212, 5270, 405, 2001, 1, 1, 1, 1, 2001, 405, 5270, 'Monday', '2001Q1', 'Y', 'N', 'Y', 2451911, 2451910, 2451545, 2451819, 'N', 'N', 'N', 'N', 'N'),"
                        + "(2451967, 'AAAAAAAAPPJGFCAA', '2001-02-26', 1213, 5278, 405, 2001, 1, 2, 26, 1, 2001, 405, 5278, 'Monday', '2001Q1', 'N', 'N', 'N', 2451942, 2451972, 2451601, 2451875, 'N', 'N', 'N', 'N', 'N'),"
                        + "(2452263, 'AAAAAAAAHCLGFCAA', '2001-12-19', 1223, 5321, 409, 2001, 3, 12, 19, 4, 2001, 409, 5321, 'Wednesday', '2001Q4', 'N', 'N', 'N', 2452245, 2452578, 2451898, 2452171, 'N', 'N', 'N', 'N', 'N')")
        .await();*/

        tableEnv.executeSql(
                "CREATE TABLE `call_center`(\n"
                        + "  `cc_call_center_sk` bigint,\n"
                        + "  `cc_call_center_id` string,\n"
                        + "  `cc_rec_start_date` date,\n"
                        + "  `cc_rec_end_date` date,\n"
                        + "  `cc_closed_date_sk` bigint,\n"
                        + "  `cc_open_date_sk` bigint,\n"
                        + "  `cc_name` string,\n"
                        + "  `cc_class` string,\n"
                        + "  `cc_employees` int,\n"
                        + "  `cc_sq_ft` int,\n"
                        + "  `cc_hours` string,\n"
                        + "  `cc_manager` string,\n"
                        + "  `cc_mkt_id` int,\n"
                        + "  `cc_mkt_class` string,\n"
                        + "  `cc_mkt_desc` string,\n"
                        + "  `cc_market_manager` string,\n"
                        + "  `cc_division` int,\n"
                        + "  `cc_division_name` string,\n"
                        + "  `cc_company` int,\n"
                        + "  `cc_company_name` string,\n"
                        + "  `cc_street_number` string,\n"
                        + "  `cc_street_name` string,\n"
                        + "  `cc_street_type` string,\n"
                        + "  `cc_suite_number` string,\n"
                        + "  `cc_city` string,\n"
                        + "  `cc_county` string,\n"
                        + "  `cc_state` string,\n"
                        + "  `cc_zip` string,\n"
                        + "  `cc_country` string,\n"
                        + "  `cc_gmt_offset` double,\n"
                        + "  `cc_tax_percentage` double)\n"
                        + " stored as orc");
        /*        tableEnv.executeSql(
                "insert into call_center values "
                        + "(2451911, 'AAAAAAAAHMJGFCAA', '2001-01-01', 1212, 5270, 405, 2001, 1, 1, 1, 1, 2001, 405, 5270, 'Monday', '2001Q1', 'Y', 'N', 'Y', 2451911, 2451910, 2451545, 2451819, 'N', 'N', 'N', 'N', 'N'),"
                        + "(2451967, 'AAAAAAAAPPJGFCAA', '2001-02-26', 1213, 5278, 405, 2001, 1, 2, 26, 1, 2001, 405, 5278, 'Monday', '2001Q1', 'N', 'N', 'N', 2451942, 2451972, 2451601, 2451875, 'N', 'N', 'N', 'N', 'N'),"
                        + "(2452263, 'AAAAAAAAHCLGFCAA', '2001-12-19', 1223, 5321, 409, 2001, 3, 12, 19, 4, 2001, 409, 5321, 'Wednesday', '2001Q4', 'N', 'N', 'N', 2452245, 2452578, 2451898, 2452171, 'N', 'N', 'N', 'N', 'N')")
        .await();*/

        tableEnv.executeSql(
                "CREATE TABLE `ship_mode`(\n"
                        + "  `sm_ship_mode_sk` bigint,\n"
                        + "  `sm_ship_mode_id` string,\n"
                        + "  `sm_type` string,\n"
                        + "  `sm_code` string,\n"
                        + "  `sm_carrier` string,\n"
                        + "  `sm_contract` string)\n"
                        + " stored as orc");
        /*        tableEnv.executeSql(
                "insert into ship_mode values "
                        + "(2451911, 'AAAAAAAAHMJGFCAA', '2001-01-01', 1212, 5270, 405, 2001, 1, 1, 1, 1, 2001, 405, 5270, 'Monday', '2001Q1', 'Y', 'N', 'Y', 2451911, 2451910, 2451545, 2451819, 'N', 'N', 'N', 'N', 'N'),"
                        + "(2451967, 'AAAAAAAAPPJGFCAA', '2001-02-26', 1213, 5278, 405, 2001, 1, 2, 26, 1, 2001, 405, 5278, 'Monday', '2001Q1', 'N', 'N', 'N', 2451942, 2451972, 2451601, 2451875, 'N', 'N', 'N', 'N', 'N'),"
                        + "(2452263, 'AAAAAAAAHCLGFCAA', '2001-12-19', 1223, 5321, 409, 2001, 3, 12, 19, 4, 2001, 409, 5321, 'Wednesday', '2001Q4', 'N', 'N', 'N', 2452245, 2452578, 2451898, 2452171, 'N', 'N', 'N', 'N', 'N')")
        .await();*/

        tableEnv.executeSql(
                "CREATE TABLE `warehouse`(\n"
                        + "  `w_warehouse_sk` bigint,\n"
                        + "  `w_warehouse_id` string,\n"
                        + "  `w_warehouse_name` string,\n"
                        + "  `w_warehouse_sq_ft` int,\n"
                        + "  `w_street_number` string,\n"
                        + "  `w_street_name` string,\n"
                        + "  `w_street_type` string,\n"
                        + "  `w_suite_number` string,\n"
                        + "  `w_city` string,\n"
                        + "  `w_county` string,\n"
                        + "  `w_state` string,\n"
                        + "  `w_zip` string,\n"
                        + "  `w_country` string,\n"
                        + "  `w_gmt_offset` double)\n"
                        + " stored as orc");
        /*        tableEnv.executeSql(
                "insert into warehouse values "
                        + "(2451911, 'AAAAAAAAHMJGFCAA', '2001-01-01', 1212, 5270, 405, 2001, 1, 1, 1, 1, 2001, 405, 5270, 'Monday', '2001Q1', 'Y', 'N', 'Y', 2451911, 2451910, 2451545, 2451819, 'N', 'N', 'N', 'N', 'N'),"
                        + "(2451967, 'AAAAAAAAPPJGFCAA', '2001-02-26', 1213, 5278, 405, 2001, 1, 2, 26, 1, 2001, 405, 5278, 'Monday', '2001Q1', 'N', 'N', 'N', 2451942, 2451972, 2451601, 2451875, 'N', 'N', 'N', 'N', 'N'),"
                        + "(2452263, 'AAAAAAAAHCLGFCAA', '2001-12-19', 1223, 5321, 409, 2001, 3, 12, 19, 4, 2001, 409, 5321, 'Wednesday', '2001Q4', 'N', 'N', 'N', 2452245, 2452578, 2451898, 2452171, 'N', 'N', 'N', 'N', 'N')")
        .await();*/

        // partitioned dest table
        tableEnv.executeSql(
                "CREATE TABLE `catalog_sales`(\n"
                        + "  `cs_sold_time_sk` bigint,\n"
                        + "  `cs_ship_date_sk` bigint,\n"
                        + "  `cs_bill_customer_sk` bigint,\n"
                        + "  `cs_bill_cdemo_sk` bigint,\n"
                        + "  `cs_bill_hdemo_sk` bigint,\n"
                        + "  `cs_bill_addr_sk` bigint,\n"
                        + "  `cs_ship_customer_sk` bigint,\n"
                        + "  `cs_ship_cdemo_sk` bigint,\n"
                        + "  `cs_ship_hdemo_sk` bigint,\n"
                        + "  `cs_ship_addr_sk` bigint,\n"
                        + "  `cs_call_center_sk` bigint,\n"
                        + "  `cs_catalog_page_sk` bigint,\n"
                        + "  `cs_ship_mode_sk` bigint,\n"
                        + "  `cs_warehouse_sk` bigint,\n"
                        + "  `cs_item_sk` bigint,\n"
                        + "  `cs_promo_sk` bigint,\n"
                        + "  `cs_order_number` bigint,\n"
                        + "  `cs_quantity` int,\n"
                        + "  `cs_wholesale_cost` double,\n"
                        + "  `cs_list_price` double,\n"
                        + "  `cs_sales_price` double,\n"
                        + "  `cs_ext_discount_amt` double,\n"
                        + "  `cs_ext_sales_price` double,\n"
                        + "  `cs_ext_wholesale_cost` double,\n"
                        + "  `cs_ext_list_price` double,\n"
                        + "  `cs_ext_tax` double,\n"
                        + "  `cs_coupon_amt` double,\n"
                        + "  `cs_ext_ship_cost` double,\n"
                        + "  `cs_net_paid` double,\n"
                        + "  `cs_net_paid_inc_tax` double,\n"
                        + "  `cs_net_paid_inc_ship` double,\n"
                        + "  `cs_net_paid_inc_ship_tax` double,\n"
                        + "  `cs_net_profit` double)\n"
                        + "PARTITIONED BY (\n"
                        + "  `cs_sold_date_sk` bigint)\n"
                        + "stored as orc");
        /*        tableEnv.executeSql(
                        "insert into store_sales partition (ss_sold_date_sk=2451911) values (30443, 40141, 62073099, 1029361, 1176, 4603838, 805, 1944, 1914078841, 84, 57.31, 111.18, 27.79, 0.0, 2334.36, 4814.04, 9339.12, 23.34, 0.0, 2334.36, 2357.7, -2479.68),"
                                + "(30443, 104379, 62073099, 1029361, 1176, 4603838, 805, 155, 1914078841, 94, 82.88, 106.08, 67.89, 510.53, 6381.66, 7790.72, 9971.52, 410.97, 510.53, 5871.13, 6282.1, -1919.59)")
                .await();
        tableEnv.executeSql(
                        "insert into store_sales partition (ss_sold_date_sk=2451967) values (38660, 85979, 52711796, 832645, 6647, 12131496, 388, 1105, 743661365, 92, 32.57, 49.83, 37.87, 2926.59, 3484.04, 2996.44, 4584.36, 39.02,2926.59, 557.45, 596.47, -2438.99)")
                .await();
        tableEnv.executeSql(
                        "insert into store_sales partition (ss_sold_date_sk=2452265) values (48831, 147511, 49856967, 515348, 3329, 22558157, 1174, 183, 731665549, 10, 77.96, 84.19, 60.61, 0.0, 606.1, 779.6, 841.9, 6.06, 0.0, 606.1, 612.16, -173.5)")
                .await();*/

        // Validate results with table statistics
        tableEnv.getCatalog(tableEnv.getCurrentCatalog())
                .get()
                .alterTableStatistics(
                        new ObjectPath(tableEnv.getCurrentDatabase(), "date_dim"),
                        new CatalogTableStatistics(365, -1, -1, -1),
                        false);

        tableEnv.getCatalog(tableEnv.getCurrentCatalog())
                .get()
                .alterTableStatistics(
                        new ObjectPath(tableEnv.getCurrentDatabase(), "call_center"),
                        new CatalogTableStatistics(54, -1, -1, -1),
                        false);

        tableEnv.getCatalog(tableEnv.getCurrentCatalog())
                .get()
                .alterTableStatistics(
                        new ObjectPath(tableEnv.getCurrentDatabase(), "ship_mode"),
                        new CatalogTableStatistics(20, -1, -1, -1),
                        false);

        tableEnv.getCatalog(tableEnv.getCurrentCatalog())
                .get()
                .alterTableStatistics(
                        new ObjectPath(tableEnv.getCurrentDatabase(), "warehouse"),
                        new CatalogTableStatistics(25, -1, -1, -1),
                        false);

        tableEnv.getCatalog(tableEnv.getCurrentCatalog())
                .get()
                .alterTableStatistics(
                        new ObjectPath(tableEnv.getCurrentDatabase(), "catalog_sales"),
                        new CatalogTableStatistics(14399964710l, -1, -1, -1),
                        false);

        tableEnv.getConfig().set(HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM, false);

        String sql =
                "select\n"
                        + "   substr(w_warehouse_name,1,20)\n"
                        + "  ,sm_type\n"
                        + "  ,cc_name\n"
                        + "  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk <= 30 ) then 1 else 0 end)  as `30 days`\n"
                        + "  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 30) and\n"
                        + "                 (cs_ship_date_sk - cs_sold_date_sk <= 60) then 1 else 0 end )  as `31-60 days`\n"
                        + "  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 60) and\n"
                        + "                 (cs_ship_date_sk - cs_sold_date_sk <= 90) then 1 else 0 end)  as `61-90 days`\n"
                        + "  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 90) and\n"
                        + "                 (cs_ship_date_sk - cs_sold_date_sk <= 120) then 1 else 0 end)  as `91-120 days`\n"
                        + "  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk  > 120) then 1 else 0 end)  as `>120 days`\n"
                        + "from\n"
                        + "   catalog_sales\n"
                        + "  ,warehouse\n"
                        + "  ,ship_mode\n"
                        + "  ,call_center\n"
                        + "  ,date_dim\n"
                        + "where\n"
                        + "    d_month_seq between 1212 and 1212 + 11\n"
                        + "and cs_ship_date_sk   = d_date_sk\n"
                        + "and cs_warehouse_sk   = w_warehouse_sk\n"
                        + "and cs_ship_mode_sk   = sm_ship_mode_sk\n"
                        + "and cs_call_center_sk = cc_call_center_sk\n"
                        + "group by\n"
                        + "   substr(w_warehouse_name,1,20)\n"
                        + "  ,sm_type\n"
                        + "  ,cc_name\n"
                        + "order by substr(w_warehouse_name,1,20)\n"
                        + "        ,sm_type\n"
                        + "        ,cc_name\n"
                        + "limit 100";

        String expected =
                "[+I[2451911, 2451911, 1212], +I[2451911, 2451911, 1212], +I[2451967, 2451967, 1213]]";

        tableEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD,
                        Long.MAX_VALUE);
        tableEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
                        "ShuffleHashJoin, NestedLoopJoin, SortMergeJoin");
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        List<Row> results = queryResult(tableEnv.sqlQuery(sql));
        assertThat(results.toString()).isEqualTo(expected);
    }

    @TestTemplate
    public void testDynamicPartitionPruning() throws Exception {
        // src table
        tableEnv.executeSql("create table dim (x int,y string,z int)");
        tableEnv.executeSql("insert into dim values (1,'a',1),(2,'b',1),(3,'c',2)").await();

        // partitioned dest table
        tableEnv.executeSql("create table fact (a int, b bigint, c string) partitioned by (p int)");
        tableEnv.executeSql(
                        "insert into fact partition (p=1) values (10,100,'aaa'),(11,101,'bbb'),(12,102,'ccc') ")
                .await();
        tableEnv.executeSql(
                        "insert into fact partition (p=2) values (20,200,'aaa'),(21,201,'bbb'),(22,202,'ccc') ")
                .await();
        tableEnv.executeSql(
                        "insert into fact partition (p=3) values (30,300,'aaa'),(31,301,'bbb'),(32,302,'ccc') ")
                .await();

        System.out.println(
                tableEnv.explainSql(
                        "select a, b, c, p, x, y from fact, dim where x = p and z = 1 order by a"));

        tableEnv.getConfig().set(HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM, false);

        String sql = "select a, b, c, p, x, y from fact, dim where x = p and z = 1 order by a";
        String sqlSwapFactDim =
                "select a, b, c, p, x, y from dim, fact where x = p and z = 1 order by a";

        String expected =
                "[+I[10, 100, aaa, 1, 1, a], +I[11, 101, bbb, 1, 1, a], +I[12, 102, ccc, 1, 1, a], "
                        + "+I[20, 200, aaa, 2, 2, b], +I[21, 201, bbb, 2, 2, b], +I[22, 202, ccc, 2, 2, b]]";

        // Check dynamic partition pruning is working
        String plan = tableEnv.explainSql(sql);
        assertThat(plan).contains("DynamicFilteringDataCollector");

        plan = tableEnv.explainSql(sqlSwapFactDim);
        assertThat(plan).contains("DynamicFilteringDataCollector");

        // Validate results
        List<Row> results = queryResult(tableEnv.sqlQuery(sql));
        assertThat(results.toString()).isEqualTo(expected);

        results = queryResult(tableEnv.sqlQuery(sqlSwapFactDim));
        assertThat(results.toString()).isEqualTo(expected);

        // Validate results with table statistics
        tableEnv.getCatalog(tableEnv.getCurrentCatalog())
                .get()
                .alterTableStatistics(
                        new ObjectPath(tableEnv.getCurrentDatabase(), "dim"),
                        new CatalogTableStatistics(3, -1, -1, -1),
                        false);

        results = queryResult(tableEnv.sqlQuery(sql));
        assertThat(results.toString()).isEqualTo(expected);

        results = queryResult(tableEnv.sqlQuery(sqlSwapFactDim));
        assertThat(results.toString()).isEqualTo(expected);
    }

    @TestTemplate
    public void testDynamicPartitionPruningOnTwoFactTables() throws Exception {
        tableEnv.executeSql("create table dim (x int,y string,z int)");
        tableEnv.executeSql("insert into dim values (1,'a',1),(2,'b',1),(3,'c',2)").await();

        // partitioned dest table
        tableEnv.executeSql("create table fact (a int, b bigint, c string) partitioned by (p int)");
        tableEnv.executeSql(
                        "insert into fact partition (p=1) values (10,100,'aaa'),(11,101,'bbb'),(12,102,'ccc') ")
                .await();
        tableEnv.executeSql(
                        "insert into fact partition (p=2) values (20,200,'aaa'),(21,201,'bbb'),(22,202,'ccc') ")
                .await();
        tableEnv.executeSql(
                        "insert into fact partition (p=3) values (30,300,'aaa'),(31,301,'bbb'),(32,302,'ccc') ")
                .await();

        // partitioned dest table
        tableEnv.executeSql(
                "create table fact2 (a int, b bigint, c string) partitioned by (p int)");
        tableEnv.executeSql(
                        "insert into fact2 partition (p=1) values (40,100,'aaa'),(41,101,'bbb'),(42,102,'ccc') ")
                .await();
        tableEnv.executeSql(
                        "insert into fact2 partition (p=2) values (50,200,'aaa'),(51,201,'bbb'),(52,202,'ccc') ")
                .await();
        tableEnv.executeSql(
                        "insert into fact2 partition (p=3) values (60,300,'aaa'),(61,301,'bbb'),(62,302,'ccc') ")
                .await();

        tableEnv.getConfig().set(HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM, false);

        // two fact sources share the same dynamic filter
        String sql =
                "select * from ((select a, b, c, p, x, y from fact, dim where x = p and z = 1) "
                        + "union all "
                        + "(select a, b, c, p, x, y from fact2, dim where x = p and z = 1)) t order by a";
        String expected =
                "[+I[10, 100, aaa, 1, 1, a], +I[11, 101, bbb, 1, 1, a], +I[12, 102, ccc, 1, 1, a], "
                        + "+I[20, 200, aaa, 2, 2, b], +I[21, 201, bbb, 2, 2, b], +I[22, 202, ccc, 2, 2, b], "
                        + "+I[40, 100, aaa, 1, 1, a], +I[41, 101, bbb, 1, 1, a], +I[42, 102, ccc, 1, 1, a], "
                        + "+I[50, 200, aaa, 2, 2, b], +I[51, 201, bbb, 2, 2, b], +I[52, 202, ccc, 2, 2, b]]";

        String plan = tableEnv.explainSql(sql);
        assertThat(plan).containsOnlyOnce("DynamicFilteringDataCollector(fields=[x])(reuse_id=");

        List<Row> results = queryResult(tableEnv.sqlQuery(sql));
        assertThat(results.toString()).isEqualTo(expected);

        // two fact sources use different dynamic filters
        String sql2 =
                "select * from ((select a, b, c, p, x, y from fact, dim where x = p and z = 1) "
                        + "union all "
                        + "(select a, b, c, p, x, y from fact2, dim where x = p and z = 2)) t order by a";
        String expected2 =
                "[+I[10, 100, aaa, 1, 1, a], +I[11, 101, bbb, 1, 1, a], +I[12, 102, ccc, 1, 1, a], "
                        + "+I[20, 200, aaa, 2, 2, b], +I[21, 201, bbb, 2, 2, b], +I[22, 202, ccc, 2, 2, b], "
                        + "+I[60, 300, aaa, 3, 3, c], +I[61, 301, bbb, 3, 3, c], +I[62, 302, ccc, 3, 3, c]]";

        plan = tableEnv.explainSql(sql2);
        assertThat(plan).contains("DynamicFilteringDataCollector");

        results = queryResult(tableEnv.sqlQuery(sql2));
        assertThat(results.toString()).isEqualTo(expected2);
    }

    private static List<Row> queryResult(org.apache.flink.table.api.Table table) {
        return CollectionUtil.iteratorToList(table.execute().collect());
    }
}
