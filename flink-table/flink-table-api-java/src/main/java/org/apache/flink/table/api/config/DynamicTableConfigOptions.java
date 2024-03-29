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

package org.apache.flink.table.api.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds {@link org.apache.flink.configuration.ConfigOption}s used by table module for
 * dynamic table.
 */
@PublicEvolving
public class DynamicTableConfigOptions {

    // For SqlGateway, always read this option from flink-conf yaml to guarantee that it will not be
    // modified
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Duration> DYNAMIC_TABLE_FRESHNESS_THRESHOLD =
            key("dynamic.table.refresh-mode.freshness-threshold")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(30))
                    .withDescription(
                            "Specifies a time interval for determine the dynamic table refresh mode."
                                    + " If FRESHNESS is less than this threshold, it is running in continuous mode. If it is greater, running in full mode.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<String> PARTITION_FIELDS_DATE_FORMATTER =
            key("partition.fields.#.date-formatter")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Specify the time partition formatter of partitioned dynamic table, # represents the string type partition field."
                                    + " This is used to hint to the framework which partition to refresh in full refresh mode.");
}
