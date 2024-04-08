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

package org.apache.flink.table.planner.operations;

import org.apache.flink.sql.parser.ddl.dynamic.SqlRefreshMode;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.dynamic.CatalogDynamicTable;

import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.type.SqlTypeFamily;

import java.time.Duration;

/** The utils for dynamic table. */
public class DynamicTableUtil {

    public static Duration getDynamicTableFreshness(SqlIntervalLiteral sqlIntervalLiteral) {
        if (sqlIntervalLiteral.getTypeName().getFamily() != SqlTypeFamily.INTERVAL_DAY_TIME) {
            throw new ValidationException(
                    "Dynamic Table freshness only support SECOND, MINUTE, HOUR, DAY as the time unit.");
        }
        if (sqlIntervalLiteral.signum() < 0) {
            throw new ValidationException(
                    "Dynamic Table freshness doesn't support negative value.");
        }

        SqlIntervalLiteral.IntervalValue intervalValue =
                sqlIntervalLiteral.getValueAs(SqlIntervalLiteral.IntervalValue.class);
        long interval = Long.parseLong(intervalValue.getIntervalLiteral());
        switch (intervalValue.getIntervalQualifier().typeName()) {
            case INTERVAL_DAY:
                return Duration.ofDays(interval);
            case INTERVAL_HOUR:
                return Duration.ofHours(interval);
            case INTERVAL_MINUTE:
                return Duration.ofMinutes(interval);
            case INTERVAL_SECOND:
                return Duration.ofSeconds(interval);
            default:
                throw new ValidationException(
                        String.format(
                                "Unsupported freshness time type: %s.",
                                intervalValue.getIntervalQualifier().typeName()));
        }
    }

    public static CatalogDynamicTable.LogicalRefreshMode getLogicalRefreshMode(
            SqlRefreshMode sqlRefreshMode) {
        if (sqlRefreshMode == null) {
            return CatalogDynamicTable.LogicalRefreshMode.AUTOMATIC;
        }

        switch (sqlRefreshMode) {
            case FULL:
                return CatalogDynamicTable.LogicalRefreshMode.FULL;
            case CONTINUOUS:
                return CatalogDynamicTable.LogicalRefreshMode.CONTINUOUS;
            default:
                throw new ValidationException(
                        String.format("Unsupported refresh mode: %s.", sqlRefreshMode));
        }
    }

    public static CatalogDynamicTable.RefreshMode deriveRefreshMode(
            Duration threshold,
            Duration specifiedFreshness,
            CatalogDynamicTable.LogicalRefreshMode specifiedRefreshMode) {
        // If the refresh mode is specified manually, so use it.
        if (specifiedRefreshMode == CatalogDynamicTable.LogicalRefreshMode.FULL) {
            return CatalogDynamicTable.RefreshMode.FULL;
        }
        if (specifiedRefreshMode == CatalogDynamicTable.LogicalRefreshMode.CONTINUOUS) {
            return CatalogDynamicTable.RefreshMode.CONTINUOUS;
        }

        // derive the refresh mode via freshness
        if (specifiedFreshness.compareTo(threshold) <= 0) {
            return CatalogDynamicTable.RefreshMode.CONTINUOUS;
        } else {
            return CatalogDynamicTable.RefreshMode.FULL;
        }
    }
}
