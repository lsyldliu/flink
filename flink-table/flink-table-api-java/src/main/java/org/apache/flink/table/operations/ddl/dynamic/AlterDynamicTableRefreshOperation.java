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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.ObjectIdentifier;

import java.util.Map;

/** Operation to describe a ALTER DYNAMIC TABLE ... REFRESH statement. */
@Internal
public class AlterDynamicTableRefreshOperation extends AlterDynamicTableOperation
        implements DynamicTableOperation {

    // How to handle non string partition field
    // We need to generate the INSERT OVERWRITE statement and the static partition condition
    // For cascaded refresh, we need to generate the ALTER DYNAMIC TABLE ... REFRESH PARTITION()
    // statement
    private final Map<String, String> staticPartitions;

    public AlterDynamicTableRefreshOperation(
            ObjectIdentifier tableIdentifier, Map<String, String> staticPartitions) {
        super(tableIdentifier);
        this.staticPartitions = staticPartitions;
    }

    public Map<String, String> getStaticPartitions() {
        return staticPartitions;
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        throw new TableException("This method shouldn't be called.");
    }

    @Override
    public String asSummaryString() {
        StringBuilder builder =
                new StringBuilder(
                        String.format("ALTER DYNAMIC TABLE %s", tableIdentifier.asSummaryString()));
        if (!staticPartitions.isEmpty()) {
            builder.append("staticPartitions");
            builder.append(staticPartitions);
        }
        return builder.toString();
    }
}
