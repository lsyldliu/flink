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

package org.apache.flink.sql.parser.ddl.dynamic;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nullable;

import java.util.List;

/** ALTER DYNAMIC TABLE ... REFRESH sql call. */
public class SqlAlterDynamicTableRefresh extends SqlAlterDynamicTable {

    private final SqlNodeList partitionSpec;

    public SqlAlterDynamicTableRefresh(
            SqlParserPos pos, SqlIdentifier tableIdentifier, @Nullable SqlNodeList partitionSpec) {
        super(pos, tableIdentifier);
        this.partitionSpec = partitionSpec;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(tableIdentifier, partitionSpec);
    }

    /**
     * Returns the partition spec if the ALTER should be applied to partitions, and null otherwise.
     */
    public SqlNodeList getPartitionSpec() {
        return partitionSpec;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        writer.keyword("REFRESH");
        if (partitionSpec != null && partitionSpec.size() > 0) {
            writer.keyword("PARTITION");
            partitionSpec.unparse(
                    writer, getOperator().getLeftPrec(), getOperator().getRightPrec());
        }
    }
}
