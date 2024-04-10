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

import org.apache.flink.sql.parser.SqlProperty;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.NlsString;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.List;

/** ALTER DYNAMIC TABLE ... REFRESH sql call. */
public class SqlAlterDynamicTableRefresh extends SqlAlterDynamicTable {

    private final SqlNodeList staticPartitions;

    public SqlAlterDynamicTableRefresh(
            SqlParserPos pos,
            SqlIdentifier tableIdentifier,
            @Nullable SqlNodeList staticPartitions) {
        super(pos, tableIdentifier);
        this.staticPartitions = staticPartitions;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(tableIdentifier, staticPartitions);
    }

    /**
     * Returns the partition spec if the ALTER should be applied to partitions, and null otherwise.
     */
    public SqlNodeList getPartitionSpecs() {
        return staticPartitions;
    }

    /**
     * Get static partition key value pair as strings.
     *
     * <p>For character literals we return the unquoted and unescaped values. For other types we use
     * {@link SqlLiteral#toString()} to get the string format of the value literal. If the string
     * format is not what you need, use {@link #getStaticPartitions()}.
     *
     * @return the mapping of column names to values of partition specifications, returns an empty
     *     map if there is no partition specifications.
     */
    public LinkedHashMap<String, String> getStaticPartitionKVs() {
        LinkedHashMap<String, String> ret = new LinkedHashMap<>();
        if (this.staticPartitions.size() == 0) {
            return ret;
        }
        for (SqlNode node : this.staticPartitions.getList()) {
            SqlProperty sqlProperty = (SqlProperty) node;
            Comparable comparable = SqlLiteral.value(sqlProperty.getValue());
            String value =
                    comparable instanceof NlsString
                            ? ((NlsString) comparable).getValue()
                            : comparable.toString();
            ret.put(sqlProperty.getKey().getSimple(), value);
        }
        return ret;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        writer.keyword("REFRESH");
        if (staticPartitions != null && staticPartitions.size() > 0) {
            writer.keyword("PARTITION");
            staticPartitions.unparse(
                    writer, getOperator().getLeftPrec(), getOperator().getRightPrec());
        }
    }
}
