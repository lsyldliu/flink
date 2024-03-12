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
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/** ALTER DYNAMIC TABLE ... SET REFRESH_MODE sql call. */
public class SqlAlterDynamicTableRefreshMode extends SqlAlterDynamicTable {

    private final SqlLiteral refreshMode;

    public SqlAlterDynamicTableRefreshMode(
            SqlParserPos pos, SqlIdentifier tableIdentifier, SqlLiteral refreshMode) {
        super(pos, tableIdentifier);
        this.refreshMode = refreshMode;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(tableIdentifier, refreshMode);
    }

    public SqlLiteral getRefreshMode() {
        return refreshMode;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        writer.keyword("SET REFRESH_MODE");
        writer.keyword("=");
        refreshMode.unparse(writer, leftPrec, rightPrec);
    }
}
