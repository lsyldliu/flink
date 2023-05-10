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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.fusion.OperatorFusionCodegenColumnarToRow;
import org.apache.flink.table.planner.codegen.fusion.OperatorFusionCodegenSupport;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

public class BatchExecColumnarToRow extends BatchExecInputAdapter {

    public BatchExecColumnarToRow(
            ReadableConfig tableConfig,
            InputProperty inputProperty,
            LogicalType outputType,
            String description) {
        super(tableConfig, inputProperty, outputType, description);
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        return (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);
    }

    @Override
    public boolean supportMultipleCodegen() {
        return true;
    }

    @Override
    protected OperatorFusionCodegenSupport translateToCodegenOpInternal(
            PlannerBase planner, ExecNodeConfig config) {
        assert (codegenInput != null);
        return codegenInput;
    }

    @Override
    public OperatorFusionCodegenSupport getInputCodegenOp(
            int multipleInputId, PlannerBase planner, ExecNodeConfig config) {
        codegenInput =
                new OperatorFusionCodegenColumnarToRow(
                        new CodeGeneratorContext(
                                config, planner.getFlinkContext().getClassLoader()),
                        multipleInputId,
                        (RowType) getOutputType());
        return codegenInput;
    }
}
