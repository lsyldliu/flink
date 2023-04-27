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
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.agg.batch.AggWithoutKeysCodeGenerator;
import org.apache.flink.table.planner.codegen.agg.batch.HashAggCodeGenerator;
import org.apache.flink.table.planner.codegen.fusion.OperatorFusionCodegenHashAgg;
import org.apache.flink.table.planner.codegen.fusion.OperatorFusionCodegenLocalHashAgg;
import org.apache.flink.table.planner.codegen.fusion.OperatorFusionCodegenSupport;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedOperator;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.core.AggregateCall;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

/** Batch {@link ExecNode} for hash-based aggregate operator. */
public class BatchExecHashAggregate extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    private final int[] grouping;
    private final int[] auxGrouping;
    private final AggregateCall[] aggCalls;
    private final RowType aggInputRowType;
    private final boolean isMerge;
    private final boolean isFinal;
    private final boolean supportAdaptiveLocalHashAgg;

    private OperatorFusionCodegenSupport hashAgg;

    public BatchExecHashAggregate(
            ReadableConfig tableConfig,
            int[] grouping,
            int[] auxGrouping,
            AggregateCall[] aggCalls,
            RowType aggInputRowType,
            boolean isMerge,
            boolean isFinal,
            boolean supportAdaptiveLocalHashAgg,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecHashAggregate.class),
                ExecNodeContext.newPersistedConfig(BatchExecHashAggregate.class, tableConfig),
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.grouping = grouping;
        this.auxGrouping = auxGrouping;
        this.aggCalls = aggCalls;
        this.aggInputRowType = aggInputRowType;
        this.isMerge = isMerge;
        this.isFinal = isFinal;
        this.supportAdaptiveLocalHashAgg = supportAdaptiveLocalHashAgg;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        final RowType inputRowType = (RowType) inputEdge.getOutputType();
        final RowType outputRowType = (RowType) getOutputType();

        final CodeGeneratorContext ctx =
                new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader());

        final AggregateInfoList aggInfos =
                AggregateUtil.transformToBatchAggregateInfoList(
                        planner.getTypeFactory(),
                        aggInputRowType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        null, // aggCallNeedRetractions
                        null); // orderKeyIndexes

        final long managedMemory;
        final GeneratedOperator<OneInputStreamOperator<RowData, RowData>> generatedOperator;
        if (grouping.length == 0) {
            managedMemory = 0L;
            generatedOperator =
                    AggWithoutKeysCodeGenerator.genWithoutKeys(
                            ctx,
                            planner.createRelBuilder(),
                            aggInfos,
                            inputRowType,
                            outputRowType,
                            isMerge,
                            isFinal,
                            "NoGrouping");
        } else {
            managedMemory =
                    config.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_HASH_AGG_MEMORY)
                            .getBytes();
            generatedOperator =
                    HashAggCodeGenerator.genWithKeys(
                            ctx,
                            planner.createRelBuilder(),
                            aggInfos,
                            inputRowType,
                            outputRowType,
                            grouping,
                            auxGrouping,
                            isMerge,
                            isFinal,
                            supportAdaptiveLocalHashAgg,
                            config.get(ExecutionConfigOptions.TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES),
                            config.get(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED),
                            (int)
                                    config.get(
                                                    ExecutionConfigOptions
                                                            .TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE)
                                            .getBytes());
        }

        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationName(config),
                createTransformationDescription(config),
                new CodeGenOperatorFactory<>(generatedOperator),
                InternalTypeInfo.of(outputRowType),
                inputTransform.getParallelism(),
                managedMemory,
                false);
    }

    public boolean isFinal() {
        return isFinal;
    }

    @Override
    public boolean supportMultipleCodegen() {
        // We don't support operator fusion codegen when aggCalls has filter now
        return Stream.of(aggCalls).noneMatch(AggregateCall::hasFilter);
    }

    @Override
    protected OperatorFusionCodegenSupport translateToCodegenOpInternal(
            PlannerBase planner, ExecNodeConfig config) {
        OperatorFusionCodegenSupport input = getInputEdges().get(0).translateToCodegenOp(planner);
        final RowType outputRowType = (RowType) getOutputType();
        final CodeGeneratorContext ctx =
                new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader());
        final AggregateInfoList aggInfos =
                AggregateUtil.transformToBatchAggregateInfoList(
                        planner.getTypeFactory(),
                        aggInputRowType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        null, // aggCallNeedRetractions
                        null); // orderKeyIndexes

        long managedMemory =
                config.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_HASH_AGG_MEMORY).getBytes();
        if (grouping.length == 0) {
            managedMemory = 0L;
        }
        int maxNumFileHandles =
                config.get(ExecutionConfigOptions.TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES);
        boolean compressionEnabled =
                config.get(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED);
        int compressionBlockSize =
                (int)
                        config.get(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE)
                                .getBytes();
        if (isFinal) {
            hashAgg =
                    new OperatorFusionCodegenHashAgg(
                            ctx,
                            planner.createRelBuilder(),
                            aggInfos,
                            outputRowType,
                            grouping,
                            auxGrouping,
                            isMerge,
                            managedMemory,
                            maxNumFileHandles,
                            compressionEnabled,
                            compressionBlockSize);
        } else {
            hashAgg =
                    new OperatorFusionCodegenLocalHashAgg(
                            ctx,
                            planner.createRelBuilder(),
                            aggInfos,
                            outputRowType,
                            grouping,
                            auxGrouping,
                            supportAdaptiveLocalHashAgg);
        }

        hashAgg.addInput(input);
        return hashAgg;
    }
}
