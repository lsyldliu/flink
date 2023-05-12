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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.fusion.RootFusionCodegenSpec;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.AbstractExecNodeExactlyOnceVisitor;
import org.apache.flink.table.runtime.operators.multipleinput.BatchMultipleInputStreamOperatorFactory;
import org.apache.flink.table.runtime.operators.multipleinput.OperatorFusionCodegenFactory;
import org.apache.flink.table.runtime.operators.multipleinput.TableOperatorWrapperGenerator;
import org.apache.flink.table.runtime.operators.multipleinput.input.InputSpec;
import org.apache.flink.table.runtime.operators.multipleinput.input.MultipleInputSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_MULTIPLE_CODEGEN_ENABLED;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Batch {@link ExecNode} for multiple input which contains a sub-graph of {@link ExecNode}s. The
 * root node of the sub-graph is {@link #rootNode}, and the leaf nodes of the sub-graph are the
 * output nodes of the {@link #getInputNodes()}.
 *
 * <p>The following example shows a graph of {@code ExecNode}s with multiple input node:
 *
 * <pre>{@code
 *          Sink
 *           |
 * +---------+--------+
 * |         |        |
 * |       Join       |
 * |     /     \      | BatchExecMultipleInput
 * |   Agg1    Agg2   |
 * |    |       |     |
 * +----+-------+-----+
 *      |       |
 * Exchange1 Exchange2
 *      |       |
 *    Scan1   Scan2
 * }</pre>
 *
 * <p>The multiple input node contains three nodes: `Join`, `Agg1` and `Agg2`. `Join` is the root
 * node ({@link #rootNode}) of the sub-graph, `Agg1` and `Agg2` are the leaf nodes of the sub-graph,
 * `Exchange1` and `Exchange2` are the input nodes of the multiple input node.
 */
public class BatchExecMultipleInput extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(BatchExecMultipleInput.class);

    private static final AtomicLong totalPlanTime = new AtomicLong(0);
    private static final AtomicLong totalMultipleNode = new AtomicLong(0);
    private static final AtomicLong codegenMultipleNode = new AtomicLong(0);
    private static final Map<Integer, Integer> nodeInputMap = new ConcurrentHashMap<>();
    private static final Map<Integer, Integer> codegenNodeInputMap = new ConcurrentHashMap<>();
    private static final Map<String, Map<Integer, Integer>> queryNodeInputMap =
            new ConcurrentHashMap<>();
    private static final Map<String, Map<Integer, Integer>> queryCodegenNodeInputMap =
            new ConcurrentHashMap<>();

    private final ExecNode<?> rootNode;
    private final List<ExecNode<?>> memberExecNodes;
    private final List<ExecEdge> originalEdges;

    public BatchExecMultipleInput(
            ReadableConfig tableConfig,
            List<InputProperty> inputProperties,
            ExecNode<?> rootNode,
            List<ExecNode<?>> memberExecNodes,
            List<ExecEdge> originalEdges,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecMultipleInput.class),
                ExecNodeContext.newPersistedConfig(BatchExecMultipleInput.class, tableConfig),
                inputProperties,
                rootNode.getOutputType(),
                description);
        this.rootNode = rootNode;
        this.memberExecNodes = memberExecNodes;
        checkArgument(inputProperties.size() == originalEdges.size());
        this.originalEdges = originalEdges;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        Long planStartTime = System.currentTimeMillis();
        List<Transformation<?>> inputTransforms = new ArrayList<>();
        final List<ExecEdge> inputEdges = getInputEdges();
        for (ExecEdge inputEdge : inputEdges) {
            inputTransforms.add(inputEdge.translateToPlan(planner));
        }
        final int[] readOrders =
                getInputProperties().stream()
                        .map(InputProperty::getPriority)
                        .mapToInt(i -> i)
                        .toArray();

        totalMultipleNode.incrementAndGet();
        int inputCnt = inputTransforms.size();
        nodeInputMap.put(inputCnt, nodeInputMap.getOrDefault(inputCnt, 0) + 1);
        String jobName = config.get(PipelineOptions.NAME);
        if (jobName == null) {
            jobName = "query";
        }
        // query multiple input distribution
        Map<Integer, Integer> queryMultipleInputCnt =
                queryNodeInputMap.getOrDefault(jobName, new HashMap<>());
        queryMultipleInputCnt.put(inputCnt, queryMultipleInputCnt.getOrDefault(inputCnt, 0) + 1);
        queryNodeInputMap.put(jobName, queryMultipleInputCnt);

        MultipleInputTransformation<RowData> multipleInputTransform = null;
        long memoryBytes = 0;
        boolean multipleCodegenEnabled = config.get(TABLE_EXEC_MULTIPLE_CODEGEN_ENABLED);
        long codegenStartTime = System.currentTimeMillis();
        boolean unsupportNode = unsupportedMultipleInput(config, readOrders);
        if (multipleCodegenEnabled && !unsupportNode && supportMultipleFusionCodegen()) {
            // multiple operator fusion codegen
            final List<MultipleInputSpec> multipleInputSpecs = new ArrayList<>();
            int i = 0;
            for (ExecEdge inputEdge : originalEdges) {
                int multipleInputId = i + 1;
                BatchExecNode<RowData> source = (BatchExecNode<RowData>) inputEdge.getSource();
                BatchExecInputAdapter inputAdapter =
                        new BatchExecInputAdapter(
                                multipleInputId,
                                TableConfig.getDefault(),
                                InputProperty.builder().priority(readOrders[i]).build(),
                                source.getOutputType(),
                                "BatchInputAdapter");
                if (source instanceof BatchExecTableSourceScan) {
                    String tableName = ((BatchExecTableSourceScan) source).getTableName();
                    Transformation sourceTransformation = inputTransforms.get(i);
                    if (sourceTransformation instanceof SourceTransformation
                            && suppportColumnarRead(jobName, tableName)) {
                        ((SourceTransformation) sourceTransformation)
                                .getSource()
                                .setVectorizedRead();
                        // insert a columnar to row ExecNode
                        inputAdapter =
                                new BatchExecColumnarToRow(
                                        multipleInputId,
                                        TableConfig.getDefault(),
                                        InputProperty.builder().priority(readOrders[i]).build(),
                                        source.getOutputType(),
                                        "BatchColumnarToRow");
                    }
                }

                inputAdapter.setInputEdges(
                        Collections.singletonList(
                                ExecEdge.builder().source(source).target(inputAdapter).build()));

                BatchExecNode<RowData> target = (BatchExecNode<RowData>) inputEdge.getTarget();
                int edgeIdxInTargetNode = target.getInputEdges().indexOf(inputEdge);
                checkArgument(edgeIdxInTargetNode >= 0);

                target.replaceInputEdge(
                        edgeIdxInTargetNode,
                        ExecEdge.builder().source(inputAdapter).target(target).build());

                // The input id and read order
                multipleInputSpecs.add(new MultipleInputSpec(multipleInputId, readOrders[i]));
                i++;
            }

            // wrap root operator of multiple codegen
            RootFusionCodegenSpec codegenRootOp =
                    new RootFusionCodegenSpec(
                            new CodeGeneratorContext(
                                    config, planner.getFlinkContext().getClassLoader()));
            codegenRootOp.addInput(rootNode.translateToFusionCodegenSpec(planner));

            // generate multiple input operator
            Tuple2<OperatorFusionCodegenFactory<RowData>, Object> multipleOperatorTuple =
                    codegenRootOp.generateMultipleOperator(multipleInputSpecs);
            Pair<Integer, Integer> parallelismPair = getInputParallelism(inputTransforms);
            multipleInputTransform =
                    new MultipleInputTransformation<>(
                            createTransformationName(config),
                            multipleOperatorTuple._1,
                            InternalTypeInfo.of(getOutputType()),
                            parallelismPair.getLeft(),
                            false);

            if (parallelismPair.getRight() > 0) {
                multipleInputTransform.setMaxParallelism(parallelismPair.getRight());
            }
            memoryBytes = (long) multipleOperatorTuple._2;

            System.out.println(
                    String.format(
                            "Multiple operators fusion codegen spent %sms.",
                            System.currentTimeMillis() - codegenStartTime));
            codegenMultipleNode.incrementAndGet();
            codegenNodeInputMap.put(inputCnt, codegenNodeInputMap.getOrDefault(inputCnt, 0) + 1);
            // query multiple input codegen distribution
            Map<Integer, Integer> queryMultipleInputCodegen =
                    queryCodegenNodeInputMap.getOrDefault(jobName, new HashMap<>());
            queryMultipleInputCodegen.put(
                    inputCnt, queryMultipleInputCodegen.getOrDefault(inputCnt, 0) + 1);
            queryCodegenNodeInputMap.put(jobName, queryMultipleInputCodegen);
        } else {
            final Transformation<?> outputTransform = rootNode.translateToPlan(planner);

            final TableOperatorWrapperGenerator generator =
                    new TableOperatorWrapperGenerator(inputTransforms, outputTransform, readOrders);
            generator.generate();

            final List<Pair<Transformation<?>, InputSpec>> inputTransformAndInputSpecPairs =
                    generator.getInputTransformAndInputSpecPairs();
            multipleInputTransform =
                    new MultipleInputTransformation<>(
                            createTransformationName(config),
                            new BatchMultipleInputStreamOperatorFactory(
                                    inputTransformAndInputSpecPairs.stream()
                                            .map(Pair::getValue)
                                            .collect(Collectors.toList()),
                                    generator.getHeadWrappers(),
                                    generator.getTailWrapper()),
                            InternalTypeInfo.of(getOutputType()),
                            generator.getParallelism(),
                            false);
            if (generator.getMaxParallelism() > 0) {
                multipleInputTransform.setMaxParallelism(generator.getMaxParallelism());
            }
            // set resources
            multipleInputTransform.setResources(
                    generator.getMinResources(), generator.getPreferredResources());
            final int memoryWeight = generator.getManagedMemoryWeight();
            memoryBytes = (long) memoryWeight << 20;
            // here set the all elements of InputTransformation and its id index indicates the order
            inputTransforms =
                    inputTransformAndInputSpecPairs.stream()
                            .map(Pair::getKey)
                            .collect(Collectors.toList());
        }

        multipleInputTransform.setDescription(createTransformationDescription(config));
        LOG.info(
                "BatchExecMultiInput transformation description: \n{}.",
                multipleInputTransform.getDescription());

        for (Transformation input : inputTransforms) {
            multipleInputTransform.addInput(input);
        }
        ExecNodeUtil.setManagedMemoryWeight(multipleInputTransform, memoryBytes);
        // set chaining strategy for source chaining
        multipleInputTransform.setChainingStrategy(ChainingStrategy.HEAD_WITH_SOURCES);

        long planInterval = System.currentTimeMillis() - planStartTime;
        System.out.println(
                String.format(
                        "Total BatchExecMultiInput translate to plan spend time: %s.",
                        totalPlanTime.addAndGet(planInterval)));
        System.out.println(
                String.format(
                        "Total BatchExecMultiInput node num: %s, support codegen node num: %s.",
                        totalMultipleNode.get(), codegenMultipleNode.get()));
        System.out.println(
                String.format(
                        "Total BatchExecMultiInput node distribution: %s, support codegen node distribution: %s.",
                        nodeInputMap, codegenNodeInputMap));
        System.out.println(
                String.format(
                        "Every query BatchExecMultiInput node distribution: %s, support codegen node distribution: %s.",
                        queryNodeInputMap, queryCodegenNodeInputMap));
        return multipleInputTransform;
    }

    @Override
    public void resetTransformation() {
        super.resetTransformation();
        // For BatchExecMultipleInput, we also need to reset transformation for
        // rootNode in BatchExecMultipleInput.
        AbstractExecNodeExactlyOnceVisitor visitor =
                new AbstractExecNodeExactlyOnceVisitor() {

                    @Override
                    protected void visitNode(ExecNode<?> node) {
                        ((ExecNodeBase<?>) node).resetTransformation();
                        visitInputs(node);
                    }
                };
        rootNode.accept(visitor);
    }

    public List<ExecEdge> getOriginalEdges() {
        return originalEdges;
    }

    @VisibleForTesting
    public ExecNode<?> getRootNode() {
        return rootNode;
    }

    private boolean supportMultipleFusionCodegen() {
        boolean supportCodegen =
                memberExecNodes.stream()
                        .map(ExecNode::supportFusionCodegen)
                        .reduce(true, (n1, n2) -> n1 && n2);
        return supportCodegen;
    }

    private Pair<Integer, Integer> getInputParallelism(
            List<Transformation<?>> inputTransformations) {
        int parallelism = -1;
        int maxParallelism = -1;
        for (Transformation<?> transform : inputTransformations) {
            parallelism = Math.max(parallelism, transform.getParallelism());
            int currentMaxParallelism = transform.getMaxParallelism();
            if (maxParallelism < 0) {
                maxParallelism = currentMaxParallelism;
            } else {
                checkState(
                        currentMaxParallelism < 0 || maxParallelism == currentMaxParallelism,
                        "Max parallelism of a transformation in MultipleInput node is different from others. This is a bug.");
            }
        }
        return Pair.of(parallelism, maxParallelism);
    }

    private boolean unsupportedMultipleInput(ExecNodeConfig config, int[] readOrders) {
        boolean unsupportedNode = false;
        String jobName = config.get(PipelineOptions.NAME);
        if ("q54.sql".equals(jobName)) {
            if (readOrders.length == 3 && readOrders[2] == 0) {
                unsupportedNode = true;
            }
        }
        return unsupportedNode;
    }

    private boolean suppportColumnarRead(String jobName, String tableName) {
        if (("q39a.sql".equals(jobName) || "q39b.sql".equals(jobName))
                && "inventory".equals(tableName)) {
            return false;
        }

        if (("q44.sql".equals(jobName) || "q58.sql".equals(jobName) || "q83.sql".equals(jobName))
                && "item".equals(tableName)) {
            return false;
        }

        return true;
    }
}
