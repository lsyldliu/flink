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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.OperatorFusionCodegenOutput;
import org.apache.flink.table.planner.codegen.OperatorFusionCodegenSupport;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

    private final ExecNode<?> rootNode;
    private final List<ExecEdge> originalEdges;

    public BatchExecMultipleInput(
            ReadableConfig tableConfig,
            List<InputProperty> inputProperties,
            ExecNode<?> rootNode,
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
        checkArgument(inputProperties.size() == originalEdges.size());
        this.originalEdges = originalEdges;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
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

        MultipleInputTransformation<RowData> multipleInputTransform = null;
        long memoryBytes = 0;
        boolean multipleCodegenEnabled = config.get(TABLE_EXEC_MULTIPLE_CODEGEN_ENABLED);
        if (multipleCodegenEnabled && supportMultipleCodegen(rootNode, originalEdges)) {
            // multiple operator fusion codegen
            final List<MultipleInputSpec> multipleInputSpecs = new ArrayList<>();
            final List<OperatorFusionCodegenSupport> codegenSupportInputs = new ArrayList<>();
            int i = 0;
            for (ExecEdge inputEdge : originalEdges) {
                int multipleInputId = i + 1;
                BatchExecNode<RowData> source = (BatchExecNode<RowData>) inputEdge.getSource();
                BatchExecInputAdapter inputAdapter =
                        new BatchExecInputAdapter(
                                TableConfig.getDefault(),
                                InputProperty.builder().priority(readOrders[i]).build(),
                                source.getOutputType(),
                                "BatchInputAdapter");
                inputAdapter.setInputEdges(
                        Collections.singletonList(
                                ExecEdge.builder().source(source).target(inputAdapter).build()));

                BatchExecNode<RowData> target = (BatchExecNode<RowData>) inputEdge.getTarget();
                int targetInputIdx = 0;
                for (ExecEdge targetInputEdge : target.getInputEdges()) {
                    if (inputEdge.equals(targetInputEdge)) {
                        target.replaceInputEdge(
                                targetInputIdx,
                                ExecEdge.builder().source(inputAdapter).target(target).build());
                        break;
                    }
                    targetInputIdx++;
                }
                codegenSupportInputs.add(
                        inputAdapter.getInputCodegenOp(multipleInputId, planner, config));
                // The input id and read order
                multipleInputSpecs.add(new MultipleInputSpec(multipleInputId, readOrders[i]));
                i++;
            }

            // wrap root operator of multiple codegen
            OperatorFusionCodegenOutput codegenRootOp =
                    new OperatorFusionCodegenOutput(
                            new CodeGeneratorContext(
                                    config, planner.getFlinkContext().getClassLoader()));
            codegenRootOp.addInput(rootNode.translateToCodegenOp(planner));

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

    private boolean supportMultipleCodegen(ExecNode rootNode, List<ExecEdge> multipleInputEdges) {
        Set<ExecNode> innerNodes = new HashSet<>();
        getAllExecNodes(innerNodes, rootNode, multipleInputEdges);

        return innerNodes.stream()
                .map(BatchExecNode.class::cast)
                .map(BatchExecNode::supportMultipleCodegen)
                .reduce(true, (n1, n2) -> n1 && n2);
    }

    private void getAllExecNodes(
            Set<ExecNode> innerNodes, ExecNode currentNode, List<ExecEdge> multipleInputEdges) {
        List<ExecEdge> inputEdges = currentNode.getInputEdges();
        for (ExecEdge inputEdge : inputEdges) {
            // If this edge is the input edge of multiple input, stop it
            if (!multipleInputEdges.contains(inputEdge)) {
                getAllExecNodes(innerNodes, inputEdge.getSource(), multipleInputEdges);
            }
        }
        innerNodes.add(currentNode);
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
}
