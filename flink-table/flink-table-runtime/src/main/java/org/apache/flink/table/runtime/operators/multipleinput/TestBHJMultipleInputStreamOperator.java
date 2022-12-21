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

package org.apache.flink.table.runtime.operators.multipleinput;

import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler;
import org.apache.flink.table.runtime.operators.multipleinput.input.InputSpec;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;

import java.util.Arrays;
import java.util.List;

/** A {@link MultipleInputStreamOperatorBase} to handle batch operators. */
public class TestBHJMultipleInputStreamOperator extends AbstractStreamOperatorV2<RowData>
        implements MultipleInputStreamOperator<RowData>, BoundedMultiInput, InputSelectable {
    private static final long serialVersionUID = 1L;

    private final StreamOperatorParameters<RowData> parameters;
    private final StreamRecord outElement = new StreamRecord(null);

    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$571;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$572;
    Projection$573 buildToBinaryRow;
    Projection$576 probeToBinaryRow;
    ConditionFunction$541 condFunc;
    org.apache.flink.table.data.GenericRowData buildSideNullRow =
            new org.apache.flink.table.data.GenericRowData(2);
    org.apache.flink.table.data.utils.JoinedRowData joinedRow =
            new org.apache.flink.table.data.utils.JoinedRowData();
    LongHashTable$570 table;

    private final InputSelectionHandler inputSelectionHandler;

    public TestBHJMultipleInputStreamOperator(
            StreamOperatorParameters<RowData> parameters, List<InputSpec> inputSpecs) {
        super(parameters, inputSpecs.size());
        this.parameters = parameters;
        this.inputSelectionHandler = new InputSelectionHandler(inputSpecs);

        buildSer$571 = new BinaryRowDataSerializer(2);
        probeSer$572 = new BinaryRowDataSerializer(1);
        buildToBinaryRow = new Projection$573();
        probeToBinaryRow = new Projection$576();
        condFunc = new ConditionFunction$541();
    }

    @Override
    public void open() throws Exception {
        super.open();
        condFunc.setRuntimeContext(getRuntimeContext());
        condFunc.open(new org.apache.flink.configuration.Configuration());

        getMetricGroup()
                .gauge(
                        "memoryUsedSizeInBytes",
                        new org.apache.flink.metrics.Gauge<Long>() {
                            @Override
                            public Long getValue() {
                                return table.getUsedMemoryInBytes();
                            }
                        });
        getMetricGroup()
                .gauge(
                        "numSpillFiles",
                        new org.apache.flink.metrics.Gauge<Long>() {
                            @Override
                            public Long getValue() {
                                return table.getNumSpillFiles();
                            }
                        });
        getMetricGroup()
                .gauge(
                        "spillInBytes",
                        new org.apache.flink.metrics.Gauge<Long>() {
                            @Override
                            public Long getValue() {
                                return table.getSpillInBytes();
                            }
                        });

        table = new LongHashTable$570(parameters);
    }

    @Override
    public void endInput(int inputId) throws Exception {
        inputSelectionHandler.endInput(inputId);
        switch (inputId) {
            case 2:
                // build hashtable
                LOG.info("Finish build phase.");
                this.table.endBuild();
                break;
            case 1:
                // calc and probe hashtable
                LOG.info("Finish probe phase.");
                while (this.table.nextMatching()) {
                    joinWithNextKey();
                }
                LOG.info("Finish rebuild phase.");
                break;
        }
    }

    @Override
    public InputSelection nextSelection() {
        return inputSelectionHandler.getInputSelection();
    }

    @Override
    public List<Input> getInputs() {
        return Arrays.asList(
                new AbstractInput(this, 1) {
                    @Override
                    public void processElement(StreamRecord element) throws Exception {
                        factTableCalc(element);
                    }
                },
                new AbstractInput(this, 2) {
                    @Override
                    public void processElement(StreamRecord element) throws Exception {
                        processElement1(element);
                    }
                });
    }

    @Override
    public void close() throws Exception {
        super.close();
        condFunc.close();

        closeHashTable();
    }

    private void closeHashTable() {
        if (this.table != null) {
            this.table.close();
            this.table.free();
            this.table = null;
        }
    }

    private void factTableCalc(StreamRecord<RowData> streamRecord) throws Exception {
        RowData in1 = streamRecord.getValue();
        long field$538;
        boolean isNull$538;

        isNull$538 = in1.isNullAt(0);
        field$538 = -1L;
        if (!isNull$538) {
            // 没必要取值
            field$538 = in1.getLong(0);
        }
        boolean result$539 = !isNull$538;
        if (result$539) {
            processElement2(streamRecord);
        }
    }

    public void processElement1(
            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element) throws Exception {
        org.apache.flink.table.data.RowData row =
                (org.apache.flink.table.data.RowData) element.getValue();

        boolean anyNull$584 = false;
        anyNull$584 |= row.isNullAt(0);

        if (!anyNull$584) {
            table.putBuildRow(
                    row instanceof org.apache.flink.table.data.binary.BinaryRowData
                            ? (org.apache.flink.table.data.binary.BinaryRowData) row
                            : buildToBinaryRow.apply(row));
        }
    }

    public void processElement2(
            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element) throws Exception {
        org.apache.flink.table.data.RowData row =
                (org.apache.flink.table.data.RowData) element.getValue();

        boolean anyNull$585 = false;
        anyNull$585 |= row.isNullAt(0);

        if (!anyNull$585) {
            if (table.tryProbe(row)) {
                joinWithNextKey();
            }
        }
    }

    private void joinWithNextKey() throws Exception {
        org.apache.flink.table.runtime.hashtable.LongHashPartition.MatchIterator buildIter =
                table.getBuildSideIterator();
        org.apache.flink.table.data.RowData probeRow = table.getCurrentProbeRow();
        if (probeRow == null) {
            throw new RuntimeException("ProbeRow should not be null");
        }

        while (buildIter.advanceNext()) {
            if (condFunc.apply(probeRow, buildIter.getRow())) {
                output.collect(outElement.replace(joinedRow.replace(probeRow, buildIter.getRow())));
            }
        }
    }

    /** Compute memory size from memory faction. */
    public long computeMemorySize(StreamOperatorParameters<RowData> parameters) {
        final Environment environment = parameters.getContainingTask().getEnvironment();
        return environment
                .getMemoryManager()
                .computeMemorySize(
                        getOperatorConfig()
                                .getManagedMemoryFractionOperatorUseCaseOfSlot(
                                        ManagedMemoryUseCase.OPERATOR,
                                        environment.getTaskManagerInfo().getConfiguration(),
                                        environment.getUserCodeClassLoader().asClassLoader()));
    }

    public class LongHashTable$570
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$570(StreamOperatorParameters<RowData> parameters) {
            super(
                    parameters.getContainingTask().getJobConfiguration(),
                    parameters.getContainingTask(),
                    buildSer$571,
                    probeSer$572,
                    parameters.getContainingTask().getEnvironment().getMemoryManager(),
                    computeMemorySize(parameters),
                    parameters.getContainingTask().getEnvironment().getIOManager(),
                    12,
                    1L / getRuntimeContext().getNumberOfParallelSubtasks());
        }

        @Override
        public long getBuildLongKey(org.apache.flink.table.data.RowData row) {
            return row.getLong(0);
        }

        @Override
        public long getProbeLongKey(org.apache.flink.table.data.RowData row) {
            return row.getLong(0);
        }

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData probeToBinary(
                org.apache.flink.table.data.RowData row) {
            if (row instanceof org.apache.flink.table.data.binary.BinaryRowData) {
                return (org.apache.flink.table.data.binary.BinaryRowData) row;
            } else {
                return probeToBinaryRow.apply(row);
            }
        }
    }

    public class Projection$576
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {
            long field$577;
            boolean isNull$577;

            outWriter.reset();

            isNull$577 = in1.isNullAt(0);
            field$577 = -1L;
            if (!isNull$577) {
                field$577 = in1.getLong(0);
            }
            if (isNull$577) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$577);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$573
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(2);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {
            long field$574;
            boolean isNull$574;
            int field$575;
            boolean isNull$575;

            outWriter.reset();

            isNull$574 = in1.isNullAt(0);
            field$574 = -1L;
            if (!isNull$574) {
                field$574 = in1.getLong(0);
            }
            if (isNull$574) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$574);
            }

            isNull$575 = in1.isNullAt(1);
            field$575 = -1;
            if (!isNull$575) {
                field$575 = in1.getInt(1);
            }
            if (isNull$575) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeInt(1, field$575);
            }

            outWriter.complete();

            return out;
        }
    }

    public class ConditionFunction$541
            extends org.apache.flink.api.common.functions.AbstractRichFunction
            implements org.apache.flink.table.runtime.generated.JoinCondition {

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters)
                throws Exception {}

        @Override
        public boolean apply(
                org.apache.flink.table.data.RowData in1, org.apache.flink.table.data.RowData in2) {

            return true;
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }
}
