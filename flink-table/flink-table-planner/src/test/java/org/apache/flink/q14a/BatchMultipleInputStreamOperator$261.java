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

package org.apache.flink.q14a;

public final class BatchMultipleInputStreamOperator$261
        extends org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2
        implements org.apache.flink.streaming.api.operators.MultipleInputStreamOperator,
                org.apache.flink.streaming.api.operators.InputSelectable,
                org.apache.flink.streaming.api.operators.BoundedMultiInput {

    private org.apache.flink.streaming.runtime.tasks.StreamTask<?, ?> getContainingTask() {
        return parameters.getContainingTask();
    }

    private long computeMemorySize(double operatorFraction) {
        final double multipleFraction =
                parameters
                        .getStreamConfig()
                        .getManagedMemoryFractionOperatorUseCaseOfSlot(
                                org.apache.flink.core.memory.ManagedMemoryUseCase.OPERATOR,
                                getRuntimeContext().getTaskManagerRuntimeInfo().getConfiguration(),
                                getRuntimeContext().getUserCodeClassLoader());
        return getContainingTask()
                .getEnvironment()
                .getMemoryManager()
                .computeMemorySize(multipleFraction * operatorFraction);
    }

    public void processInput2(org.apache.flink.table.data.RowData in2) throws Exception {

        // evaluate the required expr in advance

        boolean anyNull$231 = false;
        boolean isNull$230 = in2.isNullAt(0);
        long field$230 = isNull$230 ? -1L : (in2.getLong(0));

        anyNull$231 |= isNull$230;

        if (!anyNull$231) {

            hashTable$232.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in2);
        }
    }

    public void processInput1(org.apache.flink.table.data.RowData in1) throws Exception {

        // evaluate the required expr in advance

        // generate join key for probe side
        boolean isNull$234 = in1.isNullAt(1);
        long field$234 = isNull$234 ? -1L : (in1.getLong(1));
        // find matches from hash table
        org.apache.flink.table.runtime.util.RowIterator buildIter$237 =
                isNull$234 ? null : hashTable$232.get(field$234);
        if (buildIter$237 != null) {
            while (buildIter$237.advanceNext()) {
                org.apache.flink.table.data.RowData buildRow$235 = buildIter$237.getRow();
                {

                    // evaluate the required expr in advance

                    // evaluate the required expr in advance

                    // wrap variable to row

                    boolean isNull$233 = in1.isNullAt(0);
                    long field$233 = isNull$233 ? -1L : (in1.getLong(0));
                    out_inputRow$227.setLong(0, field$233);

                    output.collect(outElement.replace(out_inputRow$227));
                }
            }
        }
    }

    public void endInput2() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$232.endBuild();
    }

    public void endInput1() throws Exception {}

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.BoxedWrapperRowData out_inputRow$227 =
            new org.apache.flink.table.data.BoxedWrapperRowData(1);
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$243;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$244;
    Projection$245 buildToBinaryRow$250;
    private transient java.lang.Object[] buildProjRefs$251;
    Projection$247 probeToBinaryRow$250;
    private transient java.lang.Object[] probeProjRefs$252;
    LongHashTable$253 hashTable$232;

    private void hj_init$256(Object[] references) throws Exception {
        buildSer$243 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$244 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$251 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$250 = new Projection$245(buildProjRefs$251);
        probeProjRefs$252 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$250 = new Projection$247(probeProjRefs$252);
    }

    private transient java.lang.Object[] hj_Refs$257;

    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$260;

    public BatchMultipleInputStreamOperator$261(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        hj_Refs$257 = (((java.lang.Object[]) references[0]));
        hj_init$256(hj_Refs$257);
        inputSpecRefs$260 = (((java.util.ArrayList) references[1]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$260);
    }

    @Override
    public void open() throws Exception {
        super.open();

        long memorySize$254 = computeMemorySize(1.0);
        hashTable$232 = new LongHashTable$253(memorySize$254);
    }

    @Override
    public java.util.List getInputs() {
        return java.util.Arrays.asList(
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 1) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`ws_item_sk` BIGINT NOT NULL, `ws_sold_date_sk` BIGINT>
                        org.apache.flink.table.data.RowData in1 =
                                (org.apache.flink.table.data.RowData) element.getValue();
                        processInput1(in1);
                    }
                },
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 2) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`d_date_sk` BIGINT NOT NULL>
                        org.apache.flink.table.data.RowData in2 =
                                (org.apache.flink.table.data.RowData) element.getValue();
                        processInput2(in2);
                    }
                });
    }

    @Override
    public void endInput(int inputId) throws Exception {
        inputSelectionHandler.endInput(inputId);
        switch (inputId) {
            case 2:
                endInput2();
                break;

            case 1:
                endInput1();
                break;
        }
    }

    @Override
    public org.apache.flink.streaming.api.operators.InputSelection nextSelection() {
        return inputSelectionHandler.getInputSelection();
    }

    @Override
    public void finish() throws Exception {

        super.finish();
    }

    @Override
    public void close() throws Exception {
        super.close();

        if (this.hashTable$232 != null) {
            this.hashTable$232.close();
            this.hashTable$232.free();
            this.hashTable$232 = null;
        }
    }

    public class Projection$245
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$245(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$246 = in1.isNullAt(0);
            long field$246 = isNull$246 ? -1L : (in1.getLong(0));
            if (isNull$246) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$246);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$247
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(2);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$247(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$248 = in1.isNullAt(0);
            long field$248 = isNull$248 ? -1L : (in1.getLong(0));
            if (isNull$248) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$248);
            }

            boolean isNull$249 = in1.isNullAt(1);
            long field$249 = isNull$249 ? -1L : (in1.getLong(1));
            if (isNull$249) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$249);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$253
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$253(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$243,
                    probeSer$244,
                    getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    getContainingTask().getEnvironment().getIOManager(),
                    8,
                    730L / getRuntimeContext().getNumberOfParallelSubtasks());
        }

        @Override
        public long getBuildLongKey(org.apache.flink.table.data.RowData row) {
            return row.getLong(0);
        }

        @Override
        public long getProbeLongKey(org.apache.flink.table.data.RowData row) {
            return row.getLong(1);
        }

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData probeToBinary(
                org.apache.flink.table.data.RowData row) {
            if (row instanceof org.apache.flink.table.data.binary.BinaryRowData) {
                return (org.apache.flink.table.data.binary.BinaryRowData) row;
            } else {
                return probeToBinaryRow$250.apply(row);
            }
        }
    }
}
