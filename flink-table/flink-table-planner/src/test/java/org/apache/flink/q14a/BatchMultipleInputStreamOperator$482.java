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

public final class BatchMultipleInputStreamOperator$482
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

    public void processInput1(org.apache.flink.table.data.RowData in1) throws Exception {

        // evaluate the required expr in advance

        boolean anyNull$449 = false;
        boolean isNull$448 = in1.isNullAt(0);
        long field$448 = isNull$448 ? -1L : (in1.getLong(0));

        anyNull$449 |= isNull$448;

        if (!anyNull$449) {

            hashTable$450.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in1);
        }
    }

    public void processInput2(org.apache.flink.table.data.RowData in2) throws Exception {

        // evaluate the required expr in advance

        boolean isNull$452 = in2.isNullAt(1);
        long field$452 = isNull$452 ? -1L : (in2.getLong(1));
        boolean result$453 = !isNull$452;
        if (result$453) {

            // evaluate the required expr in advance

            // generate join key for probe side

            // find matches from hash table
            org.apache.flink.table.runtime.util.RowIterator buildIter$457 =
                    isNull$452 ? null : hashTable$450.get(field$452);
            if (buildIter$457 != null) {
                while (buildIter$457.advanceNext()) {
                    org.apache.flink.table.data.RowData buildRow$455 = buildIter$457.getRow();
                    {

                        // evaluate the required expr in advance

                        // evaluate the required expr in advance

                        // wrap variable to row

                        boolean isNull$451 = in2.isNullAt(0);
                        long field$451 = isNull$451 ? -1L : (in2.getLong(0));
                        out_inputRow$444.setLong(0, field$451);

                        output.collect(outElement.replace(out_inputRow$444));
                    }
                }
            }
        }
    }

    public void endInput1() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$450.endBuild();
    }

    public void endInput2() throws Exception {}

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.BoxedWrapperRowData out_inputRow$444 =
            new org.apache.flink.table.data.BoxedWrapperRowData(1);
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$463;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$464;
    Projection$465 buildToBinaryRow$470;
    private transient java.lang.Object[] buildProjRefs$471;
    Projection$467 probeToBinaryRow$470;
    private transient java.lang.Object[] probeProjRefs$472;
    LongHashTable$473 hashTable$450;

    private void hj_init$476(Object[] references) throws Exception {
        buildSer$463 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$464 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$471 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$470 = new Projection$465(buildProjRefs$471);
        probeProjRefs$472 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$470 = new Projection$467(probeProjRefs$472);
    }

    private transient java.lang.Object[] hj_Refs$477;

    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$481;

    public BatchMultipleInputStreamOperator$482(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        hj_Refs$477 = (((java.lang.Object[]) references[0]));
        hj_init$476(hj_Refs$477);
        inputSpecRefs$481 = (((java.util.ArrayList) references[1]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$481);
    }

    @Override
    public void open() throws Exception {
        super.open();

        long memorySize$474 = computeMemorySize(1.0);
        hashTable$450 = new LongHashTable$473(memorySize$474);
    }

    @Override
    public java.util.List getInputs() {
        return java.util.Arrays.asList(
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 1) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`d_date_sk` BIGINT NOT NULL>
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
                        // InputType: ROW<`cs_item_sk` BIGINT NOT NULL, `cs_sold_date_sk` BIGINT>
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
            case 1:
                endInput1();
                break;

            case 2:
                endInput2();
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

        if (this.hashTable$450 != null) {
            this.hashTable$450.close();
            this.hashTable$450.free();
            this.hashTable$450 = null;
        }
    }

    public class Projection$467
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(2);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$467(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$468 = in1.isNullAt(0);
            long field$468 = isNull$468 ? -1L : (in1.getLong(0));
            if (isNull$468) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$468);
            }

            boolean isNull$469 = in1.isNullAt(1);
            long field$469 = isNull$469 ? -1L : (in1.getLong(1));
            if (isNull$469) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$469);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$473
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$473(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$463,
                    probeSer$464,
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
                return probeToBinaryRow$470.apply(row);
            }
        }
    }

    public class Projection$465
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$465(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$466 = in1.isNullAt(0);
            long field$466 = isNull$466 ? -1L : (in1.getLong(0));
            if (isNull$466) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$466);
            }

            outWriter.complete();

            return out;
        }
    }
}
