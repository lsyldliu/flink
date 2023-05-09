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

package org.apache.flink.q4;

public final class BatchMultipleInputStreamOperator$516
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

        boolean anyNull$475 = false;
        boolean isNull$474 = in1.isNullAt(0);
        long field$474 = isNull$474 ? -1L : (in1.getLong(0));

        anyNull$475 |= isNull$474;

        if (!anyNull$475) {

            hashTable$476.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in1);
        }
    }

    public void processInput2(org.apache.flink.table.data.RowData in2) throws Exception {

        // evaluate the required expr in advance

        boolean isNull$482 = in2.isNullAt(5);
        long field$482 = isNull$482 ? -1L : (in2.getLong(5));
        boolean result$483 = !isNull$482;
        if (result$483) {

            // evaluate the required expr in advance

            // generate join key for probe side

            // find matches from hash table
            org.apache.flink.table.runtime.util.RowIterator buildIter$487 =
                    isNull$482 ? null : hashTable$476.get(field$482);
            if (buildIter$487 != null) {
                while (buildIter$487.advanceNext()) {
                    org.apache.flink.table.data.RowData buildRow$485 = buildIter$487.getRow();
                    {

                        // evaluate the required expr in advance

                        // evaluate the required expr in advance

                        // wrap variable to row

                        boolean isNull$477 = in2.isNullAt(0);
                        long field$477 = isNull$477 ? -1L : (in2.getLong(0));
                        if (isNull$477) {
                            out_inputRow$470.setNullAt(0);
                        } else {
                            out_inputRow$470.setLong(0, field$477);
                        }

                        boolean isNull$478 = in2.isNullAt(1);
                        double field$478 = isNull$478 ? -1.0d : (in2.getDouble(1));
                        if (isNull$478) {
                            out_inputRow$470.setNullAt(1);
                        } else {
                            out_inputRow$470.setDouble(1, field$478);
                        }

                        boolean isNull$479 = in2.isNullAt(2);
                        double field$479 = isNull$479 ? -1.0d : (in2.getDouble(2));
                        if (isNull$479) {
                            out_inputRow$470.setNullAt(2);
                        } else {
                            out_inputRow$470.setDouble(2, field$479);
                        }

                        boolean isNull$480 = in2.isNullAt(3);
                        double field$480 = isNull$480 ? -1.0d : (in2.getDouble(3));
                        if (isNull$480) {
                            out_inputRow$470.setNullAt(3);
                        } else {
                            out_inputRow$470.setDouble(3, field$480);
                        }

                        boolean isNull$481 = in2.isNullAt(4);
                        double field$481 = isNull$481 ? -1.0d : (in2.getDouble(4));
                        if (isNull$481) {
                            out_inputRow$470.setNullAt(4);
                        } else {
                            out_inputRow$470.setDouble(4, field$481);
                        }

                        output.collect(outElement.replace(out_inputRow$470));
                    }
                }
            }
        }
    }

    public void endInput1() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$476.endBuild();
    }

    public void endInput2() throws Exception {}

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.BoxedWrapperRowData out_inputRow$470 =
            new org.apache.flink.table.data.BoxedWrapperRowData(5);
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$493;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$494;
    Projection$495 buildToBinaryRow$504;
    private transient java.lang.Object[] buildProjRefs$505;
    Projection$497 probeToBinaryRow$504;
    private transient java.lang.Object[] probeProjRefs$506;
    LongHashTable$507 hashTable$476;

    private void hj_init$510(Object[] references) throws Exception {
        buildSer$493 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$494 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$505 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$504 = new Projection$495(buildProjRefs$505);
        probeProjRefs$506 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$504 = new Projection$497(probeProjRefs$506);
    }

    private transient java.lang.Object[] hj_Refs$511;

    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$515;

    public BatchMultipleInputStreamOperator$516(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        hj_Refs$511 = (((java.lang.Object[]) references[0]));
        hj_init$510(hj_Refs$511);
        inputSpecRefs$515 = (((java.util.ArrayList) references[1]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$515);
    }

    @Override
    public void open() throws Exception {
        super.open();

        long memorySize$508 = computeMemorySize(1.0);
        hashTable$476 = new LongHashTable$507(memorySize$508);
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
                        // InputType: ROW<`ss_customer_sk` BIGINT, `ss_ext_discount_amt` DOUBLE,
                        // `ss_ext_sales_price` DOUBLE, `ss_ext_wholesale_cost` DOUBLE,
                        // `ss_ext_list_price` DOUBLE, `ss_sold_date_sk` BIGINT>
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

        if (this.hashTable$476 != null) {
            this.hashTable$476.close();
            this.hashTable$476.free();
            this.hashTable$476 = null;
        }
    }

    public class LongHashTable$507
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$507(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$493,
                    probeSer$494,
                    getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    getContainingTask().getEnvironment().getIOManager(),
                    8,
                    367L / getRuntimeContext().getNumberOfParallelSubtasks());
        }

        @Override
        public long getBuildLongKey(org.apache.flink.table.data.RowData row) {
            return row.getLong(0);
        }

        @Override
        public long getProbeLongKey(org.apache.flink.table.data.RowData row) {
            return row.getLong(5);
        }

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData probeToBinary(
                org.apache.flink.table.data.RowData row) {
            if (row instanceof org.apache.flink.table.data.binary.BinaryRowData) {
                return (org.apache.flink.table.data.binary.BinaryRowData) row;
            } else {
                return probeToBinaryRow$504.apply(row);
            }
        }
    }

    public class Projection$495
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$495(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$496 = in1.isNullAt(0);
            long field$496 = isNull$496 ? -1L : (in1.getLong(0));
            if (isNull$496) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$496);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$497
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(6);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$497(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$498 = in1.isNullAt(0);
            long field$498 = isNull$498 ? -1L : (in1.getLong(0));
            if (isNull$498) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$498);
            }

            boolean isNull$499 = in1.isNullAt(1);
            double field$499 = isNull$499 ? -1.0d : (in1.getDouble(1));
            if (isNull$499) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeDouble(1, field$499);
            }

            boolean isNull$500 = in1.isNullAt(2);
            double field$500 = isNull$500 ? -1.0d : (in1.getDouble(2));
            if (isNull$500) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeDouble(2, field$500);
            }

            boolean isNull$501 = in1.isNullAt(3);
            double field$501 = isNull$501 ? -1.0d : (in1.getDouble(3));
            if (isNull$501) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeDouble(3, field$501);
            }

            boolean isNull$502 = in1.isNullAt(4);
            double field$502 = isNull$502 ? -1.0d : (in1.getDouble(4));
            if (isNull$502) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeDouble(4, field$502);
            }

            boolean isNull$503 = in1.isNullAt(5);
            long field$503 = isNull$503 ? -1L : (in1.getLong(5));
            if (isNull$503) {
                outWriter.setNullAt(5);
            } else {
                outWriter.writeLong(5, field$503);
            }

            outWriter.complete();

            return out;
        }
    }
}
