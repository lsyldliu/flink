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

public final class BatchMultipleInputStreamOperator$60
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

        boolean anyNull$22 = false;
        boolean isNull$21 = in2.isNullAt(0);
        long field$21 = isNull$21 ? -1L : (in2.getLong(0));

        anyNull$22 |= isNull$21;

        if (!anyNull$22) {

            hashTable$23.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in2);
        }
    }

    public void processInput1(org.apache.flink.table.data.RowData in1) throws Exception {

        // evaluate the required expr in advance

        // generate join key for probe side
        boolean isNull$29 = in1.isNullAt(5);
        long field$29 = isNull$29 ? -1L : (in1.getLong(5));
        // find matches from hash table
        org.apache.flink.table.runtime.util.RowIterator buildIter$32 =
                isNull$29 ? null : hashTable$23.get(field$29);
        if (buildIter$32 != null) {
            while (buildIter$32.advanceNext()) {
                org.apache.flink.table.data.RowData buildRow$30 = buildIter$32.getRow();
                {

                    // evaluate the required expr in advance

                    // evaluate the required expr in advance

                    // wrap variable to row

                    boolean isNull$24 = in1.isNullAt(0);
                    long field$24 = isNull$24 ? -1L : (in1.getLong(0));
                    if (isNull$24) {
                        out_inputRow$18.setNullAt(0);
                    } else {
                        out_inputRow$18.setLong(0, field$24);
                    }

                    boolean isNull$25 = in1.isNullAt(1);
                    double field$25 = isNull$25 ? -1.0d : (in1.getDouble(1));
                    if (isNull$25) {
                        out_inputRow$18.setNullAt(1);
                    } else {
                        out_inputRow$18.setDouble(1, field$25);
                    }

                    boolean isNull$26 = in1.isNullAt(2);
                    double field$26 = isNull$26 ? -1.0d : (in1.getDouble(2));
                    if (isNull$26) {
                        out_inputRow$18.setNullAt(2);
                    } else {
                        out_inputRow$18.setDouble(2, field$26);
                    }

                    boolean isNull$27 = in1.isNullAt(3);
                    double field$27 = isNull$27 ? -1.0d : (in1.getDouble(3));
                    if (isNull$27) {
                        out_inputRow$18.setNullAt(3);
                    } else {
                        out_inputRow$18.setDouble(3, field$27);
                    }

                    boolean isNull$28 = in1.isNullAt(4);
                    double field$28 = isNull$28 ? -1.0d : (in1.getDouble(4));
                    if (isNull$28) {
                        out_inputRow$18.setNullAt(4);
                    } else {
                        out_inputRow$18.setDouble(4, field$28);
                    }

                    output.collect(outElement.replace(out_inputRow$18));
                }
            }
        }
    }

    public void endInput2() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$23.endBuild();
    }

    public void endInput1() throws Exception {}

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.BoxedWrapperRowData out_inputRow$18 =
            new org.apache.flink.table.data.BoxedWrapperRowData(5);
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$38;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$39;
    Projection$40 buildToBinaryRow$49;
    private transient java.lang.Object[] buildProjRefs$50;
    Projection$42 probeToBinaryRow$49;
    private transient java.lang.Object[] probeProjRefs$51;
    LongHashTable$52 hashTable$23;

    private void hj_init$55(Object[] references) throws Exception {
        buildSer$38 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$39 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$50 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$49 = new Projection$40(buildProjRefs$50);
        probeProjRefs$51 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$49 = new Projection$42(probeProjRefs$51);
    }

    private transient java.lang.Object[] hj_Refs$56;

    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$59;

    public BatchMultipleInputStreamOperator$60(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        hj_Refs$56 = (((java.lang.Object[]) references[0]));
        hj_init$55(hj_Refs$56);
        inputSpecRefs$59 = (((java.util.ArrayList) references[1]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$59);
    }

    @Override
    public void open() throws Exception {
        super.open();

        long memorySize$53 = computeMemorySize(1.0);
        hashTable$23 = new LongHashTable$52(memorySize$53);
    }

    @Override
    public java.util.List getInputs() {
        return java.util.Arrays.asList(
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 1) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`ws_bill_customer_sk` BIGINT, `ws_ext_discount_amt`
                        // DOUBLE, `ws_ext_sales_price` DOUBLE, `ws_ext_wholesale_cost` DOUBLE,
                        // `ws_ext_list_price` DOUBLE, `ws_sold_date_sk` BIGINT>
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

        if (this.hashTable$23 != null) {
            this.hashTable$23.close();
            this.hashTable$23.free();
            this.hashTable$23 = null;
        }
    }

    public class Projection$40
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$40(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$41 = in1.isNullAt(0);
            long field$41 = isNull$41 ? -1L : (in1.getLong(0));
            if (isNull$41) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$41);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$42
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(6);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$42(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$43 = in1.isNullAt(0);
            long field$43 = isNull$43 ? -1L : (in1.getLong(0));
            if (isNull$43) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$43);
            }

            boolean isNull$44 = in1.isNullAt(1);
            double field$44 = isNull$44 ? -1.0d : (in1.getDouble(1));
            if (isNull$44) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeDouble(1, field$44);
            }

            boolean isNull$45 = in1.isNullAt(2);
            double field$45 = isNull$45 ? -1.0d : (in1.getDouble(2));
            if (isNull$45) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeDouble(2, field$45);
            }

            boolean isNull$46 = in1.isNullAt(3);
            double field$46 = isNull$46 ? -1.0d : (in1.getDouble(3));
            if (isNull$46) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeDouble(3, field$46);
            }

            boolean isNull$47 = in1.isNullAt(4);
            double field$47 = isNull$47 ? -1.0d : (in1.getDouble(4));
            if (isNull$47) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeDouble(4, field$47);
            }

            boolean isNull$48 = in1.isNullAt(5);
            long field$48 = isNull$48 ? -1L : (in1.getLong(5));
            if (isNull$48) {
                outWriter.setNullAt(5);
            } else {
                outWriter.writeLong(5, field$48);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$52
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$52(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$38,
                    probeSer$39,
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
                return probeToBinaryRow$49.apply(row);
            }
        }
    }
}
