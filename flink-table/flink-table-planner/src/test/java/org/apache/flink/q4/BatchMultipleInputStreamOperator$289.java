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

public final class BatchMultipleInputStreamOperator$289
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

        boolean anyNull$251 = false;
        boolean isNull$250 = in2.isNullAt(0);
        long field$250 = isNull$250 ? -1L : (in2.getLong(0));

        anyNull$251 |= isNull$250;

        if (!anyNull$251) {

            hashTable$252.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in2);
        }
    }

    public void processInput1(org.apache.flink.table.data.RowData in1) throws Exception {

        // evaluate the required expr in advance

        // generate join key for probe side
        boolean isNull$258 = in1.isNullAt(5);
        long field$258 = isNull$258 ? -1L : (in1.getLong(5));
        // find matches from hash table
        org.apache.flink.table.runtime.util.RowIterator buildIter$261 =
                isNull$258 ? null : hashTable$252.get(field$258);
        if (buildIter$261 != null) {
            while (buildIter$261.advanceNext()) {
                org.apache.flink.table.data.RowData buildRow$259 = buildIter$261.getRow();
                {

                    // evaluate the required expr in advance

                    // evaluate the required expr in advance

                    // wrap variable to row

                    boolean isNull$253 = in1.isNullAt(0);
                    long field$253 = isNull$253 ? -1L : (in1.getLong(0));
                    if (isNull$253) {
                        out_inputRow$247.setNullAt(0);
                    } else {
                        out_inputRow$247.setLong(0, field$253);
                    }

                    boolean isNull$254 = in1.isNullAt(1);
                    double field$254 = isNull$254 ? -1.0d : (in1.getDouble(1));
                    if (isNull$254) {
                        out_inputRow$247.setNullAt(1);
                    } else {
                        out_inputRow$247.setDouble(1, field$254);
                    }

                    boolean isNull$255 = in1.isNullAt(2);
                    double field$255 = isNull$255 ? -1.0d : (in1.getDouble(2));
                    if (isNull$255) {
                        out_inputRow$247.setNullAt(2);
                    } else {
                        out_inputRow$247.setDouble(2, field$255);
                    }

                    boolean isNull$256 = in1.isNullAt(3);
                    double field$256 = isNull$256 ? -1.0d : (in1.getDouble(3));
                    if (isNull$256) {
                        out_inputRow$247.setNullAt(3);
                    } else {
                        out_inputRow$247.setDouble(3, field$256);
                    }

                    boolean isNull$257 = in1.isNullAt(4);
                    double field$257 = isNull$257 ? -1.0d : (in1.getDouble(4));
                    if (isNull$257) {
                        out_inputRow$247.setNullAt(4);
                    } else {
                        out_inputRow$247.setDouble(4, field$257);
                    }

                    output.collect(outElement.replace(out_inputRow$247));
                }
            }
        }
    }

    public void endInput2() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$252.endBuild();
    }

    public void endInput1() throws Exception {}

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.BoxedWrapperRowData out_inputRow$247 =
            new org.apache.flink.table.data.BoxedWrapperRowData(5);
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$267;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$268;
    Projection$269 buildToBinaryRow$278;
    private transient java.lang.Object[] buildProjRefs$279;
    Projection$271 probeToBinaryRow$278;
    private transient java.lang.Object[] probeProjRefs$280;
    LongHashTable$281 hashTable$252;

    private void hj_init$284(Object[] references) throws Exception {
        buildSer$267 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$268 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$279 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$278 = new Projection$269(buildProjRefs$279);
        probeProjRefs$280 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$278 = new Projection$271(probeProjRefs$280);
    }

    private transient java.lang.Object[] hj_Refs$285;

    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$288;

    public BatchMultipleInputStreamOperator$289(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        hj_Refs$285 = (((java.lang.Object[]) references[0]));
        hj_init$284(hj_Refs$285);
        inputSpecRefs$288 = (((java.util.ArrayList) references[1]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$288);
    }

    @Override
    public void open() throws Exception {
        super.open();

        long memorySize$282 = computeMemorySize(1.0);
        hashTable$252 = new LongHashTable$281(memorySize$282);
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

        if (this.hashTable$252 != null) {
            this.hashTable$252.close();
            this.hashTable$252.free();
            this.hashTable$252 = null;
        }
    }

    public class Projection$269
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$269(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$270 = in1.isNullAt(0);
            long field$270 = isNull$270 ? -1L : (in1.getLong(0));
            if (isNull$270) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$270);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$271
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(6);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$271(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$272 = in1.isNullAt(0);
            long field$272 = isNull$272 ? -1L : (in1.getLong(0));
            if (isNull$272) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$272);
            }

            boolean isNull$273 = in1.isNullAt(1);
            double field$273 = isNull$273 ? -1.0d : (in1.getDouble(1));
            if (isNull$273) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeDouble(1, field$273);
            }

            boolean isNull$274 = in1.isNullAt(2);
            double field$274 = isNull$274 ? -1.0d : (in1.getDouble(2));
            if (isNull$274) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeDouble(2, field$274);
            }

            boolean isNull$275 = in1.isNullAt(3);
            double field$275 = isNull$275 ? -1.0d : (in1.getDouble(3));
            if (isNull$275) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeDouble(3, field$275);
            }

            boolean isNull$276 = in1.isNullAt(4);
            double field$276 = isNull$276 ? -1.0d : (in1.getDouble(4));
            if (isNull$276) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeDouble(4, field$276);
            }

            boolean isNull$277 = in1.isNullAt(5);
            long field$277 = isNull$277 ? -1L : (in1.getLong(5));
            if (isNull$277) {
                outWriter.setNullAt(5);
            } else {
                outWriter.writeLong(5, field$277);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$281
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$281(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$267,
                    probeSer$268,
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
                return probeToBinaryRow$278.apply(row);
            }
        }
    }
}
