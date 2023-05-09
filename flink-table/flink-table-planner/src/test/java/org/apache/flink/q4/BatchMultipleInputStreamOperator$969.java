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

public final class BatchMultipleInputStreamOperator$969
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

        boolean anyNull$928 = false;
        boolean isNull$927 = in1.isNullAt(0);
        long field$927 = isNull$927 ? -1L : (in1.getLong(0));

        anyNull$928 |= isNull$927;

        if (!anyNull$928) {

            hashTable$929.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in1);
        }
    }

    public void processInput2(org.apache.flink.table.data.RowData in2) throws Exception {

        // evaluate the required expr in advance

        boolean isNull$935 = in2.isNullAt(5);
        long field$935 = isNull$935 ? -1L : (in2.getLong(5));
        boolean result$936 = !isNull$935;
        if (result$936) {

            // evaluate the required expr in advance

            // generate join key for probe side

            // find matches from hash table
            org.apache.flink.table.runtime.util.RowIterator buildIter$940 =
                    isNull$935 ? null : hashTable$929.get(field$935);
            if (buildIter$940 != null) {
                while (buildIter$940.advanceNext()) {
                    org.apache.flink.table.data.RowData buildRow$938 = buildIter$940.getRow();
                    {

                        // evaluate the required expr in advance

                        // evaluate the required expr in advance

                        // wrap variable to row

                        boolean isNull$930 = in2.isNullAt(0);
                        long field$930 = isNull$930 ? -1L : (in2.getLong(0));
                        if (isNull$930) {
                            out_inputRow$923.setNullAt(0);
                        } else {
                            out_inputRow$923.setLong(0, field$930);
                        }

                        boolean isNull$931 = in2.isNullAt(1);
                        double field$931 = isNull$931 ? -1.0d : (in2.getDouble(1));
                        if (isNull$931) {
                            out_inputRow$923.setNullAt(1);
                        } else {
                            out_inputRow$923.setDouble(1, field$931);
                        }

                        boolean isNull$932 = in2.isNullAt(2);
                        double field$932 = isNull$932 ? -1.0d : (in2.getDouble(2));
                        if (isNull$932) {
                            out_inputRow$923.setNullAt(2);
                        } else {
                            out_inputRow$923.setDouble(2, field$932);
                        }

                        boolean isNull$933 = in2.isNullAt(3);
                        double field$933 = isNull$933 ? -1.0d : (in2.getDouble(3));
                        if (isNull$933) {
                            out_inputRow$923.setNullAt(3);
                        } else {
                            out_inputRow$923.setDouble(3, field$933);
                        }

                        boolean isNull$934 = in2.isNullAt(4);
                        double field$934 = isNull$934 ? -1.0d : (in2.getDouble(4));
                        if (isNull$934) {
                            out_inputRow$923.setNullAt(4);
                        } else {
                            out_inputRow$923.setDouble(4, field$934);
                        }

                        output.collect(outElement.replace(out_inputRow$923));
                    }
                }
            }
        }
    }

    public void endInput1() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$929.endBuild();
    }

    public void endInput2() throws Exception {}

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.BoxedWrapperRowData out_inputRow$923 =
            new org.apache.flink.table.data.BoxedWrapperRowData(5);
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$946;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$947;
    Projection$948 buildToBinaryRow$957;
    private transient java.lang.Object[] buildProjRefs$958;
    Projection$950 probeToBinaryRow$957;
    private transient java.lang.Object[] probeProjRefs$959;
    LongHashTable$960 hashTable$929;

    private void hj_init$963(Object[] references) throws Exception {
        buildSer$946 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$947 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$958 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$957 = new Projection$948(buildProjRefs$958);
        probeProjRefs$959 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$957 = new Projection$950(probeProjRefs$959);
    }

    private transient java.lang.Object[] hj_Refs$964;

    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$968;

    public BatchMultipleInputStreamOperator$969(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        hj_Refs$964 = (((java.lang.Object[]) references[0]));
        hj_init$963(hj_Refs$964);
        inputSpecRefs$968 = (((java.util.ArrayList) references[1]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$968);
    }

    @Override
    public void open() throws Exception {
        super.open();

        long memorySize$961 = computeMemorySize(1.0);
        hashTable$929 = new LongHashTable$960(memorySize$961);
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

        if (this.hashTable$929 != null) {
            this.hashTable$929.close();
            this.hashTable$929.free();
            this.hashTable$929 = null;
        }
    }

    public class Projection$950
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(6);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$950(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$951 = in1.isNullAt(0);
            long field$951 = isNull$951 ? -1L : (in1.getLong(0));
            if (isNull$951) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$951);
            }

            boolean isNull$952 = in1.isNullAt(1);
            double field$952 = isNull$952 ? -1.0d : (in1.getDouble(1));
            if (isNull$952) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeDouble(1, field$952);
            }

            boolean isNull$953 = in1.isNullAt(2);
            double field$953 = isNull$953 ? -1.0d : (in1.getDouble(2));
            if (isNull$953) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeDouble(2, field$953);
            }

            boolean isNull$954 = in1.isNullAt(3);
            double field$954 = isNull$954 ? -1.0d : (in1.getDouble(3));
            if (isNull$954) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeDouble(3, field$954);
            }

            boolean isNull$955 = in1.isNullAt(4);
            double field$955 = isNull$955 ? -1.0d : (in1.getDouble(4));
            if (isNull$955) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeDouble(4, field$955);
            }

            boolean isNull$956 = in1.isNullAt(5);
            long field$956 = isNull$956 ? -1L : (in1.getLong(5));
            if (isNull$956) {
                outWriter.setNullAt(5);
            } else {
                outWriter.writeLong(5, field$956);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$960
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$960(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$946,
                    probeSer$947,
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
                return probeToBinaryRow$957.apply(row);
            }
        }
    }

    public class Projection$948
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$948(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$949 = in1.isNullAt(0);
            long field$949 = isNull$949 ? -1L : (in1.getLong(0));
            if (isNull$949) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$949);
            }

            outWriter.complete();

            return out;
        }
    }
}
