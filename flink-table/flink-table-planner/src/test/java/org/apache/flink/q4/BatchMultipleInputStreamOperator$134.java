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

public final class BatchMultipleInputStreamOperator$134
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

        boolean anyNull$80 = false;
        boolean isNull$72 = in2.isNullAt(0);
        long field$72 = isNull$72 ? -1L : (in2.getLong(0));

        anyNull$80 |= isNull$72;

        if (!anyNull$80) {

            hashTable$81.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in2);
        }
    }

    public void processInput1(org.apache.flink.table.data.RowData in1) throws Exception {

        // evaluate the required expr in advance

        // generate join key for probe side
        boolean isNull$82 = in1.isNullAt(0);
        long field$82 = isNull$82 ? -1L : (in1.getLong(0));
        // find matches from hash table
        org.apache.flink.table.runtime.util.RowIterator buildIter$96 =
                isNull$82 ? null : hashTable$81.get(field$82);
        if (buildIter$96 != null) {
            while (buildIter$96.advanceNext()) {
                org.apache.flink.table.data.RowData buildRow$87 = buildIter$96.getRow();
                {

                    // evaluate the required expr in advance

                    // evaluate the required expr in advance

                    // wrap variable to row

                    boolean isNull$89 = buildRow$87.isNullAt(1);
                    org.apache.flink.table.data.binary.BinaryStringData field$89 =
                            isNull$89
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$87.getString(1)));
                    out_inputRow$69.setNonPrimitiveValue(0, field$89);

                    boolean isNull$90 = buildRow$87.isNullAt(2);
                    org.apache.flink.table.data.binary.BinaryStringData field$90 =
                            isNull$90
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$87.getString(2)));
                    if (isNull$90) {
                        out_inputRow$69.setNullAt(1);
                    } else {
                        out_inputRow$69.setNonPrimitiveValue(1, field$90);
                    }

                    boolean isNull$91 = buildRow$87.isNullAt(3);
                    org.apache.flink.table.data.binary.BinaryStringData field$91 =
                            isNull$91
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$87.getString(3)));
                    if (isNull$91) {
                        out_inputRow$69.setNullAt(2);
                    } else {
                        out_inputRow$69.setNonPrimitiveValue(2, field$91);
                    }

                    boolean isNull$92 = buildRow$87.isNullAt(4);
                    org.apache.flink.table.data.binary.BinaryStringData field$92 =
                            isNull$92
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$87.getString(4)));
                    if (isNull$92) {
                        out_inputRow$69.setNullAt(3);
                    } else {
                        out_inputRow$69.setNonPrimitiveValue(3, field$92);
                    }

                    boolean isNull$93 = buildRow$87.isNullAt(5);
                    org.apache.flink.table.data.binary.BinaryStringData field$93 =
                            isNull$93
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$87.getString(5)));
                    if (isNull$93) {
                        out_inputRow$69.setNullAt(4);
                    } else {
                        out_inputRow$69.setNonPrimitiveValue(4, field$93);
                    }

                    boolean isNull$94 = buildRow$87.isNullAt(6);
                    org.apache.flink.table.data.binary.BinaryStringData field$94 =
                            isNull$94
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$87.getString(6)));
                    if (isNull$94) {
                        out_inputRow$69.setNullAt(5);
                    } else {
                        out_inputRow$69.setNonPrimitiveValue(5, field$94);
                    }

                    boolean isNull$95 = buildRow$87.isNullAt(7);
                    org.apache.flink.table.data.binary.BinaryStringData field$95 =
                            isNull$95
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$87.getString(7)));
                    if (isNull$95) {
                        out_inputRow$69.setNullAt(6);
                    } else {
                        out_inputRow$69.setNonPrimitiveValue(6, field$95);
                    }

                    // --- Cast section generated by
                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                    // --- End cast section

                    if (false) {
                        out_inputRow$69.setNullAt(7);
                    } else {
                        out_inputRow$69.setInt(7, ((int) 2002));
                    }

                    boolean isNull$86 = in1.isNullAt(4);
                    double field$86 = isNull$86 ? -1.0d : (in1.getDouble(4));
                    boolean isNull$85 = in1.isNullAt(3);
                    double field$85 = isNull$85 ? -1.0d : (in1.getDouble(3));
                    boolean isNull$98 = isNull$86 || isNull$85;
                    double result$98 = -1.0d;
                    if (!isNull$98) {

                        result$98 = (double) (field$86 - field$85);
                    }

                    boolean isNull$83 = in1.isNullAt(1);
                    double field$83 = isNull$83 ? -1.0d : (in1.getDouble(1));
                    boolean isNull$99 = isNull$98 || isNull$83;
                    double result$99 = -1.0d;
                    if (!isNull$99) {

                        result$99 = (double) (result$98 - field$83);
                    }

                    boolean isNull$84 = in1.isNullAt(2);
                    double field$84 = isNull$84 ? -1.0d : (in1.getDouble(2));
                    boolean isNull$100 = isNull$99 || isNull$84;
                    double result$100 = -1.0d;
                    if (!isNull$100) {

                        result$100 = (double) (result$99 + field$84);
                    }

                    boolean isNull$101 = isNull$100 || false;
                    double result$101 = -1.0d;
                    if (!isNull$101) {

                        result$101 = (double) (result$100 / ((double) (((int) 2))));
                    }

                    if (isNull$101) {
                        out_inputRow$69.setNullAt(8);
                    } else {
                        out_inputRow$69.setDouble(8, result$101);
                    }

                    output.collect(outElement.replace(out_inputRow$69));
                }
            }
        }
    }

    public void endInput2() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$81.endBuild();
    }

    public void endInput1() throws Exception {}

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.BoxedWrapperRowData out_inputRow$69 =
            new org.apache.flink.table.data.BoxedWrapperRowData(9);
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$106;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$107;
    Projection$108 buildToBinaryRow$123;
    private transient java.lang.Object[] buildProjRefs$124;
    Projection$117 probeToBinaryRow$123;
    private transient java.lang.Object[] probeProjRefs$125;
    LongHashTable$126 hashTable$81;

    private void hj_init$129(Object[] references) throws Exception {
        buildSer$106 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$107 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$124 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$123 = new Projection$108(buildProjRefs$124);
        probeProjRefs$125 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$123 = new Projection$117(probeProjRefs$125);
    }

    private transient java.lang.Object[] hj_Refs$130;

    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$133;

    public BatchMultipleInputStreamOperator$134(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        hj_Refs$130 = (((java.lang.Object[]) references[0]));
        hj_init$129(hj_Refs$130);
        inputSpecRefs$133 = (((java.util.ArrayList) references[1]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$133);
    }

    @Override
    public void open() throws Exception {
        super.open();

        long memorySize$127 = computeMemorySize(1.0);
        hashTable$81 = new LongHashTable$126(memorySize$127);
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
                        // `ws_ext_list_price` DOUBLE>
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
                        // InputType: ROW<`c_customer_sk` BIGINT NOT NULL, `c_customer_id`
                        // VARCHAR(2147483647) NOT NULL, `c_first_name` VARCHAR(2147483647),
                        // `c_last_name` VARCHAR(2147483647), `c_preferred_cust_flag`
                        // VARCHAR(2147483647), `c_birth_country` VARCHAR(2147483647), `c_login`
                        // VARCHAR(2147483647), `c_email_address` VARCHAR(2147483647)>
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

        if (this.hashTable$81 != null) {
            this.hashTable$81.close();
            this.hashTable$81.free();
            this.hashTable$81 = null;
        }
    }

    public class LongHashTable$126
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$126(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$106,
                    probeSer$107,
                    getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    getContainingTask().getEnvironment().getIOManager(),
                    71,
                    65000000L / getRuntimeContext().getNumberOfParallelSubtasks());
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
                return probeToBinaryRow$123.apply(row);
            }
        }
    }

    public class Projection$108
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(8);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$108(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$109 = in1.isNullAt(0);
            long field$109 = isNull$109 ? -1L : (in1.getLong(0));
            if (isNull$109) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$109);
            }

            boolean isNull$110 = in1.isNullAt(1);
            org.apache.flink.table.data.binary.BinaryStringData field$110 =
                    isNull$110
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(1)));
            if (isNull$110) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeString(1, field$110);
            }

            boolean isNull$111 = in1.isNullAt(2);
            org.apache.flink.table.data.binary.BinaryStringData field$111 =
                    isNull$111
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(2)));
            if (isNull$111) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeString(2, field$111);
            }

            boolean isNull$112 = in1.isNullAt(3);
            org.apache.flink.table.data.binary.BinaryStringData field$112 =
                    isNull$112
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(3)));
            if (isNull$112) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeString(3, field$112);
            }

            boolean isNull$113 = in1.isNullAt(4);
            org.apache.flink.table.data.binary.BinaryStringData field$113 =
                    isNull$113
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(4)));
            if (isNull$113) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeString(4, field$113);
            }

            boolean isNull$114 = in1.isNullAt(5);
            org.apache.flink.table.data.binary.BinaryStringData field$114 =
                    isNull$114
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(5)));
            if (isNull$114) {
                outWriter.setNullAt(5);
            } else {
                outWriter.writeString(5, field$114);
            }

            boolean isNull$115 = in1.isNullAt(6);
            org.apache.flink.table.data.binary.BinaryStringData field$115 =
                    isNull$115
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(6)));
            if (isNull$115) {
                outWriter.setNullAt(6);
            } else {
                outWriter.writeString(6, field$115);
            }

            boolean isNull$116 = in1.isNullAt(7);
            org.apache.flink.table.data.binary.BinaryStringData field$116 =
                    isNull$116
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(7)));
            if (isNull$116) {
                outWriter.setNullAt(7);
            } else {
                outWriter.writeString(7, field$116);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$117
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(5);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$117(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$118 = in1.isNullAt(0);
            long field$118 = isNull$118 ? -1L : (in1.getLong(0));
            if (isNull$118) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$118);
            }

            boolean isNull$119 = in1.isNullAt(1);
            double field$119 = isNull$119 ? -1.0d : (in1.getDouble(1));
            if (isNull$119) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeDouble(1, field$119);
            }

            boolean isNull$120 = in1.isNullAt(2);
            double field$120 = isNull$120 ? -1.0d : (in1.getDouble(2));
            if (isNull$120) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeDouble(2, field$120);
            }

            boolean isNull$121 = in1.isNullAt(3);
            double field$121 = isNull$121 ? -1.0d : (in1.getDouble(3));
            if (isNull$121) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeDouble(3, field$121);
            }

            boolean isNull$122 = in1.isNullAt(4);
            double field$122 = isNull$122 ? -1.0d : (in1.getDouble(4));
            if (isNull$122) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeDouble(4, field$122);
            }

            outWriter.complete();

            return out;
        }
    }
}
