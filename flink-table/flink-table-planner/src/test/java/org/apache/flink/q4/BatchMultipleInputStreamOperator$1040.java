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

public final class BatchMultipleInputStreamOperator$1040
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

        boolean anyNull$986 = false;
        boolean isNull$978 = in2.isNullAt(0);
        long field$978 = isNull$978 ? -1L : (in2.getLong(0));

        anyNull$986 |= isNull$978;

        if (!anyNull$986) {

            hashTable$987.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in2);
        }
    }

    public void processInput1(org.apache.flink.table.data.RowData in1) throws Exception {

        // evaluate the required expr in advance

        // generate join key for probe side
        boolean isNull$988 = in1.isNullAt(0);
        long field$988 = isNull$988 ? -1L : (in1.getLong(0));
        // find matches from hash table
        org.apache.flink.table.runtime.util.RowIterator buildIter$1002 =
                isNull$988 ? null : hashTable$987.get(field$988);
        if (buildIter$1002 != null) {
            while (buildIter$1002.advanceNext()) {
                org.apache.flink.table.data.RowData buildRow$993 = buildIter$1002.getRow();
                {

                    // evaluate the required expr in advance

                    // evaluate the required expr in advance

                    // wrap variable to row

                    boolean isNull$995 = buildRow$993.isNullAt(1);
                    org.apache.flink.table.data.binary.BinaryStringData field$995 =
                            isNull$995
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$993.getString(1)));
                    out_inputRow$975.setNonPrimitiveValue(0, field$995);

                    boolean isNull$996 = buildRow$993.isNullAt(2);
                    org.apache.flink.table.data.binary.BinaryStringData field$996 =
                            isNull$996
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$993.getString(2)));
                    if (isNull$996) {
                        out_inputRow$975.setNullAt(1);
                    } else {
                        out_inputRow$975.setNonPrimitiveValue(1, field$996);
                    }

                    boolean isNull$997 = buildRow$993.isNullAt(3);
                    org.apache.flink.table.data.binary.BinaryStringData field$997 =
                            isNull$997
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$993.getString(3)));
                    if (isNull$997) {
                        out_inputRow$975.setNullAt(2);
                    } else {
                        out_inputRow$975.setNonPrimitiveValue(2, field$997);
                    }

                    boolean isNull$998 = buildRow$993.isNullAt(4);
                    org.apache.flink.table.data.binary.BinaryStringData field$998 =
                            isNull$998
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$993.getString(4)));
                    if (isNull$998) {
                        out_inputRow$975.setNullAt(3);
                    } else {
                        out_inputRow$975.setNonPrimitiveValue(3, field$998);
                    }

                    boolean isNull$999 = buildRow$993.isNullAt(5);
                    org.apache.flink.table.data.binary.BinaryStringData field$999 =
                            isNull$999
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$993.getString(5)));
                    if (isNull$999) {
                        out_inputRow$975.setNullAt(4);
                    } else {
                        out_inputRow$975.setNonPrimitiveValue(4, field$999);
                    }

                    boolean isNull$1000 = buildRow$993.isNullAt(6);
                    org.apache.flink.table.data.binary.BinaryStringData field$1000 =
                            isNull$1000
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$993.getString(6)));
                    if (isNull$1000) {
                        out_inputRow$975.setNullAt(5);
                    } else {
                        out_inputRow$975.setNonPrimitiveValue(5, field$1000);
                    }

                    boolean isNull$1001 = buildRow$993.isNullAt(7);
                    org.apache.flink.table.data.binary.BinaryStringData field$1001 =
                            isNull$1001
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$993.getString(7)));
                    if (isNull$1001) {
                        out_inputRow$975.setNullAt(6);
                    } else {
                        out_inputRow$975.setNonPrimitiveValue(6, field$1001);
                    }

                    // --- Cast section generated by
                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                    // --- End cast section

                    if (false) {
                        out_inputRow$975.setNullAt(7);
                    } else {
                        out_inputRow$975.setInt(7, ((int) 2001));
                    }

                    boolean isNull$992 = in1.isNullAt(4);
                    double field$992 = isNull$992 ? -1.0d : (in1.getDouble(4));
                    boolean isNull$991 = in1.isNullAt(3);
                    double field$991 = isNull$991 ? -1.0d : (in1.getDouble(3));
                    boolean isNull$1004 = isNull$992 || isNull$991;
                    double result$1004 = -1.0d;
                    if (!isNull$1004) {

                        result$1004 = (double) (field$992 - field$991);
                    }

                    boolean isNull$989 = in1.isNullAt(1);
                    double field$989 = isNull$989 ? -1.0d : (in1.getDouble(1));
                    boolean isNull$1005 = isNull$1004 || isNull$989;
                    double result$1005 = -1.0d;
                    if (!isNull$1005) {

                        result$1005 = (double) (result$1004 - field$989);
                    }

                    boolean isNull$990 = in1.isNullAt(2);
                    double field$990 = isNull$990 ? -1.0d : (in1.getDouble(2));
                    boolean isNull$1006 = isNull$1005 || isNull$990;
                    double result$1006 = -1.0d;
                    if (!isNull$1006) {

                        result$1006 = (double) (result$1005 + field$990);
                    }

                    boolean isNull$1007 = isNull$1006 || false;
                    double result$1007 = -1.0d;
                    if (!isNull$1007) {

                        result$1007 = (double) (result$1006 / ((double) (((int) 2))));
                    }

                    if (isNull$1007) {
                        out_inputRow$975.setNullAt(8);
                    } else {
                        out_inputRow$975.setDouble(8, result$1007);
                    }

                    output.collect(outElement.replace(out_inputRow$975));
                }
            }
        }
    }

    public void endInput2() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$987.endBuild();
    }

    public void endInput1() throws Exception {}

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.BoxedWrapperRowData out_inputRow$975 =
            new org.apache.flink.table.data.BoxedWrapperRowData(9);
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer
            buildSer$1012;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer
            probeSer$1013;
    Projection$1014 buildToBinaryRow$1029;
    private transient java.lang.Object[] buildProjRefs$1030;
    Projection$1023 probeToBinaryRow$1029;
    private transient java.lang.Object[] probeProjRefs$1031;
    LongHashTable$1032 hashTable$987;

    private void hj_init$1035(Object[] references) throws Exception {
        buildSer$1012 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$1013 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$1030 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$1029 = new Projection$1014(buildProjRefs$1030);
        probeProjRefs$1031 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$1029 = new Projection$1023(probeProjRefs$1031);
    }

    private transient java.lang.Object[] hj_Refs$1036;

    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$1039;

    public BatchMultipleInputStreamOperator$1040(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        hj_Refs$1036 = (((java.lang.Object[]) references[0]));
        hj_init$1035(hj_Refs$1036);
        inputSpecRefs$1039 = (((java.util.ArrayList) references[1]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$1039);
    }

    @Override
    public void open() throws Exception {
        super.open();

        long memorySize$1033 = computeMemorySize(1.0);
        hashTable$987 = new LongHashTable$1032(memorySize$1033);
    }

    @Override
    public java.util.List getInputs() {
        return java.util.Arrays.asList(
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 1) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`ss_customer_sk` BIGINT, `ss_ext_discount_amt` DOUBLE,
                        // `ss_ext_sales_price` DOUBLE, `ss_ext_wholesale_cost` DOUBLE,
                        // `ss_ext_list_price` DOUBLE>
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

        if (this.hashTable$987 != null) {
            this.hashTable$987.close();
            this.hashTable$987.free();
            this.hashTable$987 = null;
        }
    }

    public class LongHashTable$1032
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$1032(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$1012,
                    probeSer$1013,
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
                return probeToBinaryRow$1029.apply(row);
            }
        }
    }

    public class Projection$1023
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(5);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$1023(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$1024 = in1.isNullAt(0);
            long field$1024 = isNull$1024 ? -1L : (in1.getLong(0));
            if (isNull$1024) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$1024);
            }

            boolean isNull$1025 = in1.isNullAt(1);
            double field$1025 = isNull$1025 ? -1.0d : (in1.getDouble(1));
            if (isNull$1025) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeDouble(1, field$1025);
            }

            boolean isNull$1026 = in1.isNullAt(2);
            double field$1026 = isNull$1026 ? -1.0d : (in1.getDouble(2));
            if (isNull$1026) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeDouble(2, field$1026);
            }

            boolean isNull$1027 = in1.isNullAt(3);
            double field$1027 = isNull$1027 ? -1.0d : (in1.getDouble(3));
            if (isNull$1027) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeDouble(3, field$1027);
            }

            boolean isNull$1028 = in1.isNullAt(4);
            double field$1028 = isNull$1028 ? -1.0d : (in1.getDouble(4));
            if (isNull$1028) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeDouble(4, field$1028);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$1014
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(8);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$1014(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$1015 = in1.isNullAt(0);
            long field$1015 = isNull$1015 ? -1L : (in1.getLong(0));
            if (isNull$1015) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$1015);
            }

            boolean isNull$1016 = in1.isNullAt(1);
            org.apache.flink.table.data.binary.BinaryStringData field$1016 =
                    isNull$1016
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(1)));
            if (isNull$1016) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeString(1, field$1016);
            }

            boolean isNull$1017 = in1.isNullAt(2);
            org.apache.flink.table.data.binary.BinaryStringData field$1017 =
                    isNull$1017
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(2)));
            if (isNull$1017) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeString(2, field$1017);
            }

            boolean isNull$1018 = in1.isNullAt(3);
            org.apache.flink.table.data.binary.BinaryStringData field$1018 =
                    isNull$1018
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(3)));
            if (isNull$1018) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeString(3, field$1018);
            }

            boolean isNull$1019 = in1.isNullAt(4);
            org.apache.flink.table.data.binary.BinaryStringData field$1019 =
                    isNull$1019
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(4)));
            if (isNull$1019) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeString(4, field$1019);
            }

            boolean isNull$1020 = in1.isNullAt(5);
            org.apache.flink.table.data.binary.BinaryStringData field$1020 =
                    isNull$1020
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(5)));
            if (isNull$1020) {
                outWriter.setNullAt(5);
            } else {
                outWriter.writeString(5, field$1020);
            }

            boolean isNull$1021 = in1.isNullAt(6);
            org.apache.flink.table.data.binary.BinaryStringData field$1021 =
                    isNull$1021
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(6)));
            if (isNull$1021) {
                outWriter.setNullAt(6);
            } else {
                outWriter.writeString(6, field$1021);
            }

            boolean isNull$1022 = in1.isNullAt(7);
            org.apache.flink.table.data.binary.BinaryStringData field$1022 =
                    isNull$1022
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(7)));
            if (isNull$1022) {
                outWriter.setNullAt(7);
            } else {
                outWriter.writeString(7, field$1022);
            }

            outWriter.complete();

            return out;
        }
    }
}
