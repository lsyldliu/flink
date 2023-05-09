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

public final class BatchMultipleInputStreamOperator$1106
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

        boolean anyNull$1045 = false;
        boolean isNull$1044 = in1.isNullAt(0);
        long field$1044 = isNull$1044 ? -1L : (in1.getLong(0));

        anyNull$1045 |= isNull$1044;

        if (!anyNull$1045) {

            hashTable$1046.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in1);
        }
    }

    public void processInput2(org.apache.flink.table.data.RowData in2) throws Exception {

        // evaluate the required expr in advance

        boolean anyNull$1048 = false;
        boolean isNull$1047 = in2.isNullAt(0);
        long field$1047 = isNull$1047 ? -1L : (in2.getLong(0));

        anyNull$1048 |= isNull$1047;

        if (!anyNull$1048) {

            hashTable$1049.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in2);
        }
    }

    public void processInput3(org.apache.flink.table.data.RowData in3) throws Exception {

        // evaluate the required expr in advance

        boolean isNull$1053 = in3.isNullAt(3);
        long field$1053 = isNull$1053 ? -1L : (in3.getLong(3));
        boolean result$1054 = !isNull$1053;
        if (result$1054) {

            // evaluate the required expr in advance

            // generate join key for probe side

            // find matches from hash table
            org.apache.flink.table.runtime.util.RowIterator buildIter$1058 =
                    isNull$1053 ? null : hashTable$1049.get(field$1053);
            if (buildIter$1058 != null) {
                while (buildIter$1058.advanceNext()) {
                    org.apache.flink.table.data.RowData buildRow$1056 = buildIter$1058.getRow();
                    {

                        // evaluate the required expr in advance

                        // evaluate the required expr in advance

                        // generate join key for probe side
                        boolean isNull$1050 = in3.isNullAt(0);
                        long field$1050 = isNull$1050 ? -1L : (in3.getLong(0));
                        // find matches from hash table
                        org.apache.flink.table.runtime.util.RowIterator buildIter$1063 =
                                isNull$1050 ? null : hashTable$1046.get(field$1050);
                        if (buildIter$1063 != null) {
                            while (buildIter$1063.advanceNext()) {
                                {

                                    // evaluate the required expr in advance

                                    // wrap variable to row

                                    out_inputRow$1039.setLong(0, field$1050);

                                    boolean isNull$1051 = in3.isNullAt(1);
                                    int field$1051 = isNull$1051 ? -1 : (in3.getInt(1));
                                    if (isNull$1051) {
                                        out_inputRow$1039.setNullAt(1);
                                    } else {
                                        out_inputRow$1039.setInt(1, field$1051);
                                    }

                                    boolean isNull$1052 = in3.isNullAt(2);
                                    double field$1052 = isNull$1052 ? -1.0d : (in3.getDouble(2));
                                    if (isNull$1052) {
                                        out_inputRow$1039.setNullAt(2);
                                    } else {
                                        out_inputRow$1039.setDouble(2, field$1052);
                                    }

                                    output.collect(outElement.replace(out_inputRow$1039));

                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public void endInput1() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$1046.endBuild();
    }

    public void endInput2() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$1049.endBuild();
    }

    public void endInput3() throws Exception {}

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.BoxedWrapperRowData out_inputRow$1039 =
            new org.apache.flink.table.data.BoxedWrapperRowData(3);
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer
            buildSer$1067;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer
            probeSer$1068;
    Projection$1069 buildToBinaryRow$1075;
    private transient java.lang.Object[] buildProjRefs$1076;
    Projection$1071 probeToBinaryRow$1075;
    private transient java.lang.Object[] probeProjRefs$1077;
    LongHashTable$1078 hashTable$1046;

    private void hj_init$1081(Object[] references) throws Exception {
        buildSer$1067 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$1068 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$1076 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$1075 = new Projection$1069(buildProjRefs$1076);
        probeProjRefs$1077 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$1075 = new Projection$1071(probeProjRefs$1077);
    }

    private transient java.lang.Object[] hj_Refs$1082;

    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer
            buildSer$1084;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer
            probeSer$1085;
    Projection$1086 buildToBinaryRow$1093;
    private transient java.lang.Object[] buildProjRefs$1094;
    Projection$1088 probeToBinaryRow$1093;
    private transient java.lang.Object[] probeProjRefs$1095;
    LongHashTable$1096 hashTable$1049;

    private void hj_init$1099(Object[] references) throws Exception {
        buildSer$1084 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$1085 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$1094 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$1093 = new Projection$1086(buildProjRefs$1094);
        probeProjRefs$1095 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$1093 = new Projection$1088(probeProjRefs$1095);
    }

    private transient java.lang.Object[] hj_Refs$1100;
    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$1105;

    public BatchMultipleInputStreamOperator$1106(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 3);
        this.parameters = parameters;
        hj_Refs$1082 = (((java.lang.Object[]) references[0]));
        hj_init$1081(hj_Refs$1082);
        hj_Refs$1100 = (((java.lang.Object[]) references[1]));
        hj_init$1099(hj_Refs$1100);
        inputSpecRefs$1105 = (((java.util.ArrayList) references[2]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$1105);
    }

    @Override
    public void open() throws Exception {
        super.open();

        long memorySize$1079 = computeMemorySize(0.5);
        hashTable$1046 = new LongHashTable$1078(memorySize$1079);
        long memorySize$1097 = computeMemorySize(0.5);
        hashTable$1049 = new LongHashTable$1096(memorySize$1097);
    }

    @Override
    public java.util.List getInputs() {
        return java.util.Arrays.asList(
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 1) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`ss_item_sk` BIGINT NOT NULL>
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
                },
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 3) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`ss_item_sk` BIGINT NOT NULL, `ss_quantity` INT,
                        // `ss_list_price` DOUBLE, `ss_sold_date_sk` BIGINT>
                        org.apache.flink.table.data.RowData in3 =
                                (org.apache.flink.table.data.RowData) element.getValue();
                        processInput3(in3);
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

            case 3:
                endInput3();
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

        if (this.hashTable$1049 != null) {
            this.hashTable$1049.close();
            this.hashTable$1049.free();
            this.hashTable$1049 = null;
        }

        if (this.hashTable$1046 != null) {
            this.hashTable$1046.close();
            this.hashTable$1046.free();
            this.hashTable$1046 = null;
        }
    }

    public class Projection$1088
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(4);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$1088(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$1089 = in1.isNullAt(0);
            long field$1089 = isNull$1089 ? -1L : (in1.getLong(0));
            if (isNull$1089) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$1089);
            }

            boolean isNull$1090 = in1.isNullAt(1);
            int field$1090 = isNull$1090 ? -1 : (in1.getInt(1));
            if (isNull$1090) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeInt(1, field$1090);
            }

            boolean isNull$1091 = in1.isNullAt(2);
            double field$1091 = isNull$1091 ? -1.0d : (in1.getDouble(2));
            if (isNull$1091) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeDouble(2, field$1091);
            }

            boolean isNull$1092 = in1.isNullAt(3);
            long field$1092 = isNull$1092 ? -1L : (in1.getLong(3));
            if (isNull$1092) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeLong(3, field$1092);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$1086
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$1086(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$1087 = in1.isNullAt(0);
            long field$1087 = isNull$1087 ? -1L : (in1.getLong(0));
            if (isNull$1087) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$1087);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$1096
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$1096(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$1084,
                    probeSer$1085,
                    getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    getContainingTask().getEnvironment().getIOManager(),
                    8,
                    30L / getRuntimeContext().getNumberOfParallelSubtasks());
        }

        @Override
        public long getBuildLongKey(org.apache.flink.table.data.RowData row) {
            return row.getLong(0);
        }

        @Override
        public long getProbeLongKey(org.apache.flink.table.data.RowData row) {
            return row.getLong(3);
        }

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData probeToBinary(
                org.apache.flink.table.data.RowData row) {
            if (row instanceof org.apache.flink.table.data.binary.BinaryRowData) {
                return (org.apache.flink.table.data.binary.BinaryRowData) row;
            } else {
                return probeToBinaryRow$1093.apply(row);
            }
        }
    }

    public class Projection$1069
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$1069(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$1070 = in1.isNullAt(0);
            long field$1070 = isNull$1070 ? -1L : (in1.getLong(0));
            if (isNull$1070) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$1070);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$1078
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$1078(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$1067,
                    probeSer$1068,
                    getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    getContainingTask().getEnvironment().getIOManager(),
                    8,
                    242770L / getRuntimeContext().getNumberOfParallelSubtasks());
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
                return probeToBinaryRow$1075.apply(row);
            }
        }
    }

    public class Projection$1071
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(3);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$1071(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$1072 = in1.isNullAt(0);
            long field$1072 = isNull$1072 ? -1L : (in1.getLong(0));
            if (isNull$1072) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$1072);
            }

            boolean isNull$1073 = in1.isNullAt(1);
            int field$1073 = isNull$1073 ? -1 : (in1.getInt(1));
            if (isNull$1073) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeInt(1, field$1073);
            }

            boolean isNull$1074 = in1.isNullAt(2);
            double field$1074 = isNull$1074 ? -1.0d : (in1.getDouble(2));
            if (isNull$1074) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeDouble(2, field$1074);
            }

            outWriter.complete();

            return out;
        }
    }
}
