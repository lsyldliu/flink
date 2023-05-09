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

public final class BatchMultipleInputStreamOperator$1196
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

        boolean anyNull$1155 = false;
        boolean isNull$1154 = in1.isNullAt(0);
        long field$1154 = isNull$1154 ? -1L : (in1.getLong(0));

        anyNull$1155 |= isNull$1154;

        if (!anyNull$1155) {

            hashTable$1156.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in1);
        }
    }

    public void processInput2(org.apache.flink.table.data.RowData in2) throws Exception {

        // evaluate the required expr in advance

        boolean isNull$1162 = in2.isNullAt(5);
        long field$1162 = isNull$1162 ? -1L : (in2.getLong(5));
        boolean result$1163 = !isNull$1162;
        if (result$1163) {

            // evaluate the required expr in advance

            // generate join key for probe side

            // find matches from hash table
            org.apache.flink.table.runtime.util.RowIterator buildIter$1167 =
                    isNull$1162 ? null : hashTable$1156.get(field$1162);
            if (buildIter$1167 != null) {
                while (buildIter$1167.advanceNext()) {
                    org.apache.flink.table.data.RowData buildRow$1165 = buildIter$1167.getRow();
                    {

                        // evaluate the required expr in advance

                        // evaluate the required expr in advance

                        // wrap variable to row

                        boolean isNull$1157 = in2.isNullAt(0);
                        long field$1157 = isNull$1157 ? -1L : (in2.getLong(0));
                        if (isNull$1157) {
                            out_inputRow$1150.setNullAt(0);
                        } else {
                            out_inputRow$1150.setLong(0, field$1157);
                        }

                        boolean isNull$1158 = in2.isNullAt(1);
                        double field$1158 = isNull$1158 ? -1.0d : (in2.getDouble(1));
                        if (isNull$1158) {
                            out_inputRow$1150.setNullAt(1);
                        } else {
                            out_inputRow$1150.setDouble(1, field$1158);
                        }

                        boolean isNull$1159 = in2.isNullAt(2);
                        double field$1159 = isNull$1159 ? -1.0d : (in2.getDouble(2));
                        if (isNull$1159) {
                            out_inputRow$1150.setNullAt(2);
                        } else {
                            out_inputRow$1150.setDouble(2, field$1159);
                        }

                        boolean isNull$1160 = in2.isNullAt(3);
                        double field$1160 = isNull$1160 ? -1.0d : (in2.getDouble(3));
                        if (isNull$1160) {
                            out_inputRow$1150.setNullAt(3);
                        } else {
                            out_inputRow$1150.setDouble(3, field$1160);
                        }

                        boolean isNull$1161 = in2.isNullAt(4);
                        double field$1161 = isNull$1161 ? -1.0d : (in2.getDouble(4));
                        if (isNull$1161) {
                            out_inputRow$1150.setNullAt(4);
                        } else {
                            out_inputRow$1150.setDouble(4, field$1161);
                        }

                        output.collect(outElement.replace(out_inputRow$1150));
                    }
                }
            }
        }
    }

    public void endInput1() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$1156.endBuild();
    }

    public void endInput2() throws Exception {}

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.BoxedWrapperRowData out_inputRow$1150 =
            new org.apache.flink.table.data.BoxedWrapperRowData(5);
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer
            buildSer$1173;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer
            probeSer$1174;
    Projection$1175 buildToBinaryRow$1184;
    private transient java.lang.Object[] buildProjRefs$1185;
    Projection$1177 probeToBinaryRow$1184;
    private transient java.lang.Object[] probeProjRefs$1186;
    LongHashTable$1187 hashTable$1156;

    private void hj_init$1190(Object[] references) throws Exception {
        buildSer$1173 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$1174 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$1185 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$1184 = new Projection$1175(buildProjRefs$1185);
        probeProjRefs$1186 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$1184 = new Projection$1177(probeProjRefs$1186);
    }

    private transient java.lang.Object[] hj_Refs$1191;

    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$1195;

    public BatchMultipleInputStreamOperator$1196(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        hj_Refs$1191 = (((java.lang.Object[]) references[0]));
        hj_init$1190(hj_Refs$1191);
        inputSpecRefs$1195 = (((java.util.ArrayList) references[1]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$1195);
    }

    @Override
    public void open() throws Exception {
        super.open();

        long memorySize$1188 = computeMemorySize(1.0);
        hashTable$1156 = new LongHashTable$1187(memorySize$1188);
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
                        // InputType: ROW<`cs_bill_customer_sk` BIGINT, `cs_ext_discount_amt`
                        // DOUBLE, `cs_ext_sales_price` DOUBLE, `cs_ext_wholesale_cost` DOUBLE,
                        // `cs_ext_list_price` DOUBLE, `cs_sold_date_sk` BIGINT>
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

        if (this.hashTable$1156 != null) {
            this.hashTable$1156.close();
            this.hashTable$1156.free();
            this.hashTable$1156 = null;
        }
    }

    public class Projection$1175
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$1175(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$1176 = in1.isNullAt(0);
            long field$1176 = isNull$1176 ? -1L : (in1.getLong(0));
            if (isNull$1176) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$1176);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$1187
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$1187(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$1173,
                    probeSer$1174,
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
                return probeToBinaryRow$1184.apply(row);
            }
        }
    }

    public class Projection$1177
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(6);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$1177(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$1178 = in1.isNullAt(0);
            long field$1178 = isNull$1178 ? -1L : (in1.getLong(0));
            if (isNull$1178) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$1178);
            }

            boolean isNull$1179 = in1.isNullAt(1);
            double field$1179 = isNull$1179 ? -1.0d : (in1.getDouble(1));
            if (isNull$1179) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeDouble(1, field$1179);
            }

            boolean isNull$1180 = in1.isNullAt(2);
            double field$1180 = isNull$1180 ? -1.0d : (in1.getDouble(2));
            if (isNull$1180) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeDouble(2, field$1180);
            }

            boolean isNull$1181 = in1.isNullAt(3);
            double field$1181 = isNull$1181 ? -1.0d : (in1.getDouble(3));
            if (isNull$1181) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeDouble(3, field$1181);
            }

            boolean isNull$1182 = in1.isNullAt(4);
            double field$1182 = isNull$1182 ? -1.0d : (in1.getDouble(4));
            if (isNull$1182) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeDouble(4, field$1182);
            }

            boolean isNull$1183 = in1.isNullAt(5);
            long field$1183 = isNull$1183 ? -1L : (in1.getLong(5));
            if (isNull$1183) {
                outWriter.setNullAt(5);
            } else {
                outWriter.writeLong(5, field$1183);
            }

            outWriter.complete();

            return out;
        }
    }
}
