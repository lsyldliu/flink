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

public final class BatchMultipleInputStreamOperator$1262
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

        boolean anyNull$1201 = false;
        boolean isNull$1200 = in1.isNullAt(0);
        long field$1200 = isNull$1200 ? -1L : (in1.getLong(0));

        anyNull$1201 |= isNull$1200;

        if (!anyNull$1201) {

            hashTable$1202.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in1);
        }
    }

    public void processInput2(org.apache.flink.table.data.RowData in2) throws Exception {

        // evaluate the required expr in advance

        boolean anyNull$1204 = false;
        boolean isNull$1203 = in2.isNullAt(0);
        long field$1203 = isNull$1203 ? -1L : (in2.getLong(0));

        anyNull$1204 |= isNull$1203;

        if (!anyNull$1204) {

            hashTable$1205.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in2);
        }
    }

    public void processInput3(org.apache.flink.table.data.RowData in3) throws Exception {

        // evaluate the required expr in advance

        boolean isNull$1209 = in3.isNullAt(3);
        long field$1209 = isNull$1209 ? -1L : (in3.getLong(3));
        boolean result$1210 = !isNull$1209;
        if (result$1210) {

            // evaluate the required expr in advance

            // generate join key for probe side

            // find matches from hash table
            org.apache.flink.table.runtime.util.RowIterator buildIter$1214 =
                    isNull$1209 ? null : hashTable$1205.get(field$1209);
            if (buildIter$1214 != null) {
                while (buildIter$1214.advanceNext()) {
                    org.apache.flink.table.data.RowData buildRow$1212 = buildIter$1214.getRow();
                    {

                        // evaluate the required expr in advance

                        // evaluate the required expr in advance

                        // generate join key for probe side
                        boolean isNull$1206 = in3.isNullAt(0);
                        long field$1206 = isNull$1206 ? -1L : (in3.getLong(0));
                        // find matches from hash table
                        org.apache.flink.table.runtime.util.RowIterator buildIter$1219 =
                                isNull$1206 ? null : hashTable$1202.get(field$1206);
                        if (buildIter$1219 != null) {
                            while (buildIter$1219.advanceNext()) {
                                {

                                    // evaluate the required expr in advance

                                    // wrap variable to row

                                    out_inputRow$1195.setLong(0, field$1206);

                                    boolean isNull$1207 = in3.isNullAt(1);
                                    int field$1207 = isNull$1207 ? -1 : (in3.getInt(1));
                                    if (isNull$1207) {
                                        out_inputRow$1195.setNullAt(1);
                                    } else {
                                        out_inputRow$1195.setInt(1, field$1207);
                                    }

                                    boolean isNull$1208 = in3.isNullAt(2);
                                    double field$1208 = isNull$1208 ? -1.0d : (in3.getDouble(2));
                                    if (isNull$1208) {
                                        out_inputRow$1195.setNullAt(2);
                                    } else {
                                        out_inputRow$1195.setDouble(2, field$1208);
                                    }

                                    output.collect(outElement.replace(out_inputRow$1195));

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
        hashTable$1202.endBuild();
    }

    public void endInput2() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$1205.endBuild();
    }

    public void endInput3() throws Exception {}

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.BoxedWrapperRowData out_inputRow$1195 =
            new org.apache.flink.table.data.BoxedWrapperRowData(3);
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer
            buildSer$1223;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer
            probeSer$1224;
    Projection$1225 buildToBinaryRow$1231;
    private transient java.lang.Object[] buildProjRefs$1232;
    Projection$1227 probeToBinaryRow$1231;
    private transient java.lang.Object[] probeProjRefs$1233;
    LongHashTable$1234 hashTable$1202;

    private void hj_init$1237(Object[] references) throws Exception {
        buildSer$1223 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$1224 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$1232 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$1231 = new Projection$1225(buildProjRefs$1232);
        probeProjRefs$1233 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$1231 = new Projection$1227(probeProjRefs$1233);
    }

    private transient java.lang.Object[] hj_Refs$1238;

    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer
            buildSer$1240;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer
            probeSer$1241;
    Projection$1242 buildToBinaryRow$1249;
    private transient java.lang.Object[] buildProjRefs$1250;
    Projection$1244 probeToBinaryRow$1249;
    private transient java.lang.Object[] probeProjRefs$1251;
    LongHashTable$1252 hashTable$1205;

    private void hj_init$1255(Object[] references) throws Exception {
        buildSer$1240 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$1241 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$1250 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$1249 = new Projection$1242(buildProjRefs$1250);
        probeProjRefs$1251 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$1249 = new Projection$1244(probeProjRefs$1251);
    }

    private transient java.lang.Object[] hj_Refs$1256;
    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$1261;

    public BatchMultipleInputStreamOperator$1262(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 3);
        this.parameters = parameters;
        hj_Refs$1238 = (((java.lang.Object[]) references[0]));
        hj_init$1237(hj_Refs$1238);
        hj_Refs$1256 = (((java.lang.Object[]) references[1]));
        hj_init$1255(hj_Refs$1256);
        inputSpecRefs$1261 = (((java.util.ArrayList) references[2]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$1261);
    }

    @Override
    public void open() throws Exception {
        super.open();

        long memorySize$1235 = computeMemorySize(0.5);
        hashTable$1202 = new LongHashTable$1234(memorySize$1235);
        long memorySize$1253 = computeMemorySize(0.5);
        hashTable$1205 = new LongHashTable$1252(memorySize$1253);
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
                        // InputType: ROW<`cs_item_sk` BIGINT NOT NULL, `cs_quantity` INT,
                        // `cs_list_price` DOUBLE, `cs_sold_date_sk` BIGINT>
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

        if (this.hashTable$1205 != null) {
            this.hashTable$1205.close();
            this.hashTable$1205.free();
            this.hashTable$1205 = null;
        }

        if (this.hashTable$1202 != null) {
            this.hashTable$1202.close();
            this.hashTable$1202.free();
            this.hashTable$1202 = null;
        }
    }

    public class Projection$1244
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(4);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$1244(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$1245 = in1.isNullAt(0);
            long field$1245 = isNull$1245 ? -1L : (in1.getLong(0));
            if (isNull$1245) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$1245);
            }

            boolean isNull$1246 = in1.isNullAt(1);
            int field$1246 = isNull$1246 ? -1 : (in1.getInt(1));
            if (isNull$1246) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeInt(1, field$1246);
            }

            boolean isNull$1247 = in1.isNullAt(2);
            double field$1247 = isNull$1247 ? -1.0d : (in1.getDouble(2));
            if (isNull$1247) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeDouble(2, field$1247);
            }

            boolean isNull$1248 = in1.isNullAt(3);
            long field$1248 = isNull$1248 ? -1L : (in1.getLong(3));
            if (isNull$1248) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeLong(3, field$1248);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$1252
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$1252(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$1240,
                    probeSer$1241,
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
                return probeToBinaryRow$1249.apply(row);
            }
        }
    }

    public class Projection$1242
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$1242(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$1243 = in1.isNullAt(0);
            long field$1243 = isNull$1243 ? -1L : (in1.getLong(0));
            if (isNull$1243) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$1243);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$1225
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$1225(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$1226 = in1.isNullAt(0);
            long field$1226 = isNull$1226 ? -1L : (in1.getLong(0));
            if (isNull$1226) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$1226);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$1234
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$1234(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$1223,
                    probeSer$1224,
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
                return probeToBinaryRow$1231.apply(row);
            }
        }
    }

    public class Projection$1227
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(3);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$1227(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$1228 = in1.isNullAt(0);
            long field$1228 = isNull$1228 ? -1L : (in1.getLong(0));
            if (isNull$1228) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$1228);
            }

            boolean isNull$1229 = in1.isNullAt(1);
            int field$1229 = isNull$1229 ? -1 : (in1.getInt(1));
            if (isNull$1229) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeInt(1, field$1229);
            }

            boolean isNull$1230 = in1.isNullAt(2);
            double field$1230 = isNull$1230 ? -1.0d : (in1.getDouble(2));
            if (isNull$1230) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeDouble(2, field$1230);
            }

            outWriter.complete();

            return out;
        }
    }
}
