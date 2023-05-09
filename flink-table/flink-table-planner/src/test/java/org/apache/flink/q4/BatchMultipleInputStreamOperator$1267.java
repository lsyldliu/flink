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

public final class BatchMultipleInputStreamOperator$1267
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

        boolean anyNull$1213 = false;
        boolean isNull$1205 = in2.isNullAt(0);
        long field$1205 = isNull$1205 ? -1L : (in2.getLong(0));

        anyNull$1213 |= isNull$1205;

        if (!anyNull$1213) {

            hashTable$1214.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in2);
        }
    }

    public void processInput1(org.apache.flink.table.data.RowData in1) throws Exception {

        // evaluate the required expr in advance

        // generate join key for probe side
        boolean isNull$1215 = in1.isNullAt(0);
        long field$1215 = isNull$1215 ? -1L : (in1.getLong(0));
        // find matches from hash table
        org.apache.flink.table.runtime.util.RowIterator buildIter$1229 =
                isNull$1215 ? null : hashTable$1214.get(field$1215);
        if (buildIter$1229 != null) {
            while (buildIter$1229.advanceNext()) {
                org.apache.flink.table.data.RowData buildRow$1220 = buildIter$1229.getRow();
                {

                    // evaluate the required expr in advance

                    // evaluate the required expr in advance

                    // wrap variable to row

                    boolean isNull$1222 = buildRow$1220.isNullAt(1);
                    org.apache.flink.table.data.binary.BinaryStringData field$1222 =
                            isNull$1222
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$1220.getString(1)));
                    out_inputRow$1202.setNonPrimitiveValue(0, field$1222);

                    boolean isNull$1223 = buildRow$1220.isNullAt(2);
                    org.apache.flink.table.data.binary.BinaryStringData field$1223 =
                            isNull$1223
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$1220.getString(2)));
                    if (isNull$1223) {
                        out_inputRow$1202.setNullAt(1);
                    } else {
                        out_inputRow$1202.setNonPrimitiveValue(1, field$1223);
                    }

                    boolean isNull$1224 = buildRow$1220.isNullAt(3);
                    org.apache.flink.table.data.binary.BinaryStringData field$1224 =
                            isNull$1224
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$1220.getString(3)));
                    if (isNull$1224) {
                        out_inputRow$1202.setNullAt(2);
                    } else {
                        out_inputRow$1202.setNonPrimitiveValue(2, field$1224);
                    }

                    boolean isNull$1225 = buildRow$1220.isNullAt(4);
                    org.apache.flink.table.data.binary.BinaryStringData field$1225 =
                            isNull$1225
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$1220.getString(4)));
                    if (isNull$1225) {
                        out_inputRow$1202.setNullAt(3);
                    } else {
                        out_inputRow$1202.setNonPrimitiveValue(3, field$1225);
                    }

                    boolean isNull$1226 = buildRow$1220.isNullAt(5);
                    org.apache.flink.table.data.binary.BinaryStringData field$1226 =
                            isNull$1226
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$1220.getString(5)));
                    if (isNull$1226) {
                        out_inputRow$1202.setNullAt(4);
                    } else {
                        out_inputRow$1202.setNonPrimitiveValue(4, field$1226);
                    }

                    boolean isNull$1227 = buildRow$1220.isNullAt(6);
                    org.apache.flink.table.data.binary.BinaryStringData field$1227 =
                            isNull$1227
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$1220.getString(6)));
                    if (isNull$1227) {
                        out_inputRow$1202.setNullAt(5);
                    } else {
                        out_inputRow$1202.setNonPrimitiveValue(5, field$1227);
                    }

                    boolean isNull$1228 = buildRow$1220.isNullAt(7);
                    org.apache.flink.table.data.binary.BinaryStringData field$1228 =
                            isNull$1228
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$1220.getString(7)));
                    if (isNull$1228) {
                        out_inputRow$1202.setNullAt(6);
                    } else {
                        out_inputRow$1202.setNonPrimitiveValue(6, field$1228);
                    }

                    // --- Cast section generated by
                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                    // --- End cast section

                    if (false) {
                        out_inputRow$1202.setNullAt(7);
                    } else {
                        out_inputRow$1202.setInt(7, ((int) 2001));
                    }

                    boolean isNull$1219 = in1.isNullAt(4);
                    double field$1219 = isNull$1219 ? -1.0d : (in1.getDouble(4));
                    boolean isNull$1218 = in1.isNullAt(3);
                    double field$1218 = isNull$1218 ? -1.0d : (in1.getDouble(3));
                    boolean isNull$1231 = isNull$1219 || isNull$1218;
                    double result$1231 = -1.0d;
                    if (!isNull$1231) {

                        result$1231 = (double) (field$1219 - field$1218);
                    }

                    boolean isNull$1216 = in1.isNullAt(1);
                    double field$1216 = isNull$1216 ? -1.0d : (in1.getDouble(1));
                    boolean isNull$1232 = isNull$1231 || isNull$1216;
                    double result$1232 = -1.0d;
                    if (!isNull$1232) {

                        result$1232 = (double) (result$1231 - field$1216);
                    }

                    boolean isNull$1217 = in1.isNullAt(2);
                    double field$1217 = isNull$1217 ? -1.0d : (in1.getDouble(2));
                    boolean isNull$1233 = isNull$1232 || isNull$1217;
                    double result$1233 = -1.0d;
                    if (!isNull$1233) {

                        result$1233 = (double) (result$1232 + field$1217);
                    }

                    boolean isNull$1234 = isNull$1233 || false;
                    double result$1234 = -1.0d;
                    if (!isNull$1234) {

                        result$1234 = (double) (result$1233 / ((double) (((int) 2))));
                    }

                    if (isNull$1234) {
                        out_inputRow$1202.setNullAt(8);
                    } else {
                        out_inputRow$1202.setDouble(8, result$1234);
                    }

                    output.collect(outElement.replace(out_inputRow$1202));
                }
            }
        }
    }

    public void endInput2() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$1214.endBuild();
    }

    public void endInput1() throws Exception {}

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.BoxedWrapperRowData out_inputRow$1202 =
            new org.apache.flink.table.data.BoxedWrapperRowData(9);
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer
            buildSer$1239;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer
            probeSer$1240;
    Projection$1241 buildToBinaryRow$1256;
    private transient java.lang.Object[] buildProjRefs$1257;
    Projection$1250 probeToBinaryRow$1256;
    private transient java.lang.Object[] probeProjRefs$1258;
    LongHashTable$1259 hashTable$1214;

    private void hj_init$1262(Object[] references) throws Exception {
        buildSer$1239 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$1240 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$1257 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$1256 = new Projection$1241(buildProjRefs$1257);
        probeProjRefs$1258 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$1256 = new Projection$1250(probeProjRefs$1258);
    }

    private transient java.lang.Object[] hj_Refs$1263;

    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$1266;

    public BatchMultipleInputStreamOperator$1267(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        hj_Refs$1263 = (((java.lang.Object[]) references[0]));
        hj_init$1262(hj_Refs$1263);
        inputSpecRefs$1266 = (((java.util.ArrayList) references[1]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$1266);
    }

    @Override
    public void open() throws Exception {
        super.open();

        long memorySize$1260 = computeMemorySize(1.0);
        hashTable$1214 = new LongHashTable$1259(memorySize$1260);
    }

    @Override
    public java.util.List getInputs() {
        return java.util.Arrays.asList(
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 1) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`cs_bill_customer_sk` BIGINT, `cs_ext_discount_amt`
                        // DOUBLE, `cs_ext_sales_price` DOUBLE, `cs_ext_wholesale_cost` DOUBLE,
                        // `cs_ext_list_price` DOUBLE>
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

        if (this.hashTable$1214 != null) {
            this.hashTable$1214.close();
            this.hashTable$1214.free();
            this.hashTable$1214 = null;
        }
    }

    public class Projection$1250
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(5);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$1250(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$1251 = in1.isNullAt(0);
            long field$1251 = isNull$1251 ? -1L : (in1.getLong(0));
            if (isNull$1251) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$1251);
            }

            boolean isNull$1252 = in1.isNullAt(1);
            double field$1252 = isNull$1252 ? -1.0d : (in1.getDouble(1));
            if (isNull$1252) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeDouble(1, field$1252);
            }

            boolean isNull$1253 = in1.isNullAt(2);
            double field$1253 = isNull$1253 ? -1.0d : (in1.getDouble(2));
            if (isNull$1253) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeDouble(2, field$1253);
            }

            boolean isNull$1254 = in1.isNullAt(3);
            double field$1254 = isNull$1254 ? -1.0d : (in1.getDouble(3));
            if (isNull$1254) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeDouble(3, field$1254);
            }

            boolean isNull$1255 = in1.isNullAt(4);
            double field$1255 = isNull$1255 ? -1.0d : (in1.getDouble(4));
            if (isNull$1255) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeDouble(4, field$1255);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$1241
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(8);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$1241(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$1242 = in1.isNullAt(0);
            long field$1242 = isNull$1242 ? -1L : (in1.getLong(0));
            if (isNull$1242) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$1242);
            }

            boolean isNull$1243 = in1.isNullAt(1);
            org.apache.flink.table.data.binary.BinaryStringData field$1243 =
                    isNull$1243
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(1)));
            if (isNull$1243) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeString(1, field$1243);
            }

            boolean isNull$1244 = in1.isNullAt(2);
            org.apache.flink.table.data.binary.BinaryStringData field$1244 =
                    isNull$1244
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(2)));
            if (isNull$1244) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeString(2, field$1244);
            }

            boolean isNull$1245 = in1.isNullAt(3);
            org.apache.flink.table.data.binary.BinaryStringData field$1245 =
                    isNull$1245
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(3)));
            if (isNull$1245) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeString(3, field$1245);
            }

            boolean isNull$1246 = in1.isNullAt(4);
            org.apache.flink.table.data.binary.BinaryStringData field$1246 =
                    isNull$1246
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(4)));
            if (isNull$1246) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeString(4, field$1246);
            }

            boolean isNull$1247 = in1.isNullAt(5);
            org.apache.flink.table.data.binary.BinaryStringData field$1247 =
                    isNull$1247
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(5)));
            if (isNull$1247) {
                outWriter.setNullAt(5);
            } else {
                outWriter.writeString(5, field$1247);
            }

            boolean isNull$1248 = in1.isNullAt(6);
            org.apache.flink.table.data.binary.BinaryStringData field$1248 =
                    isNull$1248
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(6)));
            if (isNull$1248) {
                outWriter.setNullAt(6);
            } else {
                outWriter.writeString(6, field$1248);
            }

            boolean isNull$1249 = in1.isNullAt(7);
            org.apache.flink.table.data.binary.BinaryStringData field$1249 =
                    isNull$1249
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(7)));
            if (isNull$1249) {
                outWriter.setNullAt(7);
            } else {
                outWriter.writeString(7, field$1249);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$1259
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$1259(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$1239,
                    probeSer$1240,
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
                return probeToBinaryRow$1256.apply(row);
            }
        }
    }
}
