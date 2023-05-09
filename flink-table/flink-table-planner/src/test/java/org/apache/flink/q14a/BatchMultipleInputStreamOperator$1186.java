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

public final class BatchMultipleInputStreamOperator$1186
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

        boolean anyNull$1120 = false;
        boolean isNull$1116 = in2.isNullAt(0);
        long field$1116 = isNull$1116 ? -1L : (in2.getLong(0));

        anyNull$1120 |= isNull$1116;

        if (!anyNull$1120) {

            hashTable$1121.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in2);
        }
    }

    public void processInput1(org.apache.flink.table.data.RowData in1) throws Exception {

        // evaluate the required expr in advance

        // generate join key for probe side
        boolean isNull$1122 = in1.isNullAt(0);
        long field$1122 = isNull$1122 ? -1L : (in1.getLong(0));
        // find matches from hash table
        org.apache.flink.table.runtime.util.RowIterator buildIter$1130 =
                isNull$1122 ? null : hashTable$1121.get(field$1122);
        if (buildIter$1130 != null) {
            while (buildIter$1130.advanceNext()) {
                org.apache.flink.table.data.RowData buildRow$1125 = buildIter$1130.getRow();
                {

                    // evaluate the required expr in advance

                    // evaluate the required expr in advance

                    // project key from input

                    // wrap variable to row

                    currentKeyWriter$1134.reset();

                    boolean isNull$1127 = buildRow$1125.isNullAt(1);
                    int field$1127 = isNull$1127 ? -1 : (buildRow$1125.getInt(1));
                    if (isNull$1127) {
                        currentKeyWriter$1134.setNullAt(0);
                    } else {
                        currentKeyWriter$1134.writeInt(0, field$1127);
                    }

                    boolean isNull$1128 = buildRow$1125.isNullAt(2);
                    int field$1128 = isNull$1128 ? -1 : (buildRow$1125.getInt(2));
                    if (isNull$1128) {
                        currentKeyWriter$1134.setNullAt(1);
                    } else {
                        currentKeyWriter$1134.writeInt(1, field$1128);
                    }

                    boolean isNull$1129 = buildRow$1125.isNullAt(3);
                    int field$1129 = isNull$1129 ? -1 : (buildRow$1125.getInt(3));
                    if (isNull$1129) {
                        currentKeyWriter$1134.setNullAt(2);
                    } else {
                        currentKeyWriter$1134.writeInt(2, field$1129);
                    }

                    currentKeyWriter$1134.complete();

                    // look up output buffer using current group key
                    org.apache.flink.table.runtime.util.collections.binary.BytesMap.LookupInfo
                            lookupInfo$1135 =
                                    (org.apache.flink.table.runtime.util.collections.binary.BytesMap
                                                    .LookupInfo)
                                            aggregateMap$1145.lookup(currentKey$1134);
                    org.apache.flink.table.data.binary.BinaryRowData currentAggBuffer$1135 =
                            (org.apache.flink.table.data.binary.BinaryRowData)
                                    lookupInfo$1135.getValue();

                    if (!lookupInfo$1135.isFound()) {

                        // append empty agg buffer into aggregate map for current group key
                        try {
                            currentAggBuffer$1135 =
                                    aggregateMap$1145.append(lookupInfo$1135, emptyAggBuffer$1137);
                        } catch (java.io.EOFException exp) {
                            LOG.info(
                                    "BytesHashMap out of memory with {} entries, output directly.",
                                    aggregateMap$1145.getNumElements());
                            // hash map out of memory, output directly

                            org.apache.flink.table.runtime.util.KeyValueIterator<
                                            org.apache.flink.table.data.RowData,
                                            org.apache.flink.table.data.RowData>
                                    iterator$1149 = null;
                            aggregateMap$1145.getEntryIterator(
                                    false); // reuse key/value during iterating
                            while (iterator$1149.advanceNext()) {
                                // set result and output
                                reuseAggMapKey$1147 =
                                        (org.apache.flink.table.data.RowData)
                                                iterator$1149.getKey();
                                reuseAggBuffer$1148 =
                                        (org.apache.flink.table.data.RowData)
                                                iterator$1149.getValue();

                                // consume the row of agg produce
                                localagg_consume$1155(
                                        hashAggOutput$1146.replace(
                                                reuseAggMapKey$1147, reuseAggBuffer$1148));
                            }

                            // retry append

                            // reset aggregate map retry append
                            aggregateMap$1145.reset();
                            lookupInfo$1135 =
                                    (org.apache.flink.table.runtime.util.collections.binary.BytesMap
                                                    .LookupInfo)
                                            aggregateMap$1145.lookup(currentKey$1134);
                            try {
                                currentAggBuffer$1135 =
                                        aggregateMap$1145.append(
                                                lookupInfo$1135, emptyAggBuffer$1137);
                            } catch (java.io.EOFException e) {
                                throw new OutOfMemoryError("BytesHashMap Out of Memory.");
                            }
                        }
                    }
                    // aggregate buffer fields access
                    boolean isNull$1139 = currentAggBuffer$1135.isNullAt(0);
                    double field$1139 = isNull$1139 ? -1.0d : (currentAggBuffer$1135.getDouble(0));
                    boolean isNull$1140 = currentAggBuffer$1135.isNullAt(1);
                    long field$1140 = isNull$1140 ? -1L : (currentAggBuffer$1135.getLong(1));
                    // do aggregate and update agg buffer
                    boolean isNull$1123 = in1.isNullAt(1);
                    int field$1123 = isNull$1123 ? -1 : (in1.getInt(1));
                    boolean isNull$1124 = in1.isNullAt(2);
                    double field$1124 = isNull$1124 ? -1.0d : (in1.getDouble(2));
                    boolean isNull$1132 = isNull$1123 || isNull$1124;
                    double result$1132 = -1.0d;
                    if (!isNull$1132) {

                        result$1132 = (double) (((double) (field$1123)) * field$1124);
                    }

                    double result$1143 = -1.0d;
                    boolean isNull$1143;
                    if (isNull$1132) {

                        // --- Cast section generated by
                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                        // --- End cast section

                        isNull$1143 = isNull$1139;
                        if (!isNull$1143) {
                            result$1143 = field$1139;
                        }
                    } else {
                        double result$1142 = -1.0d;
                        boolean isNull$1142;
                        if (isNull$1139) {

                            // --- Cast section generated by
                            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                            // --- End cast section

                            isNull$1142 = isNull$1132;
                            if (!isNull$1142) {
                                result$1142 = result$1132;
                            }
                        } else {

                            boolean isNull$1141 = isNull$1139 || isNull$1132;
                            double result$1141 = -1.0d;
                            if (!isNull$1141) {

                                result$1141 = (double) (field$1139 + result$1132);
                            }

                            // --- Cast section generated by
                            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                            // --- End cast section

                            isNull$1142 = isNull$1141;
                            if (!isNull$1142) {
                                result$1142 = result$1141;
                            }
                        }
                        // --- Cast section generated by
                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                        // --- End cast section

                        isNull$1143 = isNull$1142;
                        if (!isNull$1143) {
                            result$1143 = result$1142;
                        }
                    }
                    if (isNull$1143) {
                        currentAggBuffer$1135.setNullAt(0);
                    } else {
                        currentAggBuffer$1135.setDouble(0, result$1143);
                    }
                    boolean isNull$1144 = isNull$1140 || false;
                    long result$1144 = -1L;
                    if (!isNull$1144) {

                        result$1144 = (long) (field$1140 + ((long) 1L));
                    }

                    if (isNull$1144) {
                        currentAggBuffer$1135.setNullAt(1);
                    } else {
                        currentAggBuffer$1135.setLong(1, result$1144);
                    }
                }
            }
        }
    }

    public void endInput2() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$1121.endBuild();
    }

    public void endInput1() throws Exception {

        localagg_withKeyEndInput$1156();
        // call downstream endInput

    }

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.binary.BinaryRowData currentKey$1134 =
            new org.apache.flink.table.data.binary.BinaryRowData(3);
    org.apache.flink.table.data.writer.BinaryRowWriter currentKeyWriter$1134 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(currentKey$1134);
    org.apache.flink.table.data.binary.BinaryRowData emptyAggBuffer$1137 =
            new org.apache.flink.table.data.binary.BinaryRowData(2);
    org.apache.flink.table.data.writer.BinaryRowWriter emptyAggBufferWriterTerm$1138 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(emptyAggBuffer$1137);
    org.apache.flink.table.data.utils.JoinedRowData hashAggOutput$1146 =
            new org.apache.flink.table.data.utils.JoinedRowData();
    private transient org.apache.flink.table.data.RowData reuseAggMapKey$1147;
    private transient org.apache.flink.table.data.RowData reuseAggBuffer$1148;

    private void localagg_consume$1155(org.apache.flink.table.data.RowData hashAggOutput$1146)
            throws Exception {

        // evaluate the required expr in advance

        output.collect(outElement.replace(hashAggOutput$1146));
    }

    private void localagg_withKeyEndInput$1156() throws Exception {

        org.apache.flink.table.runtime.util.KeyValueIterator<
                        org.apache.flink.table.data.RowData, org.apache.flink.table.data.RowData>
                iterator$1149 = null;
        aggregateMap$1145.getEntryIterator(false); // reuse key/value during iterating
        while (iterator$1149.advanceNext()) {
            // set result and output
            reuseAggMapKey$1147 = (org.apache.flink.table.data.RowData) iterator$1149.getKey();
            reuseAggBuffer$1148 = (org.apache.flink.table.data.RowData) iterator$1149.getValue();

            // consume the row of agg produce
            localagg_consume$1155(
                    hashAggOutput$1146.replace(reuseAggMapKey$1147, reuseAggBuffer$1148));
        }
    }

    private transient org.apache.flink.table.types.logical.LogicalType[] groupKeyTypes$1158;
    private transient org.apache.flink.table.types.logical.LogicalType[] aggBufferTypes$1158;
    private transient org.apache.flink.table.runtime.util.collections.binary.BytesHashMap
            aggregateMap$1145;

    private void localagg_init$1161(Object[] references) throws Exception {
        groupKeyTypes$1158 = (((org.apache.flink.table.types.logical.LogicalType[]) references[0]));
        aggBufferTypes$1158 =
                (((org.apache.flink.table.types.logical.LogicalType[]) references[1]));
    }

    private transient java.lang.Object[] localagg_Refs$1162;

    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer
            buildSer$1164;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer
            probeSer$1165;
    Projection$1166 buildToBinaryRow$1175;
    private transient java.lang.Object[] buildProjRefs$1176;
    Projection$1171 probeToBinaryRow$1175;
    private transient java.lang.Object[] probeProjRefs$1177;
    LongHashTable$1178 hashTable$1121;

    private void hj_init$1181(Object[] references) throws Exception {
        buildSer$1164 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$1165 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$1176 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$1175 = new Projection$1166(buildProjRefs$1176);
        probeProjRefs$1177 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$1175 = new Projection$1171(probeProjRefs$1177);
    }

    private transient java.lang.Object[] hj_Refs$1182;
    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$1185;

    public BatchMultipleInputStreamOperator$1186(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        localagg_Refs$1162 = (((java.lang.Object[]) references[0]));
        localagg_init$1161(localagg_Refs$1162);
        hj_Refs$1182 = (((java.lang.Object[]) references[1]));
        hj_init$1181(hj_Refs$1182);
        inputSpecRefs$1185 = (((java.util.ArrayList) references[2]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$1185);
    }

    @Override
    public void open() throws Exception {
        super.open();

        // wrap variable to row

        emptyAggBufferWriterTerm$1138.reset();

        if (true) {
            emptyAggBufferWriterTerm$1138.setNullAt(0);
        } else {
            emptyAggBufferWriterTerm$1138.writeDouble(0, ((double) -1.0d));
        }

        if (false) {
            emptyAggBufferWriterTerm$1138.setNullAt(1);
        } else {
            emptyAggBufferWriterTerm$1138.writeLong(1, ((long) 0L));
        }

        emptyAggBufferWriterTerm$1138.complete();

        long memorySize$1159 = computeMemorySize(0.3248730964467005);
        aggregateMap$1145 =
                new org.apache.flink.table.runtime.util.collections.binary.BytesHashMap(
                        getContainingTask(),
                        getContainingTask().getEnvironment().getMemoryManager(),
                        memorySize$1159,
                        groupKeyTypes$1158,
                        aggBufferTypes$1158);

        long memorySize$1179 = computeMemorySize(0.6751269035532995);
        hashTable$1121 = new LongHashTable$1178(memorySize$1179);
    }

    @Override
    public java.util.List getInputs() {
        return java.util.Arrays.asList(
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 1) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`ss_item_sk` BIGINT NOT NULL, `ss_quantity` INT,
                        // `ss_list_price` DOUBLE>
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
                        // InputType: ROW<`i_item_sk` BIGINT NOT NULL, `i_brand_id` INT,
                        // `i_class_id` INT, `i_category_id` INT>
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

        if (this.hashTable$1121 != null) {
            this.hashTable$1121.close();
            this.hashTable$1121.free();
            this.hashTable$1121 = null;
        }

        aggregateMap$1145.free();
    }

    public class Projection$1166
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(4);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$1166(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$1167 = in1.isNullAt(0);
            long field$1167 = isNull$1167 ? -1L : (in1.getLong(0));
            if (isNull$1167) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$1167);
            }

            boolean isNull$1168 = in1.isNullAt(1);
            int field$1168 = isNull$1168 ? -1 : (in1.getInt(1));
            if (isNull$1168) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeInt(1, field$1168);
            }

            boolean isNull$1169 = in1.isNullAt(2);
            int field$1169 = isNull$1169 ? -1 : (in1.getInt(2));
            if (isNull$1169) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeInt(2, field$1169);
            }

            boolean isNull$1170 = in1.isNullAt(3);
            int field$1170 = isNull$1170 ? -1 : (in1.getInt(3));
            if (isNull$1170) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeInt(3, field$1170);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$1178
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$1178(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$1164,
                    probeSer$1165,
                    getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    getContainingTask().getEnvironment().getIOManager(),
                    32,
                    402000L / getRuntimeContext().getNumberOfParallelSubtasks());
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
                return probeToBinaryRow$1175.apply(row);
            }
        }
    }

    public class Projection$1171
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(3);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$1171(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$1172 = in1.isNullAt(0);
            long field$1172 = isNull$1172 ? -1L : (in1.getLong(0));
            if (isNull$1172) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$1172);
            }

            boolean isNull$1173 = in1.isNullAt(1);
            int field$1173 = isNull$1173 ? -1 : (in1.getInt(1));
            if (isNull$1173) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeInt(1, field$1173);
            }

            boolean isNull$1174 = in1.isNullAt(2);
            double field$1174 = isNull$1174 ? -1.0d : (in1.getDouble(2));
            if (isNull$1174) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeDouble(2, field$1174);
            }

            outWriter.complete();

            return out;
        }
    }
}
