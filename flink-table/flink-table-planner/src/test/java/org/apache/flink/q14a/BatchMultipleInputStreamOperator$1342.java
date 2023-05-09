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

public final class BatchMultipleInputStreamOperator$1342
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

        boolean anyNull$1276 = false;
        boolean isNull$1272 = in2.isNullAt(0);
        long field$1272 = isNull$1272 ? -1L : (in2.getLong(0));

        anyNull$1276 |= isNull$1272;

        if (!anyNull$1276) {

            hashTable$1277.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in2);
        }
    }

    public void processInput1(org.apache.flink.table.data.RowData in1) throws Exception {

        // evaluate the required expr in advance

        // generate join key for probe side
        boolean isNull$1278 = in1.isNullAt(0);
        long field$1278 = isNull$1278 ? -1L : (in1.getLong(0));
        // find matches from hash table
        org.apache.flink.table.runtime.util.RowIterator buildIter$1286 =
                isNull$1278 ? null : hashTable$1277.get(field$1278);
        if (buildIter$1286 != null) {
            while (buildIter$1286.advanceNext()) {
                org.apache.flink.table.data.RowData buildRow$1281 = buildIter$1286.getRow();
                {

                    // evaluate the required expr in advance

                    // evaluate the required expr in advance

                    // project key from input

                    // wrap variable to row

                    currentKeyWriter$1290.reset();

                    boolean isNull$1283 = buildRow$1281.isNullAt(1);
                    int field$1283 = isNull$1283 ? -1 : (buildRow$1281.getInt(1));
                    if (isNull$1283) {
                        currentKeyWriter$1290.setNullAt(0);
                    } else {
                        currentKeyWriter$1290.writeInt(0, field$1283);
                    }

                    boolean isNull$1284 = buildRow$1281.isNullAt(2);
                    int field$1284 = isNull$1284 ? -1 : (buildRow$1281.getInt(2));
                    if (isNull$1284) {
                        currentKeyWriter$1290.setNullAt(1);
                    } else {
                        currentKeyWriter$1290.writeInt(1, field$1284);
                    }

                    boolean isNull$1285 = buildRow$1281.isNullAt(3);
                    int field$1285 = isNull$1285 ? -1 : (buildRow$1281.getInt(3));
                    if (isNull$1285) {
                        currentKeyWriter$1290.setNullAt(2);
                    } else {
                        currentKeyWriter$1290.writeInt(2, field$1285);
                    }

                    currentKeyWriter$1290.complete();

                    // look up output buffer using current group key
                    org.apache.flink.table.runtime.util.collections.binary.BytesMap.LookupInfo
                            lookupInfo$1291 =
                                    (org.apache.flink.table.runtime.util.collections.binary.BytesMap
                                                    .LookupInfo)
                                            aggregateMap$1301.lookup(currentKey$1290);
                    org.apache.flink.table.data.binary.BinaryRowData currentAggBuffer$1291 =
                            (org.apache.flink.table.data.binary.BinaryRowData)
                                    lookupInfo$1291.getValue();

                    if (!lookupInfo$1291.isFound()) {

                        // append empty agg buffer into aggregate map for current group key
                        try {
                            currentAggBuffer$1291 =
                                    aggregateMap$1301.append(lookupInfo$1291, emptyAggBuffer$1293);
                        } catch (java.io.EOFException exp) {
                            LOG.info(
                                    "BytesHashMap out of memory with {} entries, output directly.",
                                    aggregateMap$1301.getNumElements());
                            // hash map out of memory, output directly

                            org.apache.flink.table.runtime.util.KeyValueIterator<
                                            org.apache.flink.table.data.RowData,
                                            org.apache.flink.table.data.RowData>
                                    iterator$1305 = null;
                            aggregateMap$1301.getEntryIterator(
                                    false); // reuse key/value during iterating
                            while (iterator$1305.advanceNext()) {
                                // set result and output
                                reuseAggMapKey$1303 =
                                        (org.apache.flink.table.data.RowData)
                                                iterator$1305.getKey();
                                reuseAggBuffer$1304 =
                                        (org.apache.flink.table.data.RowData)
                                                iterator$1305.getValue();

                                // consume the row of agg produce
                                localagg_consume$1311(
                                        hashAggOutput$1302.replace(
                                                reuseAggMapKey$1303, reuseAggBuffer$1304));
                            }

                            // retry append

                            // reset aggregate map retry append
                            aggregateMap$1301.reset();
                            lookupInfo$1291 =
                                    (org.apache.flink.table.runtime.util.collections.binary.BytesMap
                                                    .LookupInfo)
                                            aggregateMap$1301.lookup(currentKey$1290);
                            try {
                                currentAggBuffer$1291 =
                                        aggregateMap$1301.append(
                                                lookupInfo$1291, emptyAggBuffer$1293);
                            } catch (java.io.EOFException e) {
                                throw new OutOfMemoryError("BytesHashMap Out of Memory.");
                            }
                        }
                    }
                    // aggregate buffer fields access
                    boolean isNull$1295 = currentAggBuffer$1291.isNullAt(0);
                    double field$1295 = isNull$1295 ? -1.0d : (currentAggBuffer$1291.getDouble(0));
                    boolean isNull$1296 = currentAggBuffer$1291.isNullAt(1);
                    long field$1296 = isNull$1296 ? -1L : (currentAggBuffer$1291.getLong(1));
                    // do aggregate and update agg buffer
                    boolean isNull$1279 = in1.isNullAt(1);
                    int field$1279 = isNull$1279 ? -1 : (in1.getInt(1));
                    boolean isNull$1280 = in1.isNullAt(2);
                    double field$1280 = isNull$1280 ? -1.0d : (in1.getDouble(2));
                    boolean isNull$1288 = isNull$1279 || isNull$1280;
                    double result$1288 = -1.0d;
                    if (!isNull$1288) {

                        result$1288 = (double) (((double) (field$1279)) * field$1280);
                    }

                    double result$1299 = -1.0d;
                    boolean isNull$1299;
                    if (isNull$1288) {

                        // --- Cast section generated by
                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                        // --- End cast section

                        isNull$1299 = isNull$1295;
                        if (!isNull$1299) {
                            result$1299 = field$1295;
                        }
                    } else {
                        double result$1298 = -1.0d;
                        boolean isNull$1298;
                        if (isNull$1295) {

                            // --- Cast section generated by
                            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                            // --- End cast section

                            isNull$1298 = isNull$1288;
                            if (!isNull$1298) {
                                result$1298 = result$1288;
                            }
                        } else {

                            boolean isNull$1297 = isNull$1295 || isNull$1288;
                            double result$1297 = -1.0d;
                            if (!isNull$1297) {

                                result$1297 = (double) (field$1295 + result$1288);
                            }

                            // --- Cast section generated by
                            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                            // --- End cast section

                            isNull$1298 = isNull$1297;
                            if (!isNull$1298) {
                                result$1298 = result$1297;
                            }
                        }
                        // --- Cast section generated by
                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                        // --- End cast section

                        isNull$1299 = isNull$1298;
                        if (!isNull$1299) {
                            result$1299 = result$1298;
                        }
                    }
                    if (isNull$1299) {
                        currentAggBuffer$1291.setNullAt(0);
                    } else {
                        currentAggBuffer$1291.setDouble(0, result$1299);
                    }
                    boolean isNull$1300 = isNull$1296 || false;
                    long result$1300 = -1L;
                    if (!isNull$1300) {

                        result$1300 = (long) (field$1296 + ((long) 1L));
                    }

                    if (isNull$1300) {
                        currentAggBuffer$1291.setNullAt(1);
                    } else {
                        currentAggBuffer$1291.setLong(1, result$1300);
                    }
                }
            }
        }
    }

    public void endInput2() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$1277.endBuild();
    }

    public void endInput1() throws Exception {

        localagg_withKeyEndInput$1312();
        // call downstream endInput

    }

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.binary.BinaryRowData currentKey$1290 =
            new org.apache.flink.table.data.binary.BinaryRowData(3);
    org.apache.flink.table.data.writer.BinaryRowWriter currentKeyWriter$1290 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(currentKey$1290);
    org.apache.flink.table.data.binary.BinaryRowData emptyAggBuffer$1293 =
            new org.apache.flink.table.data.binary.BinaryRowData(2);
    org.apache.flink.table.data.writer.BinaryRowWriter emptyAggBufferWriterTerm$1294 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(emptyAggBuffer$1293);
    org.apache.flink.table.data.utils.JoinedRowData hashAggOutput$1302 =
            new org.apache.flink.table.data.utils.JoinedRowData();
    private transient org.apache.flink.table.data.RowData reuseAggMapKey$1303;
    private transient org.apache.flink.table.data.RowData reuseAggBuffer$1304;

    private void localagg_consume$1311(org.apache.flink.table.data.RowData hashAggOutput$1302)
            throws Exception {

        // evaluate the required expr in advance

        output.collect(outElement.replace(hashAggOutput$1302));
    }

    private void localagg_withKeyEndInput$1312() throws Exception {

        org.apache.flink.table.runtime.util.KeyValueIterator<
                        org.apache.flink.table.data.RowData, org.apache.flink.table.data.RowData>
                iterator$1305 = null;
        aggregateMap$1301.getEntryIterator(false); // reuse key/value during iterating
        while (iterator$1305.advanceNext()) {
            // set result and output
            reuseAggMapKey$1303 = (org.apache.flink.table.data.RowData) iterator$1305.getKey();
            reuseAggBuffer$1304 = (org.apache.flink.table.data.RowData) iterator$1305.getValue();

            // consume the row of agg produce
            localagg_consume$1311(
                    hashAggOutput$1302.replace(reuseAggMapKey$1303, reuseAggBuffer$1304));
        }
    }

    private transient org.apache.flink.table.types.logical.LogicalType[] groupKeyTypes$1314;
    private transient org.apache.flink.table.types.logical.LogicalType[] aggBufferTypes$1314;
    private transient org.apache.flink.table.runtime.util.collections.binary.BytesHashMap
            aggregateMap$1301;

    private void localagg_init$1317(Object[] references) throws Exception {
        groupKeyTypes$1314 = (((org.apache.flink.table.types.logical.LogicalType[]) references[0]));
        aggBufferTypes$1314 =
                (((org.apache.flink.table.types.logical.LogicalType[]) references[1]));
    }

    private transient java.lang.Object[] localagg_Refs$1318;

    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer
            buildSer$1320;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer
            probeSer$1321;
    Projection$1322 buildToBinaryRow$1331;
    private transient java.lang.Object[] buildProjRefs$1332;
    Projection$1327 probeToBinaryRow$1331;
    private transient java.lang.Object[] probeProjRefs$1333;
    LongHashTable$1334 hashTable$1277;

    private void hj_init$1337(Object[] references) throws Exception {
        buildSer$1320 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$1321 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$1332 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$1331 = new Projection$1322(buildProjRefs$1332);
        probeProjRefs$1333 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$1331 = new Projection$1327(probeProjRefs$1333);
    }

    private transient java.lang.Object[] hj_Refs$1338;
    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$1341;

    public BatchMultipleInputStreamOperator$1342(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        localagg_Refs$1318 = (((java.lang.Object[]) references[0]));
        localagg_init$1317(localagg_Refs$1318);
        hj_Refs$1338 = (((java.lang.Object[]) references[1]));
        hj_init$1337(hj_Refs$1338);
        inputSpecRefs$1341 = (((java.util.ArrayList) references[2]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$1341);
    }

    @Override
    public void open() throws Exception {
        super.open();

        // wrap variable to row

        emptyAggBufferWriterTerm$1294.reset();

        if (true) {
            emptyAggBufferWriterTerm$1294.setNullAt(0);
        } else {
            emptyAggBufferWriterTerm$1294.writeDouble(0, ((double) -1.0d));
        }

        if (false) {
            emptyAggBufferWriterTerm$1294.setNullAt(1);
        } else {
            emptyAggBufferWriterTerm$1294.writeLong(1, ((long) 0L));
        }

        emptyAggBufferWriterTerm$1294.complete();

        long memorySize$1315 = computeMemorySize(0.3248730964467005);
        aggregateMap$1301 =
                new org.apache.flink.table.runtime.util.collections.binary.BytesHashMap(
                        getContainingTask(),
                        getContainingTask().getEnvironment().getMemoryManager(),
                        memorySize$1315,
                        groupKeyTypes$1314,
                        aggBufferTypes$1314);

        long memorySize$1335 = computeMemorySize(0.6751269035532995);
        hashTable$1277 = new LongHashTable$1334(memorySize$1335);
    }

    @Override
    public java.util.List getInputs() {
        return java.util.Arrays.asList(
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 1) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`cs_item_sk` BIGINT NOT NULL, `cs_quantity` INT,
                        // `cs_list_price` DOUBLE>
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

        if (this.hashTable$1277 != null) {
            this.hashTable$1277.close();
            this.hashTable$1277.free();
            this.hashTable$1277 = null;
        }

        aggregateMap$1301.free();
    }

    public class Projection$1322
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(4);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$1322(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$1323 = in1.isNullAt(0);
            long field$1323 = isNull$1323 ? -1L : (in1.getLong(0));
            if (isNull$1323) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$1323);
            }

            boolean isNull$1324 = in1.isNullAt(1);
            int field$1324 = isNull$1324 ? -1 : (in1.getInt(1));
            if (isNull$1324) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeInt(1, field$1324);
            }

            boolean isNull$1325 = in1.isNullAt(2);
            int field$1325 = isNull$1325 ? -1 : (in1.getInt(2));
            if (isNull$1325) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeInt(2, field$1325);
            }

            boolean isNull$1326 = in1.isNullAt(3);
            int field$1326 = isNull$1326 ? -1 : (in1.getInt(3));
            if (isNull$1326) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeInt(3, field$1326);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$1334
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$1334(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$1320,
                    probeSer$1321,
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
                return probeToBinaryRow$1331.apply(row);
            }
        }
    }

    public class Projection$1327
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(3);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$1327(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$1328 = in1.isNullAt(0);
            long field$1328 = isNull$1328 ? -1L : (in1.getLong(0));
            if (isNull$1328) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$1328);
            }

            boolean isNull$1329 = in1.isNullAt(1);
            int field$1329 = isNull$1329 ? -1 : (in1.getInt(1));
            if (isNull$1329) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeInt(1, field$1329);
            }

            boolean isNull$1330 = in1.isNullAt(2);
            double field$1330 = isNull$1330 ? -1.0d : (in1.getDouble(2));
            if (isNull$1330) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeDouble(2, field$1330);
            }

            outWriter.complete();

            return out;
        }
    }
}
