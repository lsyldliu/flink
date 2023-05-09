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

public final class BatchMultipleInputStreamOperator$395
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

        boolean anyNull$345 = false;
        boolean isNull$344 = in1.isNullAt(0);
        long field$344 = isNull$344 ? -1L : (in1.getLong(0));

        anyNull$345 |= isNull$344;

        if (!anyNull$345) {

            hashTable$346.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in1);
        }
    }

    public void processInput2(org.apache.flink.table.data.RowData in2) throws Exception {

        // evaluate the required expr in advance

        boolean isNull$348 = in2.isNullAt(1);
        long field$348 = isNull$348 ? -1L : (in2.getLong(1));
        boolean result$349 = !isNull$348;
        if (result$349) {

            // evaluate the required expr in advance

            // generate join key for probe side

            // find matches from hash table
            org.apache.flink.table.runtime.util.RowIterator buildIter$353 =
                    isNull$348 ? null : hashTable$346.get(field$348);
            if (buildIter$353 != null) {
                while (buildIter$353.advanceNext()) {
                    org.apache.flink.table.data.RowData buildRow$351 = buildIter$353.getRow();
                    {

                        // evaluate the required expr in advance

                        // evaluate the required expr in advance

                        // project key from input

                        // wrap variable to row

                        currentKeyWriter$356.reset();

                        boolean isNull$347 = in2.isNullAt(0);
                        long field$347 = isNull$347 ? -1L : (in2.getLong(0));
                        if (isNull$347) {
                            currentKeyWriter$356.setNullAt(0);
                        } else {
                            currentKeyWriter$356.writeLong(0, field$347);
                        }

                        currentKeyWriter$356.complete();

                        // look up output buffer using current group key
                        org.apache.flink.table.runtime.util.collections.binary.BytesMap.LookupInfo
                                lookupInfo$357 =
                                        (org.apache.flink.table.runtime.util.collections.binary
                                                        .BytesMap.LookupInfo)
                                                aggregateMap$361.lookup(currentKey$356);
                        org.apache.flink.table.data.binary.BinaryRowData currentAggBuffer$357 =
                                (org.apache.flink.table.data.binary.BinaryRowData)
                                        lookupInfo$357.getValue();

                        if (!lookupInfo$357.isFound()) {

                            // append empty agg buffer into aggregate map for current group key
                            try {
                                currentAggBuffer$357 =
                                        aggregateMap$361.append(lookupInfo$357, emptyAggBuffer$359);
                            } catch (java.io.EOFException exp) {
                                LOG.info(
                                        "BytesHashMap out of memory with {} entries, output directly.",
                                        aggregateMap$361.getNumElements());
                                // hash map out of memory, output directly

                                org.apache.flink.table.runtime.util.KeyValueIterator<
                                                org.apache.flink.table.data.RowData,
                                                org.apache.flink.table.data.RowData>
                                        iterator$365 = null;
                                aggregateMap$361.getEntryIterator(
                                        false); // reuse key/value during iterating
                                while (iterator$365.advanceNext()) {
                                    // set result and output
                                    reuseAggMapKey$363 =
                                            (org.apache.flink.table.data.RowData)
                                                    iterator$365.getKey();
                                    reuseAggBuffer$364 =
                                            (org.apache.flink.table.data.RowData)
                                                    iterator$365.getValue();

                                    // consume the row of agg produce
                                    localagg_consume$367(
                                            hashAggOutput$362.replace(
                                                    reuseAggMapKey$363, reuseAggBuffer$364));
                                }

                                // retry append

                                // reset aggregate map retry append
                                aggregateMap$361.reset();
                                lookupInfo$357 =
                                        (org.apache.flink.table.runtime.util.collections.binary
                                                        .BytesMap.LookupInfo)
                                                aggregateMap$361.lookup(currentKey$356);
                                try {
                                    currentAggBuffer$357 =
                                            aggregateMap$361.append(
                                                    lookupInfo$357, emptyAggBuffer$359);
                                } catch (java.io.EOFException e) {
                                    throw new OutOfMemoryError("BytesHashMap Out of Memory.");
                                }
                            }
                        }
                        // aggregate buffer fields access

                        // do aggregate and update agg buffer

                    }
                }
            }
        }
    }

    public void endInput1() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$346.endBuild();
    }

    public void endInput2() throws Exception {

        localagg_withKeyEndInput$368();
        // call downstream endInput

    }

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.binary.BinaryRowData currentKey$356 =
            new org.apache.flink.table.data.binary.BinaryRowData(1);
    org.apache.flink.table.data.writer.BinaryRowWriter currentKeyWriter$356 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(currentKey$356);
    org.apache.flink.table.data.binary.BinaryRowData emptyAggBuffer$359 =
            new org.apache.flink.table.data.binary.BinaryRowData(0);
    org.apache.flink.table.data.writer.BinaryRowWriter emptyAggBufferWriterTerm$360 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(emptyAggBuffer$359);
    org.apache.flink.table.data.utils.JoinedRowData hashAggOutput$362 =
            new org.apache.flink.table.data.utils.JoinedRowData();
    private transient org.apache.flink.table.data.RowData reuseAggMapKey$363;
    private transient org.apache.flink.table.data.RowData reuseAggBuffer$364;

    private void localagg_consume$367(org.apache.flink.table.data.RowData hashAggOutput$362)
            throws Exception {

        // evaluate the required expr in advance

        output.collect(outElement.replace(hashAggOutput$362));
    }

    private void localagg_withKeyEndInput$368() throws Exception {

        org.apache.flink.table.runtime.util.KeyValueIterator<
                        org.apache.flink.table.data.RowData, org.apache.flink.table.data.RowData>
                iterator$365 = null;
        aggregateMap$361.getEntryIterator(false); // reuse key/value during iterating
        while (iterator$365.advanceNext()) {
            // set result and output
            reuseAggMapKey$363 = (org.apache.flink.table.data.RowData) iterator$365.getKey();
            reuseAggBuffer$364 = (org.apache.flink.table.data.RowData) iterator$365.getValue();

            // consume the row of agg produce
            localagg_consume$367(hashAggOutput$362.replace(reuseAggMapKey$363, reuseAggBuffer$364));
        }
    }

    private transient org.apache.flink.table.types.logical.LogicalType[] groupKeyTypes$370;
    private transient org.apache.flink.table.types.logical.LogicalType[] aggBufferTypes$370;
    private transient org.apache.flink.table.runtime.util.collections.binary.BytesHashMap
            aggregateMap$361;

    private void localagg_init$373(Object[] references) throws Exception {
        groupKeyTypes$370 = (((org.apache.flink.table.types.logical.LogicalType[]) references[0]));
        aggBufferTypes$370 = (((org.apache.flink.table.types.logical.LogicalType[]) references[1]));
    }

    private transient java.lang.Object[] localagg_Refs$374;

    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$376;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$377;
    Projection$378 buildToBinaryRow$383;
    private transient java.lang.Object[] buildProjRefs$384;
    Projection$380 probeToBinaryRow$383;
    private transient java.lang.Object[] probeProjRefs$385;
    LongHashTable$386 hashTable$346;

    private void hj_init$389(Object[] references) throws Exception {
        buildSer$376 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$377 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$384 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$383 = new Projection$378(buildProjRefs$384);
        probeProjRefs$385 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$383 = new Projection$380(probeProjRefs$385);
    }

    private transient java.lang.Object[] hj_Refs$390;
    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$394;

    public BatchMultipleInputStreamOperator$395(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        localagg_Refs$374 = (((java.lang.Object[]) references[0]));
        localagg_init$373(localagg_Refs$374);
        hj_Refs$390 = (((java.lang.Object[]) references[1]));
        hj_init$389(hj_Refs$390);
        inputSpecRefs$394 = (((java.util.ArrayList) references[2]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$394);
    }

    @Override
    public void open() throws Exception {
        super.open();

        // wrap variable to row

        emptyAggBufferWriterTerm$360.reset();

        emptyAggBufferWriterTerm$360.complete();

        long memorySize$371 = computeMemorySize(0.3248730964467005);
        aggregateMap$361 =
                new org.apache.flink.table.runtime.util.collections.binary.BytesHashMap(
                        getContainingTask(),
                        getContainingTask().getEnvironment().getMemoryManager(),
                        memorySize$371,
                        groupKeyTypes$370,
                        aggBufferTypes$370);

        long memorySize$387 = computeMemorySize(0.6751269035532995);
        hashTable$346 = new LongHashTable$386(memorySize$387);
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
                        // InputType: ROW<`ss_item_sk` BIGINT NOT NULL, `ss_sold_date_sk` BIGINT>
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

        if (this.hashTable$346 != null) {
            this.hashTable$346.close();
            this.hashTable$346.free();
            this.hashTable$346 = null;
        }

        aggregateMap$361.free();
    }

    public class Projection$380
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(2);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$380(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$381 = in1.isNullAt(0);
            long field$381 = isNull$381 ? -1L : (in1.getLong(0));
            if (isNull$381) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$381);
            }

            boolean isNull$382 = in1.isNullAt(1);
            long field$382 = isNull$382 ? -1L : (in1.getLong(1));
            if (isNull$382) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$382);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$386
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$386(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$376,
                    probeSer$377,
                    getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    getContainingTask().getEnvironment().getIOManager(),
                    8,
                    730L / getRuntimeContext().getNumberOfParallelSubtasks());
        }

        @Override
        public long getBuildLongKey(org.apache.flink.table.data.RowData row) {
            return row.getLong(0);
        }

        @Override
        public long getProbeLongKey(org.apache.flink.table.data.RowData row) {
            return row.getLong(1);
        }

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData probeToBinary(
                org.apache.flink.table.data.RowData row) {
            if (row instanceof org.apache.flink.table.data.binary.BinaryRowData) {
                return (org.apache.flink.table.data.binary.BinaryRowData) row;
            } else {
                return probeToBinaryRow$383.apply(row);
            }
        }
    }

    public class Projection$378
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$378(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$379 = in1.isNullAt(0);
            long field$379 = isNull$379 ? -1L : (in1.getLong(0));
            if (isNull$379) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$379);
            }

            outWriter.complete();

            return out;
        }
    }
}
