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

public final class BatchMultipleInputStreamOperator$549
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

        boolean anyNull$496 = false;
        boolean isNull$492 = in2.isNullAt(0);
        long field$492 = isNull$492 ? -1L : (in2.getLong(0));

        anyNull$496 |= isNull$492;

        if (!anyNull$496) {

            hashTable$497.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in2);
        }
    }

    public void processInput1(org.apache.flink.table.data.RowData in1) throws Exception {

        // evaluate the required expr in advance

        // generate join key for probe side
        boolean isNull$498 = in1.isNullAt(0);
        long field$498 = isNull$498 ? -1L : (in1.getLong(0));
        // find matches from hash table
        org.apache.flink.table.runtime.util.RowIterator buildIter$504 =
                isNull$498 ? null : hashTable$497.get(field$498);
        if (buildIter$504 != null) {
            while (buildIter$504.advanceNext()) {
                org.apache.flink.table.data.RowData buildRow$499 = buildIter$504.getRow();
                {

                    // evaluate the required expr in advance

                    // evaluate the required expr in advance

                    // project key from input

                    // wrap variable to row

                    currentKeyWriter$507.reset();

                    boolean isNull$501 = buildRow$499.isNullAt(1);
                    int field$501 = isNull$501 ? -1 : (buildRow$499.getInt(1));
                    if (isNull$501) {
                        currentKeyWriter$507.setNullAt(0);
                    } else {
                        currentKeyWriter$507.writeInt(0, field$501);
                    }

                    boolean isNull$502 = buildRow$499.isNullAt(2);
                    int field$502 = isNull$502 ? -1 : (buildRow$499.getInt(2));
                    if (isNull$502) {
                        currentKeyWriter$507.setNullAt(1);
                    } else {
                        currentKeyWriter$507.writeInt(1, field$502);
                    }

                    boolean isNull$503 = buildRow$499.isNullAt(3);
                    int field$503 = isNull$503 ? -1 : (buildRow$499.getInt(3));
                    if (isNull$503) {
                        currentKeyWriter$507.setNullAt(2);
                    } else {
                        currentKeyWriter$507.writeInt(2, field$503);
                    }

                    currentKeyWriter$507.complete();

                    // look up output buffer using current group key
                    org.apache.flink.table.runtime.util.collections.binary.BytesMap.LookupInfo
                            lookupInfo$508 =
                                    (org.apache.flink.table.runtime.util.collections.binary.BytesMap
                                                    .LookupInfo)
                                            aggregateMap$512.lookup(currentKey$507);
                    org.apache.flink.table.data.binary.BinaryRowData currentAggBuffer$508 =
                            (org.apache.flink.table.data.binary.BinaryRowData)
                                    lookupInfo$508.getValue();

                    if (!lookupInfo$508.isFound()) {

                        // append empty agg buffer into aggregate map for current group key
                        try {
                            currentAggBuffer$508 =
                                    aggregateMap$512.append(lookupInfo$508, emptyAggBuffer$510);
                        } catch (java.io.EOFException exp) {
                            LOG.info(
                                    "BytesHashMap out of memory with {} entries, output directly.",
                                    aggregateMap$512.getNumElements());
                            // hash map out of memory, output directly

                            org.apache.flink.table.runtime.util.KeyValueIterator<
                                            org.apache.flink.table.data.RowData,
                                            org.apache.flink.table.data.RowData>
                                    iterator$516 = null;
                            aggregateMap$512.getEntryIterator(
                                    false); // reuse key/value during iterating
                            while (iterator$516.advanceNext()) {
                                // set result and output
                                reuseAggMapKey$514 =
                                        (org.apache.flink.table.data.RowData) iterator$516.getKey();
                                reuseAggBuffer$515 =
                                        (org.apache.flink.table.data.RowData)
                                                iterator$516.getValue();

                                // consume the row of agg produce
                                localagg_consume$520(
                                        hashAggOutput$513.replace(
                                                reuseAggMapKey$514, reuseAggBuffer$515));
                            }

                            // retry append

                            // reset aggregate map retry append
                            aggregateMap$512.reset();
                            lookupInfo$508 =
                                    (org.apache.flink.table.runtime.util.collections.binary.BytesMap
                                                    .LookupInfo)
                                            aggregateMap$512.lookup(currentKey$507);
                            try {
                                currentAggBuffer$508 =
                                        aggregateMap$512.append(lookupInfo$508, emptyAggBuffer$510);
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

    public void endInput2() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$497.endBuild();
    }

    public void endInput1() throws Exception {

        localagg_withKeyEndInput$521();
        // call downstream endInput

    }

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.binary.BinaryRowData currentKey$507 =
            new org.apache.flink.table.data.binary.BinaryRowData(3);
    org.apache.flink.table.data.writer.BinaryRowWriter currentKeyWriter$507 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(currentKey$507);
    org.apache.flink.table.data.binary.BinaryRowData emptyAggBuffer$510 =
            new org.apache.flink.table.data.binary.BinaryRowData(0);
    org.apache.flink.table.data.writer.BinaryRowWriter emptyAggBufferWriterTerm$511 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(emptyAggBuffer$510);
    org.apache.flink.table.data.utils.JoinedRowData hashAggOutput$513 =
            new org.apache.flink.table.data.utils.JoinedRowData();
    private transient org.apache.flink.table.data.RowData reuseAggMapKey$514;
    private transient org.apache.flink.table.data.RowData reuseAggBuffer$515;

    private void localagg_consume$520(org.apache.flink.table.data.RowData hashAggOutput$513)
            throws Exception {

        // evaluate the required expr in advance

        output.collect(outElement.replace(hashAggOutput$513));
    }

    private void localagg_withKeyEndInput$521() throws Exception {

        org.apache.flink.table.runtime.util.KeyValueIterator<
                        org.apache.flink.table.data.RowData, org.apache.flink.table.data.RowData>
                iterator$516 = null;
        aggregateMap$512.getEntryIterator(false); // reuse key/value during iterating
        while (iterator$516.advanceNext()) {
            // set result and output
            reuseAggMapKey$514 = (org.apache.flink.table.data.RowData) iterator$516.getKey();
            reuseAggBuffer$515 = (org.apache.flink.table.data.RowData) iterator$516.getValue();

            // consume the row of agg produce
            localagg_consume$520(hashAggOutput$513.replace(reuseAggMapKey$514, reuseAggBuffer$515));
        }
    }

    private transient org.apache.flink.table.types.logical.LogicalType[] groupKeyTypes$523;
    private transient org.apache.flink.table.types.logical.LogicalType[] aggBufferTypes$523;
    private transient org.apache.flink.table.runtime.util.collections.binary.BytesHashMap
            aggregateMap$512;

    private void localagg_init$526(Object[] references) throws Exception {
        groupKeyTypes$523 = (((org.apache.flink.table.types.logical.LogicalType[]) references[0]));
        aggBufferTypes$523 = (((org.apache.flink.table.types.logical.LogicalType[]) references[1]));
    }

    private transient java.lang.Object[] localagg_Refs$527;

    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$529;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$530;
    Projection$531 buildToBinaryRow$538;
    private transient java.lang.Object[] buildProjRefs$539;
    Projection$536 probeToBinaryRow$538;
    private transient java.lang.Object[] probeProjRefs$540;
    LongHashTable$541 hashTable$497;

    private void hj_init$544(Object[] references) throws Exception {
        buildSer$529 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$530 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$539 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$538 = new Projection$531(buildProjRefs$539);
        probeProjRefs$540 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$538 = new Projection$536(probeProjRefs$540);
    }

    private transient java.lang.Object[] hj_Refs$545;
    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$548;

    public BatchMultipleInputStreamOperator$549(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        localagg_Refs$527 = (((java.lang.Object[]) references[0]));
        localagg_init$526(localagg_Refs$527);
        hj_Refs$545 = (((java.lang.Object[]) references[1]));
        hj_init$544(hj_Refs$545);
        inputSpecRefs$548 = (((java.util.ArrayList) references[2]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$548);
    }

    @Override
    public void open() throws Exception {
        super.open();

        // wrap variable to row

        emptyAggBufferWriterTerm$511.reset();

        emptyAggBufferWriterTerm$511.complete();

        long memorySize$524 = computeMemorySize(0.3248730964467005);
        aggregateMap$512 =
                new org.apache.flink.table.runtime.util.collections.binary.BytesHashMap(
                        getContainingTask(),
                        getContainingTask().getEnvironment().getMemoryManager(),
                        memorySize$524,
                        groupKeyTypes$523,
                        aggBufferTypes$523);

        long memorySize$542 = computeMemorySize(0.6751269035532995);
        hashTable$497 = new LongHashTable$541(memorySize$542);
    }

    @Override
    public java.util.List getInputs() {
        return java.util.Arrays.asList(
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 1) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`cs_item_sk` BIGINT NOT NULL>
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

        if (this.hashTable$497 != null) {
            this.hashTable$497.close();
            this.hashTable$497.free();
            this.hashTable$497 = null;
        }

        aggregateMap$512.free();
    }

    public class LongHashTable$541
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$541(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$529,
                    probeSer$530,
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
                return probeToBinaryRow$538.apply(row);
            }
        }
    }

    public class Projection$536
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$536(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$537 = in1.isNullAt(0);
            long field$537 = isNull$537 ? -1L : (in1.getLong(0));
            if (isNull$537) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$537);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$531
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(4);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$531(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$532 = in1.isNullAt(0);
            long field$532 = isNull$532 ? -1L : (in1.getLong(0));
            if (isNull$532) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$532);
            }

            boolean isNull$533 = in1.isNullAt(1);
            int field$533 = isNull$533 ? -1 : (in1.getInt(1));
            if (isNull$533) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeInt(1, field$533);
            }

            boolean isNull$534 = in1.isNullAt(2);
            int field$534 = isNull$534 ? -1 : (in1.getInt(2));
            if (isNull$534) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeInt(2, field$534);
            }

            boolean isNull$535 = in1.isNullAt(3);
            int field$535 = isNull$535 ? -1 : (in1.getInt(3));
            if (isNull$535) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeInt(3, field$535);
            }

            outWriter.complete();

            return out;
        }
    }
}
