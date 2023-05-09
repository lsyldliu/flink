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

public final class BatchMultipleInputStreamOperator$331
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

        boolean anyNull$278 = false;
        boolean isNull$274 = in2.isNullAt(0);
        long field$274 = isNull$274 ? -1L : (in2.getLong(0));

        anyNull$278 |= isNull$274;

        if (!anyNull$278) {

            hashTable$279.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in2);
        }
    }

    public void processInput1(org.apache.flink.table.data.RowData in1) throws Exception {

        // evaluate the required expr in advance

        // generate join key for probe side
        boolean isNull$280 = in1.isNullAt(0);
        long field$280 = isNull$280 ? -1L : (in1.getLong(0));
        // find matches from hash table
        org.apache.flink.table.runtime.util.RowIterator buildIter$286 =
                isNull$280 ? null : hashTable$279.get(field$280);
        if (buildIter$286 != null) {
            while (buildIter$286.advanceNext()) {
                org.apache.flink.table.data.RowData buildRow$281 = buildIter$286.getRow();
                {

                    // evaluate the required expr in advance

                    // evaluate the required expr in advance

                    // project key from input

                    // wrap variable to row

                    currentKeyWriter$289.reset();

                    boolean isNull$283 = buildRow$281.isNullAt(1);
                    int field$283 = isNull$283 ? -1 : (buildRow$281.getInt(1));
                    if (isNull$283) {
                        currentKeyWriter$289.setNullAt(0);
                    } else {
                        currentKeyWriter$289.writeInt(0, field$283);
                    }

                    boolean isNull$284 = buildRow$281.isNullAt(2);
                    int field$284 = isNull$284 ? -1 : (buildRow$281.getInt(2));
                    if (isNull$284) {
                        currentKeyWriter$289.setNullAt(1);
                    } else {
                        currentKeyWriter$289.writeInt(1, field$284);
                    }

                    boolean isNull$285 = buildRow$281.isNullAt(3);
                    int field$285 = isNull$285 ? -1 : (buildRow$281.getInt(3));
                    if (isNull$285) {
                        currentKeyWriter$289.setNullAt(2);
                    } else {
                        currentKeyWriter$289.writeInt(2, field$285);
                    }

                    currentKeyWriter$289.complete();

                    // look up output buffer using current group key
                    org.apache.flink.table.runtime.util.collections.binary.BytesMap.LookupInfo
                            lookupInfo$290 =
                                    (org.apache.flink.table.runtime.util.collections.binary.BytesMap
                                                    .LookupInfo)
                                            aggregateMap$294.lookup(currentKey$289);
                    org.apache.flink.table.data.binary.BinaryRowData currentAggBuffer$290 =
                            (org.apache.flink.table.data.binary.BinaryRowData)
                                    lookupInfo$290.getValue();

                    if (!lookupInfo$290.isFound()) {

                        // append empty agg buffer into aggregate map for current group key
                        try {
                            currentAggBuffer$290 =
                                    aggregateMap$294.append(lookupInfo$290, emptyAggBuffer$292);
                        } catch (java.io.EOFException exp) {
                            LOG.info(
                                    "BytesHashMap out of memory with {} entries, output directly.",
                                    aggregateMap$294.getNumElements());
                            // hash map out of memory, output directly

                            org.apache.flink.table.runtime.util.KeyValueIterator<
                                            org.apache.flink.table.data.RowData,
                                            org.apache.flink.table.data.RowData>
                                    iterator$298 = null;
                            aggregateMap$294.getEntryIterator(
                                    false); // reuse key/value during iterating
                            while (iterator$298.advanceNext()) {
                                // set result and output
                                reuseAggMapKey$296 =
                                        (org.apache.flink.table.data.RowData) iterator$298.getKey();
                                reuseAggBuffer$297 =
                                        (org.apache.flink.table.data.RowData)
                                                iterator$298.getValue();

                                // consume the row of agg produce
                                localagg_consume$302(
                                        hashAggOutput$295.replace(
                                                reuseAggMapKey$296, reuseAggBuffer$297));
                            }

                            // retry append

                            // reset aggregate map retry append
                            aggregateMap$294.reset();
                            lookupInfo$290 =
                                    (org.apache.flink.table.runtime.util.collections.binary.BytesMap
                                                    .LookupInfo)
                                            aggregateMap$294.lookup(currentKey$289);
                            try {
                                currentAggBuffer$290 =
                                        aggregateMap$294.append(lookupInfo$290, emptyAggBuffer$292);
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
        hashTable$279.endBuild();
    }

    public void endInput1() throws Exception {

        localagg_withKeyEndInput$303();
        // call downstream endInput

    }

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.binary.BinaryRowData currentKey$289 =
            new org.apache.flink.table.data.binary.BinaryRowData(3);
    org.apache.flink.table.data.writer.BinaryRowWriter currentKeyWriter$289 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(currentKey$289);
    org.apache.flink.table.data.binary.BinaryRowData emptyAggBuffer$292 =
            new org.apache.flink.table.data.binary.BinaryRowData(0);
    org.apache.flink.table.data.writer.BinaryRowWriter emptyAggBufferWriterTerm$293 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(emptyAggBuffer$292);
    org.apache.flink.table.data.utils.JoinedRowData hashAggOutput$295 =
            new org.apache.flink.table.data.utils.JoinedRowData();
    private transient org.apache.flink.table.data.RowData reuseAggMapKey$296;
    private transient org.apache.flink.table.data.RowData reuseAggBuffer$297;

    private void localagg_consume$302(org.apache.flink.table.data.RowData hashAggOutput$295)
            throws Exception {

        // evaluate the required expr in advance

        output.collect(outElement.replace(hashAggOutput$295));
    }

    private void localagg_withKeyEndInput$303() throws Exception {

        org.apache.flink.table.runtime.util.KeyValueIterator<
                        org.apache.flink.table.data.RowData, org.apache.flink.table.data.RowData>
                iterator$298 = null;
        aggregateMap$294.getEntryIterator(false); // reuse key/value during iterating
        while (iterator$298.advanceNext()) {
            // set result and output
            reuseAggMapKey$296 = (org.apache.flink.table.data.RowData) iterator$298.getKey();
            reuseAggBuffer$297 = (org.apache.flink.table.data.RowData) iterator$298.getValue();

            // consume the row of agg produce
            localagg_consume$302(hashAggOutput$295.replace(reuseAggMapKey$296, reuseAggBuffer$297));
        }
    }

    private transient org.apache.flink.table.types.logical.LogicalType[] groupKeyTypes$305;
    private transient org.apache.flink.table.types.logical.LogicalType[] aggBufferTypes$305;
    private transient org.apache.flink.table.runtime.util.collections.binary.BytesHashMap
            aggregateMap$294;

    private void localagg_init$308(Object[] references) throws Exception {
        groupKeyTypes$305 = (((org.apache.flink.table.types.logical.LogicalType[]) references[0]));
        aggBufferTypes$305 = (((org.apache.flink.table.types.logical.LogicalType[]) references[1]));
    }

    private transient java.lang.Object[] localagg_Refs$309;

    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$311;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$312;
    Projection$313 buildToBinaryRow$320;
    private transient java.lang.Object[] buildProjRefs$321;
    Projection$318 probeToBinaryRow$320;
    private transient java.lang.Object[] probeProjRefs$322;
    LongHashTable$323 hashTable$279;

    private void hj_init$326(Object[] references) throws Exception {
        buildSer$311 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$312 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$321 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$320 = new Projection$313(buildProjRefs$321);
        probeProjRefs$322 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$320 = new Projection$318(probeProjRefs$322);
    }

    private transient java.lang.Object[] hj_Refs$327;
    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$330;

    public BatchMultipleInputStreamOperator$331(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        localagg_Refs$309 = (((java.lang.Object[]) references[0]));
        localagg_init$308(localagg_Refs$309);
        hj_Refs$327 = (((java.lang.Object[]) references[1]));
        hj_init$326(hj_Refs$327);
        inputSpecRefs$330 = (((java.util.ArrayList) references[2]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$330);
    }

    @Override
    public void open() throws Exception {
        super.open();

        // wrap variable to row

        emptyAggBufferWriterTerm$293.reset();

        emptyAggBufferWriterTerm$293.complete();

        long memorySize$306 = computeMemorySize(0.3248730964467005);
        aggregateMap$294 =
                new org.apache.flink.table.runtime.util.collections.binary.BytesHashMap(
                        getContainingTask(),
                        getContainingTask().getEnvironment().getMemoryManager(),
                        memorySize$306,
                        groupKeyTypes$305,
                        aggBufferTypes$305);

        long memorySize$324 = computeMemorySize(0.6751269035532995);
        hashTable$279 = new LongHashTable$323(memorySize$324);
    }

    @Override
    public java.util.List getInputs() {
        return java.util.Arrays.asList(
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 1) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`ws_item_sk` BIGINT NOT NULL>
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

        if (this.hashTable$279 != null) {
            this.hashTable$279.close();
            this.hashTable$279.free();
            this.hashTable$279 = null;
        }

        aggregateMap$294.free();
    }

    public class Projection$313
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(4);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$313(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$314 = in1.isNullAt(0);
            long field$314 = isNull$314 ? -1L : (in1.getLong(0));
            if (isNull$314) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$314);
            }

            boolean isNull$315 = in1.isNullAt(1);
            int field$315 = isNull$315 ? -1 : (in1.getInt(1));
            if (isNull$315) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeInt(1, field$315);
            }

            boolean isNull$316 = in1.isNullAt(2);
            int field$316 = isNull$316 ? -1 : (in1.getInt(2));
            if (isNull$316) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeInt(2, field$316);
            }

            boolean isNull$317 = in1.isNullAt(3);
            int field$317 = isNull$317 ? -1 : (in1.getInt(3));
            if (isNull$317) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeInt(3, field$317);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$323
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$323(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$311,
                    probeSer$312,
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
                return probeToBinaryRow$320.apply(row);
            }
        }
    }

    public class Projection$318
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$318(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$319 = in1.isNullAt(0);
            long field$319 = isNull$319 ? -1L : (in1.getLong(0));
            if (isNull$319) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$319);
            }

            outWriter.complete();

            return out;
        }
    }
}
