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

package org.apache.flink.q3;

/** q3 */
public final class BatchMultipleInputStreamOperator$80
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

        boolean anyNull$20 = false;
        boolean isNull$17 = in2.isNullAt(0);
        long field$17 = isNull$17 ? -1L : (in2.getLong(0));

        anyNull$20 |= isNull$17;

        if (!anyNull$20) {

            hashTable$21.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in2);
        }
    }

    public void processInput1(org.apache.flink.table.data.RowData in1) throws Exception {

        // evaluate the required expr in advance

        // generate join key for probe side
        boolean isNull$22 = in1.isNullAt(0);
        long field$22 = isNull$22 ? -1L : (in1.getLong(0));
        // find matches from hash table
        org.apache.flink.table.runtime.util.RowIterator buildIter$29 =
                isNull$22 ? null : hashTable$21.get(field$22);
        if (buildIter$29 != null) {
            while (buildIter$29.advanceNext()) {
                org.apache.flink.table.data.RowData buildRow$25 = buildIter$29.getRow();
                {

                    // evaluate the required expr in advance

                    // evaluate the required expr in advance

                    // project key from input

                    // wrap variable to row

                    currentKeyWriter$32.reset();

                    boolean isNull$24 = in1.isNullAt(2);
                    long field$24 = isNull$24 ? -1L : (in1.getLong(2));
                    if (isNull$24) {
                        currentKeyWriter$32.setNullAt(0);
                    } else {
                        currentKeyWriter$32.writeLong(0, field$24);
                    }

                    boolean isNull$27 = buildRow$25.isNullAt(1);
                    int field$27 = isNull$27 ? -1 : (buildRow$25.getInt(1));
                    if (isNull$27) {
                        currentKeyWriter$32.setNullAt(1);
                    } else {
                        currentKeyWriter$32.writeInt(1, field$27);
                    }

                    boolean isNull$28 = buildRow$25.isNullAt(2);
                    org.apache.flink.table.data.binary.BinaryStringData field$28 =
                            isNull$28
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$25.getString(2)));
                    if (isNull$28) {
                        currentKeyWriter$32.setNullAt(2);
                    } else {
                        currentKeyWriter$32.writeString(2, field$28);
                    }

                    currentKeyWriter$32.complete();

                    // look up output buffer using current group key
                    org.apache.flink.table.runtime.util.collections.binary.BytesMap.LookupInfo
                            lookupInfo$33 =
                                    (org.apache.flink.table.runtime.util.collections.binary.BytesMap
                                                    .LookupInfo)
                                            aggregateMap$41.lookup(currentKey$32);
                    org.apache.flink.table.data.binary.BinaryRowData currentAggBuffer$33 =
                            (org.apache.flink.table.data.binary.BinaryRowData)
                                    lookupInfo$33.getValue();

                    if (!lookupInfo$33.isFound()) {

                        // append empty agg buffer into aggregate map for current group key
                        try {
                            currentAggBuffer$33 =
                                    aggregateMap$41.append(lookupInfo$33, emptyAggBuffer$35);
                        } catch (java.io.EOFException exp) {
                            LOG.info(
                                    "BytesHashMap out of memory with {} entries, output directly.",
                                    aggregateMap$41.getNumElements());
                            // hash map out of memory, output directly

                            org.apache.flink.table.runtime.util.KeyValueIterator<
                                            org.apache.flink.table.data.RowData,
                                            org.apache.flink.table.data.RowData>
                                    iterator$45 = null;
                            aggregateMap$41.getEntryIterator(
                                    false); // reuse key/value during iterating
                            while (iterator$45.advanceNext()) {
                                // set result and output
                                reuseAggMapKey$43 =
                                        (org.apache.flink.table.data.RowData) iterator$45.getKey();
                                reuseAggBuffer$44 =
                                        (org.apache.flink.table.data.RowData)
                                                iterator$45.getValue();

                                // consume the row of agg produce
                                localagg_consume$50(
                                        hashAggOutput$42.replace(
                                                reuseAggMapKey$43, reuseAggBuffer$44));
                            }

                            // retry append

                            // reset aggregate map retry append
                            aggregateMap$41.reset();
                            lookupInfo$33 =
                                    (org.apache.flink.table.runtime.util.collections.binary.BytesMap
                                                    .LookupInfo)
                                            aggregateMap$41.lookup(currentKey$32);
                            try {
                                currentAggBuffer$33 =
                                        aggregateMap$41.append(lookupInfo$33, emptyAggBuffer$35);
                            } catch (java.io.EOFException e) {
                                throw new OutOfMemoryError("BytesHashMap Out of Memory.");
                            }
                        }
                    }
                    // aggregate buffer fields access
                    boolean isNull$37 = currentAggBuffer$33.isNullAt(0);
                    double field$37 = isNull$37 ? -1.0d : (currentAggBuffer$33.getDouble(0));
                    // do aggregate and update agg buffer
                    boolean isNull$23 = in1.isNullAt(1);
                    double field$23 = isNull$23 ? -1.0d : (in1.getDouble(1));
                    double result$40 = -1.0d;
                    boolean isNull$40;
                    if (isNull$23) {

                        // --- Cast section generated by
                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                        // --- End cast section

                        isNull$40 = isNull$37;
                        if (!isNull$40) {
                            result$40 = field$37;
                        }
                    } else {
                        double result$39 = -1.0d;
                        boolean isNull$39;
                        if (isNull$37) {

                            // --- Cast section generated by
                            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                            // --- End cast section

                            isNull$39 = isNull$23;
                            if (!isNull$39) {
                                result$39 = field$23;
                            }
                        } else {

                            boolean isNull$38 = isNull$37 || isNull$23;
                            double result$38 = -1.0d;
                            if (!isNull$38) {

                                result$38 = (double) (field$37 + field$23);
                            }

                            // --- Cast section generated by
                            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                            // --- End cast section

                            isNull$39 = isNull$38;
                            if (!isNull$39) {
                                result$39 = result$38;
                            }
                        }
                        // --- Cast section generated by
                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                        // --- End cast section

                        isNull$40 = isNull$39;
                        if (!isNull$40) {
                            result$40 = result$39;
                        }
                    }
                    if (isNull$40) {
                        currentAggBuffer$33.setNullAt(0);
                    } else {
                        currentAggBuffer$33.setDouble(0, result$40);
                    }
                }
            }
        }
    }

    public void endInput2() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$21.endBuild();
    }

    public void endInput1() throws Exception {

        localagg_withKeyEndInput$51();
        // call downstream endInput

    }

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.binary.BinaryRowData currentKey$32 =
            new org.apache.flink.table.data.binary.BinaryRowData(3);
    org.apache.flink.table.data.writer.BinaryRowWriter currentKeyWriter$32 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(currentKey$32);
    org.apache.flink.table.data.binary.BinaryRowData emptyAggBuffer$35 =
            new org.apache.flink.table.data.binary.BinaryRowData(1);
    org.apache.flink.table.data.writer.BinaryRowWriter emptyAggBufferWriterTerm$36 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(emptyAggBuffer$35);
    org.apache.flink.table.data.utils.JoinedRowData hashAggOutput$42 =
            new org.apache.flink.table.data.utils.JoinedRowData();
    private transient org.apache.flink.table.data.RowData reuseAggMapKey$43;
    private transient org.apache.flink.table.data.RowData reuseAggBuffer$44;

    private void localagg_consume$50(org.apache.flink.table.data.RowData hashAggOutput$42)
            throws Exception {

        // evaluate the required expr in advance

        output.collect(outElement.replace(hashAggOutput$42));
    }

    private void localagg_withKeyEndInput$51() throws Exception {

        org.apache.flink.table.runtime.util.KeyValueIterator<
                        org.apache.flink.table.data.RowData, org.apache.flink.table.data.RowData>
                iterator$45 = null;
        aggregateMap$41.getEntryIterator(false); // reuse key/value during iterating
        while (iterator$45.advanceNext()) {
            // set result and output
            reuseAggMapKey$43 = (org.apache.flink.table.data.RowData) iterator$45.getKey();
            reuseAggBuffer$44 = (org.apache.flink.table.data.RowData) iterator$45.getValue();

            // consume the row of agg produce
            localagg_consume$50(hashAggOutput$42.replace(reuseAggMapKey$43, reuseAggBuffer$44));
        }
    }

    private transient org.apache.flink.table.types.logical.LogicalType[] groupKeyTypes$53;
    private transient org.apache.flink.table.types.logical.LogicalType[] aggBufferTypes$53;
    private transient org.apache.flink.table.runtime.util.collections.binary.BytesHashMap
            aggregateMap$41;

    private void localagg_init$56(Object[] references) throws Exception {
        groupKeyTypes$53 = (((org.apache.flink.table.types.logical.LogicalType[]) references[0]));
        aggBufferTypes$53 = (((org.apache.flink.table.types.logical.LogicalType[]) references[1]));
    }

    private transient java.lang.Object[] localagg_Refs$57;

    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$59;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$60;
    Projection$61 buildToBinaryRow$69;
    private transient java.lang.Object[] buildProjRefs$70;
    Projection$65 probeToBinaryRow$69;
    private transient java.lang.Object[] probeProjRefs$71;
    LongHashTable$72 hashTable$21;

    private void hj_init$75(Object[] references) throws Exception {
        buildSer$59 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$60 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$70 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$69 = new Projection$61(buildProjRefs$70);
        probeProjRefs$71 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$69 = new Projection$65(probeProjRefs$71);
    }

    private transient java.lang.Object[] hj_Refs$76;
    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$79;

    public BatchMultipleInputStreamOperator$80(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        localagg_Refs$57 = (((java.lang.Object[]) references[0]));
        localagg_init$56(localagg_Refs$57);
        hj_Refs$76 = (((java.lang.Object[]) references[1]));
        hj_init$75(hj_Refs$76);
        inputSpecRefs$79 = (((java.util.ArrayList) references[2]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$79);
    }

    @Override
    public void open() throws Exception {
        super.open();

        // wrap variable to row

        emptyAggBufferWriterTerm$36.reset();

        if (true) {
            emptyAggBufferWriterTerm$36.setNullAt(0);
        } else {
            emptyAggBufferWriterTerm$36.writeDouble(0, ((double) -1.0d));
        }

        emptyAggBufferWriterTerm$36.complete();

        long memorySize$54 = computeMemorySize(0.3248730964467005);
        aggregateMap$41 =
                new org.apache.flink.table.runtime.util.collections.binary.BytesHashMap(
                        getContainingTask(),
                        getContainingTask().getEnvironment().getMemoryManager(),
                        memorySize$54,
                        groupKeyTypes$53,
                        aggBufferTypes$53);

        long memorySize$73 = computeMemorySize(0.6751269035532995);
        hashTable$21 = new LongHashTable$72(memorySize$73);
    }

    @Override
    public java.util.List getInputs() {
        return java.util.Arrays.asList(
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 1) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`ss_item_sk` BIGINT NOT NULL, `ss_ext_sales_price` DOUBLE,
                        // `ss_sold_date_sk` BIGINT>
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
                        // InputType: ROW<`i_item_sk` BIGINT NOT NULL, `i_brand_id` INT, `i_brand`
                        // VARCHAR(2147483647)>
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

        if (this.hashTable$21 != null) {
            this.hashTable$21.close();
            this.hashTable$21.free();
            this.hashTable$21 = null;
        }

        aggregateMap$41.free();
    }

    public class LongHashTable$72
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$72(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$59,
                    probeSer$60,
                    getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    getContainingTask().getEnvironment().getIOManager(),
                    32,
                    407L / getRuntimeContext().getNumberOfParallelSubtasks());
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
                return probeToBinaryRow$69.apply(row);
            }
        }
    }

    public class Projection$65
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(3);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$65(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$66 = in1.isNullAt(0);
            long field$66 = isNull$66 ? -1L : (in1.getLong(0));
            if (isNull$66) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$66);
            }

            boolean isNull$67 = in1.isNullAt(1);
            double field$67 = isNull$67 ? -1.0d : (in1.getDouble(1));
            if (isNull$67) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeDouble(1, field$67);
            }

            boolean isNull$68 = in1.isNullAt(2);
            long field$68 = isNull$68 ? -1L : (in1.getLong(2));
            if (isNull$68) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeLong(2, field$68);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$61
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(3);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$61(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$62 = in1.isNullAt(0);
            long field$62 = isNull$62 ? -1L : (in1.getLong(0));
            if (isNull$62) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$62);
            }

            boolean isNull$63 = in1.isNullAt(1);
            int field$63 = isNull$63 ? -1 : (in1.getInt(1));
            if (isNull$63) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeInt(1, field$63);
            }

            boolean isNull$64 = in1.isNullAt(2);
            org.apache.flink.table.data.binary.BinaryStringData field$64 =
                    isNull$64
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(2)));
            if (isNull$64) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeString(2, field$64);
            }

            outWriter.complete();

            return out;
        }
    }
}
