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

public final class BatchMultipleInputStreamOperator$178
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

        boolean anyNull$97 = false;
        boolean isNull$95 = in1.isNullAt(0);
        long field$95 = isNull$95 ? -1L : (in1.getLong(0));

        anyNull$97 |= isNull$95;

        if (!anyNull$97) {

            hashTable$98.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in1);
        }
    }

    public void processInput2(org.apache.flink.table.data.RowData in2) throws Exception {

        // evaluate the required expr in advance

        // project key from input

        // wrap variable to row

        currentKeyWriter$103.reset();

        boolean isNull$99 = in2.isNullAt(0);
        long field$99 = isNull$99 ? -1L : (in2.getLong(0));
        if (isNull$99) {
            currentKeyWriter$103.setNullAt(0);
        } else {
            currentKeyWriter$103.writeLong(0, field$99);
        }

        boolean isNull$100 = in2.isNullAt(1);
        int field$100 = isNull$100 ? -1 : (in2.getInt(1));
        if (isNull$100) {
            currentKeyWriter$103.setNullAt(1);
        } else {
            currentKeyWriter$103.writeInt(1, field$100);
        }

        boolean isNull$101 = in2.isNullAt(2);
        org.apache.flink.table.data.binary.BinaryStringData field$101 =
                isNull$101
                        ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                        : (((org.apache.flink.table.data.binary.BinaryStringData)
                                in2.getString(2)));
        if (isNull$101) {
            currentKeyWriter$103.setNullAt(2);
        } else {
            currentKeyWriter$103.writeString(2, field$101);
        }

        currentKeyWriter$103.complete();

        // look up output buffer using current group key
        org.apache.flink.table.runtime.util.collections.binary.BytesMap.LookupInfo lookupInfo$104 =
                (org.apache.flink.table.runtime.util.collections.binary.BytesMap.LookupInfo)
                        aggregateMap$112.lookup(currentKey$103);
        org.apache.flink.table.data.binary.BinaryRowData currentAggBuffer$104 =
                (org.apache.flink.table.data.binary.BinaryRowData) lookupInfo$104.getValue();

        if (!lookupInfo$104.isFound()) {

            // append empty agg buffer into aggregate map for current group key
            try {
                currentAggBuffer$104 = aggregateMap$112.append(lookupInfo$104, emptyAggBuffer$106);
            } catch (java.io.EOFException exp) {
                throw new OutOfMemoryError(
                        "Global HashAgg doesn't support fallback to sort-based agg currently.");
            }
        }
        // aggregate buffer fields access
        boolean isNull$108 = currentAggBuffer$104.isNullAt(0);
        double field$108 = isNull$108 ? -1.0d : (currentAggBuffer$104.getDouble(0));
        // do aggregate and update agg buffer

        // wrap variable to row

        boolean isNull$102 = in2.isNullAt(3);
        double field$102 = isNull$102 ? -1.0d : (in2.getDouble(3));
        double result$111 = -1.0d;
        boolean isNull$111;
        if (isNull$102) {

            // --- Cast section generated by
            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

            // --- End cast section

            isNull$111 = isNull$108;
            if (!isNull$111) {
                result$111 = field$108;
            }
        } else {
            double result$110 = -1.0d;
            boolean isNull$110;
            if (isNull$108) {

                // --- Cast section generated by
                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                // --- End cast section

                isNull$110 = isNull$102;
                if (!isNull$110) {
                    result$110 = field$102;
                }
            } else {

                boolean isNull$109 = isNull$108 || isNull$102;
                double result$109 = -1.0d;
                if (!isNull$109) {

                    result$109 = (double) (field$108 + field$102);
                }

                // --- Cast section generated by
                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                // --- End cast section

                isNull$110 = isNull$109;
                if (!isNull$110) {
                    result$110 = result$109;
                }
            }
            // --- Cast section generated by
            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

            // --- End cast section

            isNull$111 = isNull$110;
            if (!isNull$111) {
                result$111 = result$110;
            }
        }
        if (isNull$111) {
            currentAggBuffer$104.setNullAt(0);
        } else {
            currentAggBuffer$104.setDouble(0, result$111);
        }
    }

    public void endInput1() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$98.endBuild();
    }

    public void endInput2() throws Exception {

        hashagg_withKeyEndInput$144();
        // call downstream endInput

        localagg_withKeyEndInput$145();
        // call downstream endInput

    }

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.binary.BinaryRowData currentKey$125 =
            new org.apache.flink.table.data.binary.BinaryRowData(3);
    org.apache.flink.table.data.writer.BinaryRowWriter currentKeyWriter$125 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(currentKey$125);
    org.apache.flink.table.data.binary.BinaryRowData emptyAggBuffer$128 =
            new org.apache.flink.table.data.binary.BinaryRowData(1);
    org.apache.flink.table.data.writer.BinaryRowWriter emptyAggBufferWriterTerm$129 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(emptyAggBuffer$128);
    org.apache.flink.table.data.utils.JoinedRowData hashAggOutput$135 =
            new org.apache.flink.table.data.utils.JoinedRowData();
    private transient org.apache.flink.table.data.RowData reuseAggMapKey$136;
    private transient org.apache.flink.table.data.RowData reuseAggBuffer$137;

    private void localagg_consume$143(org.apache.flink.table.data.RowData hashAggOutput$135)
            throws Exception {

        // evaluate the required expr in advance

        output.collect(outElement.replace(hashAggOutput$135));
    }

    private void localagg_withKeyEndInput$145() throws Exception {

        org.apache.flink.table.runtime.util.KeyValueIterator<
                        org.apache.flink.table.data.RowData, org.apache.flink.table.data.RowData>
                iterator$138 = null;
        aggregateMap$134.getEntryIterator(false); // reuse key/value during iterating
        while (iterator$138.advanceNext()) {
            // set result and output
            reuseAggMapKey$136 = (org.apache.flink.table.data.RowData) iterator$138.getKey();
            reuseAggBuffer$137 = (org.apache.flink.table.data.RowData) iterator$138.getValue();

            // consume the row of agg produce
            localagg_consume$143(hashAggOutput$135.replace(reuseAggMapKey$136, reuseAggBuffer$137));
        }
    }

    private transient org.apache.flink.table.types.logical.LogicalType[] groupKeyTypes$147;
    private transient org.apache.flink.table.types.logical.LogicalType[] aggBufferTypes$147;
    private transient org.apache.flink.table.runtime.util.collections.binary.BytesHashMap
            aggregateMap$134;

    private void localagg_init$150(Object[] references) throws Exception {
        groupKeyTypes$147 = (((org.apache.flink.table.types.logical.LogicalType[]) references[0]));
        aggBufferTypes$147 = (((org.apache.flink.table.types.logical.LogicalType[]) references[1]));
    }

    private transient java.lang.Object[] localagg_Refs$151;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$152;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$153;
    Projection$154 buildToBinaryRow$162;
    private transient java.lang.Object[] buildProjRefs$163;
    Projection$157 probeToBinaryRow$162;
    private transient java.lang.Object[] probeProjRefs$164;
    LongHashTable$165 hashTable$98;

    private void hj_init$168(Object[] references) throws Exception {
        buildSer$152 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$153 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$163 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$162 = new Projection$154(buildProjRefs$163);
        probeProjRefs$164 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$162 = new Projection$157(probeProjRefs$164);
    }

    private transient java.lang.Object[] hj_Refs$169;
    org.apache.flink.table.data.binary.BinaryRowData currentKey$103 =
            new org.apache.flink.table.data.binary.BinaryRowData(3);
    org.apache.flink.table.data.writer.BinaryRowWriter currentKeyWriter$103 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(currentKey$103);
    org.apache.flink.table.data.binary.BinaryRowData emptyAggBuffer$106 =
            new org.apache.flink.table.data.binary.BinaryRowData(1);
    org.apache.flink.table.data.writer.BinaryRowWriter emptyAggBufferWriterTerm$107 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(emptyAggBuffer$106);

    private void hashagg_withKeyEndInput$144() throws Exception {

        org.apache.flink.table.data.RowData reuseAggMapKey$113;
        org.apache.flink.table.data.RowData reuseAggBuffer$113;
        org.apache.flink.table.runtime.util.KeyValueIterator<
                        org.apache.flink.table.data.RowData, org.apache.flink.table.data.RowData>
                iterator$114 = null;
        aggregateMap$112.getEntryIterator(false); // reuse key/value during iterating
        while (iterator$114.advanceNext()) {
            // set result and output
            reuseAggMapKey$113 = (org.apache.flink.table.data.RowData) iterator$114.getKey();
            reuseAggBuffer$113 = (org.apache.flink.table.data.RowData) iterator$114.getValue();

            // consume the row of agg produce

            // evaluate the required expr in advance

            // generate join key for probe side
            boolean isNull$116 = reuseAggMapKey$113.isNullAt(0);
            long field$116 = isNull$116 ? -1L : (reuseAggMapKey$113.getLong(0));
            // find matches from hash table
            org.apache.flink.table.runtime.util.RowIterator buildIter$123 =
                    isNull$116 ? null : hashTable$98.get(field$116);
            if (buildIter$123 != null) {
                while (buildIter$123.advanceNext()) {
                    org.apache.flink.table.data.RowData buildRow$120 = buildIter$123.getRow();
                    {

                        // evaluate the required expr in advance

                        // project key from input

                        // wrap variable to row

                        currentKeyWriter$125.reset();

                        boolean isNull$117 = reuseAggMapKey$113.isNullAt(1);
                        int field$117 = isNull$117 ? -1 : (reuseAggMapKey$113.getInt(1));
                        if (isNull$117) {
                            currentKeyWriter$125.setNullAt(0);
                        } else {
                            currentKeyWriter$125.writeInt(0, field$117);
                        }

                        boolean isNull$118 = reuseAggMapKey$113.isNullAt(2);
                        org.apache.flink.table.data.binary.BinaryStringData field$118 =
                                isNull$118
                                        ? org.apache.flink.table.data.binary.BinaryStringData
                                                .EMPTY_UTF8
                                        : (((org.apache.flink.table.data.binary.BinaryStringData)
                                                reuseAggMapKey$113.getString(2)));
                        if (isNull$118) {
                            currentKeyWriter$125.setNullAt(1);
                        } else {
                            currentKeyWriter$125.writeString(1, field$118);
                        }

                        boolean isNull$122 = buildRow$120.isNullAt(1);
                        int field$122 = isNull$122 ? -1 : (buildRow$120.getInt(1));
                        if (isNull$122) {
                            currentKeyWriter$125.setNullAt(2);
                        } else {
                            currentKeyWriter$125.writeInt(2, field$122);
                        }

                        currentKeyWriter$125.complete();

                        // look up output buffer using current group key
                        org.apache.flink.table.runtime.util.collections.binary.BytesMap.LookupInfo
                                lookupInfo$126 =
                                        (org.apache.flink.table.runtime.util.collections.binary
                                                        .BytesMap.LookupInfo)
                                                aggregateMap$134.lookup(currentKey$125);
                        org.apache.flink.table.data.binary.BinaryRowData currentAggBuffer$126 =
                                (org.apache.flink.table.data.binary.BinaryRowData)
                                        lookupInfo$126.getValue();

                        if (!lookupInfo$126.isFound()) {

                            // append empty agg buffer into aggregate map for current group key
                            try {
                                currentAggBuffer$126 =
                                        aggregateMap$134.append(lookupInfo$126, emptyAggBuffer$128);
                            } catch (java.io.EOFException exp) {
                                LOG.info(
                                        "BytesHashMap out of memory with {} entries, output directly.",
                                        aggregateMap$134.getNumElements());
                                // hash map out of memory, output directly

                                org.apache.flink.table.runtime.util.KeyValueIterator<
                                                org.apache.flink.table.data.RowData,
                                                org.apache.flink.table.data.RowData>
                                        iterator$138 = null;
                                aggregateMap$134.getEntryIterator(
                                        false); // reuse key/value during iterating
                                while (iterator$138.advanceNext()) {
                                    // set result and output
                                    reuseAggMapKey$136 =
                                            (org.apache.flink.table.data.RowData)
                                                    iterator$138.getKey();
                                    reuseAggBuffer$137 =
                                            (org.apache.flink.table.data.RowData)
                                                    iterator$138.getValue();

                                    // consume the row of agg produce
                                    localagg_consume$143(
                                            hashAggOutput$135.replace(
                                                    reuseAggMapKey$136, reuseAggBuffer$137));
                                }

                                // retry append

                                // reset aggregate map retry append
                                aggregateMap$134.reset();
                                lookupInfo$126 =
                                        (org.apache.flink.table.runtime.util.collections.binary
                                                        .BytesMap.LookupInfo)
                                                aggregateMap$134.lookup(currentKey$125);
                                try {
                                    currentAggBuffer$126 =
                                            aggregateMap$134.append(
                                                    lookupInfo$126, emptyAggBuffer$128);
                                } catch (java.io.EOFException e) {
                                    throw new OutOfMemoryError("BytesHashMap Out of Memory.");
                                }
                            }
                        }
                        // aggregate buffer fields access
                        boolean isNull$130 = currentAggBuffer$126.isNullAt(0);
                        double field$130 = isNull$130 ? -1.0d : (currentAggBuffer$126.getDouble(0));
                        // do aggregate and update agg buffer
                        boolean isNull$115 = reuseAggBuffer$113.isNullAt(0);
                        double field$115 = isNull$115 ? -1.0d : (reuseAggBuffer$113.getDouble(0));
                        double result$133 = -1.0d;
                        boolean isNull$133;
                        if (isNull$115) {

                            // --- Cast section generated by
                            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                            // --- End cast section

                            isNull$133 = isNull$130;
                            if (!isNull$133) {
                                result$133 = field$130;
                            }
                        } else {
                            double result$132 = -1.0d;
                            boolean isNull$132;
                            if (isNull$130) {

                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$132 = isNull$115;
                                if (!isNull$132) {
                                    result$132 = field$115;
                                }
                            } else {

                                boolean isNull$131 = isNull$130 || isNull$115;
                                double result$131 = -1.0d;
                                if (!isNull$131) {

                                    result$131 = (double) (field$130 + field$115);
                                }

                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$132 = isNull$131;
                                if (!isNull$132) {
                                    result$132 = result$131;
                                }
                            }
                            // --- Cast section generated by
                            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                            // --- End cast section

                            isNull$133 = isNull$132;
                            if (!isNull$133) {
                                result$133 = result$132;
                            }
                        }
                        if (isNull$133) {
                            currentAggBuffer$126.setNullAt(0);
                        } else {
                            currentAggBuffer$126.setDouble(0, result$133);
                        }
                    }
                }
            }
        }
    }

    private transient org.apache.flink.table.types.logical.LogicalType[] groupKeyTypes$170;
    private transient org.apache.flink.table.types.logical.LogicalType[] aggBufferTypes$170;
    private transient org.apache.flink.table.runtime.util.collections.binary.BytesHashMap
            aggregateMap$112;

    private void hashagg_init$173(Object[] references) throws Exception {
        groupKeyTypes$170 = (((org.apache.flink.table.types.logical.LogicalType[]) references[0]));
        aggBufferTypes$170 = (((org.apache.flink.table.types.logical.LogicalType[]) references[1]));
    }

    private transient java.lang.Object[] hashagg_Refs$174;

    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$177;

    public BatchMultipleInputStreamOperator$178(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        localagg_Refs$151 = (((java.lang.Object[]) references[0]));
        localagg_init$150(localagg_Refs$151);
        hj_Refs$169 = (((java.lang.Object[]) references[1]));
        hj_init$168(hj_Refs$169);
        hashagg_Refs$174 = (((java.lang.Object[]) references[2]));
        hashagg_init$173(hashagg_Refs$174);
        inputSpecRefs$177 = (((java.util.ArrayList) references[3]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$177);
    }

    @Override
    public void open() throws Exception {
        super.open();

        // wrap variable to row

        emptyAggBufferWriterTerm$129.reset();

        if (true) {
            emptyAggBufferWriterTerm$129.setNullAt(0);
        } else {
            emptyAggBufferWriterTerm$129.writeDouble(0, ((double) -1.0d));
        }

        emptyAggBufferWriterTerm$129.complete();

        long memorySize$148 = computeMemorySize(0.24521072796934865);
        aggregateMap$134 =
                new org.apache.flink.table.runtime.util.collections.binary.BytesHashMap(
                        getContainingTask(),
                        getContainingTask().getEnvironment().getMemoryManager(),
                        memorySize$148,
                        groupKeyTypes$147,
                        aggBufferTypes$147);

        long memorySize$166 = computeMemorySize(0.5095785440613027);
        hashTable$98 = new LongHashTable$165(memorySize$166);

        // wrap variable to row

        emptyAggBufferWriterTerm$107.reset();

        if (true) {
            emptyAggBufferWriterTerm$107.setNullAt(0);
        } else {
            emptyAggBufferWriterTerm$107.writeDouble(0, ((double) -1.0d));
        }

        emptyAggBufferWriterTerm$107.complete();

        long memorySize$171 = computeMemorySize(0.24521072796934865);
        aggregateMap$112 =
                new org.apache.flink.table.runtime.util.collections.binary.BytesHashMap(
                        getContainingTask(),
                        getContainingTask().getEnvironment().getMemoryManager(),
                        memorySize$171,
                        groupKeyTypes$170,
                        aggBufferTypes$170);
    }

    @Override
    public java.util.List getInputs() {
        return java.util.Arrays.asList(
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 1) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`d_date_sk` BIGINT NOT NULL, `d_year` INT>
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
                        // InputType: ROW<`ss_sold_date_sk` BIGINT, `i_brand_id` INT, `i_brand`
                        // VARCHAR(2147483647), `sum$0` DOUBLE>
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

        aggregateMap$112.free();

        if (this.hashTable$98 != null) {
            this.hashTable$98.close();
            this.hashTable$98.free();
            this.hashTable$98 = null;
        }

        aggregateMap$134.free();
    }

    public class LongHashTable$165
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$165(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$152,
                    probeSer$153,
                    getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    getContainingTask().getEnvironment().getIOManager(),
                    16,
                    6087L / getRuntimeContext().getNumberOfParallelSubtasks());
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
                return probeToBinaryRow$162.apply(row);
            }
        }
    }

    public class Projection$154
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(2);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$154(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$155 = in1.isNullAt(0);
            long field$155 = isNull$155 ? -1L : (in1.getLong(0));
            if (isNull$155) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$155);
            }

            boolean isNull$156 = in1.isNullAt(1);
            int field$156 = isNull$156 ? -1 : (in1.getInt(1));
            if (isNull$156) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeInt(1, field$156);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$157
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(4);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$157(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$158 = in1.isNullAt(0);
            long field$158 = isNull$158 ? -1L : (in1.getLong(0));
            if (isNull$158) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$158);
            }

            boolean isNull$159 = in1.isNullAt(1);
            int field$159 = isNull$159 ? -1 : (in1.getInt(1));
            if (isNull$159) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeInt(1, field$159);
            }

            boolean isNull$160 = in1.isNullAt(2);
            org.apache.flink.table.data.binary.BinaryStringData field$160 =
                    isNull$160
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(2)));
            if (isNull$160) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeString(2, field$160);
            }

            boolean isNull$161 = in1.isNullAt(3);
            double field$161 = isNull$161 ? -1.0d : (in1.getDouble(3));
            if (isNull$161) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeDouble(3, field$161);
            }

            outWriter.complete();

            return out;
        }
    }
}
