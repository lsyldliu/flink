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

public final class BatchMultipleInputStreamOperator$175
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

        boolean anyNull$94 = false;
        boolean isNull$92 = in1.isNullAt(0);
        long field$92 = isNull$92 ? -1L : (in1.getLong(0));

        anyNull$94 |= isNull$92;

        if (!anyNull$94) {

            hashTable$95.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in1);
        }
    }

    public void processInput2(org.apache.flink.table.data.RowData in2) throws Exception {

        // evaluate the required expr in advance

        // project key from input

        // wrap variable to row

        currentKeyWriter$100.reset();

        boolean isNull$96 = in2.isNullAt(0);
        long field$96 = isNull$96 ? -1L : (in2.getLong(0));
        if (isNull$96) {
            currentKeyWriter$100.setNullAt(0);
        } else {
            currentKeyWriter$100.writeLong(0, field$96);
        }

        boolean isNull$97 = in2.isNullAt(1);
        int field$97 = isNull$97 ? -1 : (in2.getInt(1));
        if (isNull$97) {
            currentKeyWriter$100.setNullAt(1);
        } else {
            currentKeyWriter$100.writeInt(1, field$97);
        }

        boolean isNull$98 = in2.isNullAt(2);
        org.apache.flink.table.data.binary.BinaryStringData field$98 =
                isNull$98
                        ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                        : (((org.apache.flink.table.data.binary.BinaryStringData)
                                in2.getString(2)));
        if (isNull$98) {
            currentKeyWriter$100.setNullAt(2);
        } else {
            currentKeyWriter$100.writeString(2, field$98);
        }

        currentKeyWriter$100.complete();

        // look up output buffer using current group key
        org.apache.flink.table.runtime.util.collections.binary.BytesMap.LookupInfo lookupInfo$101 =
                (org.apache.flink.table.runtime.util.collections.binary.BytesMap.LookupInfo)
                        aggregateMap$109.lookup(currentKey$100);
        org.apache.flink.table.data.binary.BinaryRowData currentAggBuffer$101 =
                (org.apache.flink.table.data.binary.BinaryRowData) lookupInfo$101.getValue();

        if (!lookupInfo$101.isFound()) {

            // append empty agg buffer into aggregate map for current group key
            try {
                currentAggBuffer$101 = aggregateMap$109.append(lookupInfo$101, emptyAggBuffer$103);
            } catch (java.io.EOFException exp) {
                throw new OutOfMemoryError(
                        "Global HashAgg doesn't support fallback to sort-based agg currently.");
            }
        }
        // aggregate buffer fields access
        boolean isNull$105 = currentAggBuffer$101.isNullAt(0);
        double field$105 = isNull$105 ? -1.0d : (currentAggBuffer$101.getDouble(0));
        // do aggregate and update agg buffer

        // wrap variable to row

        boolean isNull$99 = in2.isNullAt(3);
        double field$99 = isNull$99 ? -1.0d : (in2.getDouble(3));
        double result$108 = -1.0d;
        boolean isNull$108;
        if (isNull$99) {

            // --- Cast section generated by
            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

            // --- End cast section

            isNull$108 = isNull$105;
            if (!isNull$108) {
                result$108 = field$105;
            }
        } else {
            double result$107 = -1.0d;
            boolean isNull$107;
            if (isNull$105) {

                // --- Cast section generated by
                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                // --- End cast section

                isNull$107 = isNull$99;
                if (!isNull$107) {
                    result$107 = field$99;
                }
            } else {

                boolean isNull$106 = isNull$105 || isNull$99;
                double result$106 = -1.0d;
                if (!isNull$106) {

                    result$106 = (double) (field$105 + field$99);
                }

                // --- Cast section generated by
                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                // --- End cast section

                isNull$107 = isNull$106;
                if (!isNull$107) {
                    result$107 = result$106;
                }
            }
            // --- Cast section generated by
            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

            // --- End cast section

            isNull$108 = isNull$107;
            if (!isNull$108) {
                result$108 = result$107;
            }
        }
        if (isNull$108) {
            currentAggBuffer$101.setNullAt(0);
        } else {
            currentAggBuffer$101.setDouble(0, result$108);
        }
    }

    public void endInput1() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$95.endBuild();
    }

    public void endInput2() throws Exception {

        hashagg_withKeyEndInput$141();
        // call downstream endInput

        localagg_withKeyEndInput$142();
        // call downstream endInput

    }

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.binary.BinaryRowData currentKey$122 =
            new org.apache.flink.table.data.binary.BinaryRowData(3);
    org.apache.flink.table.data.writer.BinaryRowWriter currentKeyWriter$122 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(currentKey$122);
    org.apache.flink.table.data.binary.BinaryRowData emptyAggBuffer$125 =
            new org.apache.flink.table.data.binary.BinaryRowData(1);
    org.apache.flink.table.data.writer.BinaryRowWriter emptyAggBufferWriterTerm$126 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(emptyAggBuffer$125);
    org.apache.flink.table.data.utils.JoinedRowData hashAggOutput$132 =
            new org.apache.flink.table.data.utils.JoinedRowData();
    private transient org.apache.flink.table.data.RowData reuseAggMapKey$133;
    private transient org.apache.flink.table.data.RowData reuseAggBuffer$134;

    private void localagg_consume$140(org.apache.flink.table.data.RowData hashAggOutput$132)
            throws Exception {

        // evaluate the required expr in advance

        output.collect(outElement.replace(hashAggOutput$132));
    }

    private void localagg_withKeyEndInput$142() throws Exception {

        org.apache.flink.table.runtime.util.KeyValueIterator<
                        org.apache.flink.table.data.RowData, org.apache.flink.table.data.RowData>
                iterator$135 = null;
        aggregateMap$131.getEntryIterator(false); // reuse key/value during iterating
        while (iterator$135.advanceNext()) {
            // set result and output
            reuseAggMapKey$133 = (org.apache.flink.table.data.RowData) iterator$135.getKey();
            reuseAggBuffer$134 = (org.apache.flink.table.data.RowData) iterator$135.getValue();

            // consume the row of agg produce
            localagg_consume$140(hashAggOutput$132.replace(reuseAggMapKey$133, reuseAggBuffer$134));
        }
    }

    private transient org.apache.flink.table.types.logical.LogicalType[] groupKeyTypes$144;
    private transient org.apache.flink.table.types.logical.LogicalType[] aggBufferTypes$144;
    private transient org.apache.flink.table.runtime.util.collections.binary.BytesHashMap
            aggregateMap$131;

    private void localagg_init$147(Object[] references) throws Exception {
        groupKeyTypes$144 = (((org.apache.flink.table.types.logical.LogicalType[]) references[0]));
        aggBufferTypes$144 = (((org.apache.flink.table.types.logical.LogicalType[]) references[1]));
    }

    private transient java.lang.Object[] localagg_Refs$148;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$149;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$150;
    Projection$151 buildToBinaryRow$159;
    private transient java.lang.Object[] buildProjRefs$160;
    Projection$154 probeToBinaryRow$159;
    private transient java.lang.Object[] probeProjRefs$161;
    LongHashTable$162 hashTable$95;

    private void hj_init$165(Object[] references) throws Exception {
        buildSer$149 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$150 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$160 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$159 = new Projection$151(buildProjRefs$160);
        probeProjRefs$161 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$159 = new Projection$154(probeProjRefs$161);
    }

    private transient java.lang.Object[] hj_Refs$166;
    org.apache.flink.table.data.binary.BinaryRowData currentKey$100 =
            new org.apache.flink.table.data.binary.BinaryRowData(3);
    org.apache.flink.table.data.writer.BinaryRowWriter currentKeyWriter$100 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(currentKey$100);
    org.apache.flink.table.data.binary.BinaryRowData emptyAggBuffer$103 =
            new org.apache.flink.table.data.binary.BinaryRowData(1);
    org.apache.flink.table.data.writer.BinaryRowWriter emptyAggBufferWriterTerm$104 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(emptyAggBuffer$103);

    private void hashagg_withKeyEndInput$141() throws Exception {

        org.apache.flink.table.data.RowData reuseAggMapKey$110;
        org.apache.flink.table.data.RowData reuseAggBuffer$110;
        org.apache.flink.table.runtime.util.KeyValueIterator<
                        org.apache.flink.table.data.RowData, org.apache.flink.table.data.RowData>
                iterator$111 = null;
        aggregateMap$109.getEntryIterator(false); // reuse key/value during iterating
        while (iterator$111.advanceNext()) {
            // set result and output
            reuseAggMapKey$110 = (org.apache.flink.table.data.RowData) iterator$111.getKey();
            reuseAggBuffer$110 = (org.apache.flink.table.data.RowData) iterator$111.getValue();

            // consume the row of agg produce

            // evaluate the required expr in advance

            // generate join key for probe side
            boolean isNull$113 = reuseAggMapKey$110.isNullAt(0);
            long field$113 = isNull$113 ? -1L : (reuseAggMapKey$110.getLong(0));
            // find matches from hash table
            org.apache.flink.table.runtime.util.RowIterator buildIter$120 =
                    isNull$113 ? null : hashTable$95.get(field$113);
            if (buildIter$120 != null) {
                while (buildIter$120.advanceNext()) {
                    org.apache.flink.table.data.RowData buildRow$117 = buildIter$120.getRow();
                    {

                        // evaluate the required expr in advance

                        // project key from input

                        // wrap variable to row

                        currentKeyWriter$122.reset();

                        boolean isNull$114 = reuseAggMapKey$110.isNullAt(1);
                        int field$114 = isNull$114 ? -1 : (reuseAggMapKey$110.getInt(1));
                        if (isNull$114) {
                            currentKeyWriter$122.setNullAt(0);
                        } else {
                            currentKeyWriter$122.writeInt(0, field$114);
                        }

                        boolean isNull$115 = reuseAggMapKey$110.isNullAt(2);
                        org.apache.flink.table.data.binary.BinaryStringData field$115 =
                                isNull$115
                                        ? org.apache.flink.table.data.binary.BinaryStringData
                                                .EMPTY_UTF8
                                        : (((org.apache.flink.table.data.binary.BinaryStringData)
                                                reuseAggMapKey$110.getString(2)));
                        if (isNull$115) {
                            currentKeyWriter$122.setNullAt(1);
                        } else {
                            currentKeyWriter$122.writeString(1, field$115);
                        }

                        boolean isNull$119 = buildRow$117.isNullAt(1);
                        int field$119 = isNull$119 ? -1 : (buildRow$117.getInt(1));
                        if (isNull$119) {
                            currentKeyWriter$122.setNullAt(2);
                        } else {
                            currentKeyWriter$122.writeInt(2, field$119);
                        }

                        currentKeyWriter$122.complete();

                        // look up output buffer using current group key
                        org.apache.flink.table.runtime.util.collections.binary.BytesMap.LookupInfo
                                lookupInfo$123 =
                                        (org.apache.flink.table.runtime.util.collections.binary
                                                        .BytesMap.LookupInfo)
                                                aggregateMap$131.lookup(currentKey$122);
                        org.apache.flink.table.data.binary.BinaryRowData currentAggBuffer$123 =
                                (org.apache.flink.table.data.binary.BinaryRowData)
                                        lookupInfo$123.getValue();

                        if (!lookupInfo$123.isFound()) {

                            // append empty agg buffer into aggregate map for current group key
                            try {
                                currentAggBuffer$123 =
                                        aggregateMap$131.append(lookupInfo$123, emptyAggBuffer$125);
                            } catch (java.io.EOFException exp) {
                                LOG.info(
                                        "BytesHashMap out of memory with {} entries, output directly.",
                                        aggregateMap$131.getNumElements());
                                // hash map out of memory, output directly

                                org.apache.flink.table.runtime.util.KeyValueIterator<
                                                org.apache.flink.table.data.RowData,
                                                org.apache.flink.table.data.RowData>
                                        iterator$135 = null;
                                aggregateMap$131.getEntryIterator(
                                        false); // reuse key/value during iterating
                                while (iterator$135.advanceNext()) {
                                    // set result and output
                                    reuseAggMapKey$133 =
                                            (org.apache.flink.table.data.RowData)
                                                    iterator$135.getKey();
                                    reuseAggBuffer$134 =
                                            (org.apache.flink.table.data.RowData)
                                                    iterator$135.getValue();

                                    // consume the row of agg produce
                                    localagg_consume$140(
                                            hashAggOutput$132.replace(
                                                    reuseAggMapKey$133, reuseAggBuffer$134));
                                }

                                // retry append

                                // reset aggregate map retry append
                                aggregateMap$131.reset();
                                lookupInfo$123 =
                                        (org.apache.flink.table.runtime.util.collections.binary
                                                        .BytesMap.LookupInfo)
                                                aggregateMap$131.lookup(currentKey$122);
                                try {
                                    currentAggBuffer$123 =
                                            aggregateMap$131.append(
                                                    lookupInfo$123, emptyAggBuffer$125);
                                } catch (java.io.EOFException e) {
                                    throw new OutOfMemoryError("BytesHashMap Out of Memory.");
                                }
                            }
                        }
                        // aggregate buffer fields access
                        boolean isNull$127 = currentAggBuffer$123.isNullAt(0);
                        double field$127 = isNull$127 ? -1.0d : (currentAggBuffer$123.getDouble(0));
                        // do aggregate and update agg buffer
                        boolean isNull$112 = reuseAggBuffer$110.isNullAt(0);
                        double field$112 = isNull$112 ? -1.0d : (reuseAggBuffer$110.getDouble(0));
                        double result$130 = -1.0d;
                        boolean isNull$130;
                        if (isNull$112) {

                            // --- Cast section generated by
                            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                            // --- End cast section

                            isNull$130 = isNull$127;
                            if (!isNull$130) {
                                result$130 = field$127;
                            }
                        } else {
                            double result$129 = -1.0d;
                            boolean isNull$129;
                            if (isNull$127) {

                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$129 = isNull$112;
                                if (!isNull$129) {
                                    result$129 = field$112;
                                }
                            } else {

                                boolean isNull$128 = isNull$127 || isNull$112;
                                double result$128 = -1.0d;
                                if (!isNull$128) {

                                    result$128 = (double) (field$127 + field$112);
                                }

                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$129 = isNull$128;
                                if (!isNull$129) {
                                    result$129 = result$128;
                                }
                            }
                            // --- Cast section generated by
                            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                            // --- End cast section

                            isNull$130 = isNull$129;
                            if (!isNull$130) {
                                result$130 = result$129;
                            }
                        }
                        if (isNull$130) {
                            currentAggBuffer$123.setNullAt(0);
                        } else {
                            currentAggBuffer$123.setDouble(0, result$130);
                        }
                    }
                }
            }
        }
    }

    private transient org.apache.flink.table.types.logical.LogicalType[] groupKeyTypes$167;
    private transient org.apache.flink.table.types.logical.LogicalType[] aggBufferTypes$167;
    private transient org.apache.flink.table.runtime.util.collections.binary.BytesHashMap
            aggregateMap$109;

    private void hashagg_init$170(Object[] references) throws Exception {
        groupKeyTypes$167 = (((org.apache.flink.table.types.logical.LogicalType[]) references[0]));
        aggBufferTypes$167 = (((org.apache.flink.table.types.logical.LogicalType[]) references[1]));
    }

    private transient java.lang.Object[] hashagg_Refs$171;

    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$174;

    public BatchMultipleInputStreamOperator$175(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        localagg_Refs$148 = (((java.lang.Object[]) references[0]));
        localagg_init$147(localagg_Refs$148);
        hj_Refs$166 = (((java.lang.Object[]) references[1]));
        hj_init$165(hj_Refs$166);
        hashagg_Refs$171 = (((java.lang.Object[]) references[2]));
        hashagg_init$170(hashagg_Refs$171);
        inputSpecRefs$174 = (((java.util.ArrayList) references[3]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$174);
    }

    @Override
    public void open() throws Exception {
        super.open();

        // wrap variable to row

        emptyAggBufferWriterTerm$126.reset();

        if (true) {
            emptyAggBufferWriterTerm$126.setNullAt(0);
        } else {
            emptyAggBufferWriterTerm$126.writeDouble(0, ((double) -1.0d));
        }

        emptyAggBufferWriterTerm$126.complete();

        long memorySize$145 = computeMemorySize(0.24521072796934865);
        aggregateMap$131 =
                new org.apache.flink.table.runtime.util.collections.binary.BytesHashMap(
                        getContainingTask(),
                        getContainingTask().getEnvironment().getMemoryManager(),
                        memorySize$145,
                        groupKeyTypes$144,
                        aggBufferTypes$144);

        long memorySize$163 = computeMemorySize(0.5095785440613027);
        hashTable$95 = new LongHashTable$162(memorySize$163);

        // wrap variable to row

        emptyAggBufferWriterTerm$104.reset();

        if (true) {
            emptyAggBufferWriterTerm$104.setNullAt(0);
        } else {
            emptyAggBufferWriterTerm$104.writeDouble(0, ((double) -1.0d));
        }

        emptyAggBufferWriterTerm$104.complete();

        long memorySize$168 = computeMemorySize(0.24521072796934865);
        aggregateMap$109 =
                new org.apache.flink.table.runtime.util.collections.binary.BytesHashMap(
                        getContainingTask(),
                        getContainingTask().getEnvironment().getMemoryManager(),
                        memorySize$168,
                        groupKeyTypes$167,
                        aggBufferTypes$167);
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

        aggregateMap$109.free();

        if (this.hashTable$95 != null) {
            this.hashTable$95.close();
            this.hashTable$95.free();
            this.hashTable$95 = null;
        }

        aggregateMap$131.free();
    }

    public class LongHashTable$162
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$162(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$149,
                    probeSer$150,
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
                return probeToBinaryRow$159.apply(row);
            }
        }
    }

    public class Projection$151
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(2);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$151(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$152 = in1.isNullAt(0);
            long field$152 = isNull$152 ? -1L : (in1.getLong(0));
            if (isNull$152) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$152);
            }

            boolean isNull$153 = in1.isNullAt(1);
            int field$153 = isNull$153 ? -1 : (in1.getInt(1));
            if (isNull$153) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeInt(1, field$153);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$154
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(4);
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

            boolean isNull$157 = in1.isNullAt(2);
            org.apache.flink.table.data.binary.BinaryStringData field$157 =
                    isNull$157
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(2)));
            if (isNull$157) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeString(2, field$157);
            }

            boolean isNull$158 = in1.isNullAt(3);
            double field$158 = isNull$158 ? -1.0d : (in1.getDouble(3));
            if (isNull$158) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeDouble(3, field$158);
            }

            outWriter.complete();

            return out;
        }
    }
}
