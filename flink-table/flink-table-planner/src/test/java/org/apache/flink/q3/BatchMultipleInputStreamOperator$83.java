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

public final class BatchMultipleInputStreamOperator$83
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
        // for loop to iterate the ColumnarRowData
        org.apache.flink.table.data.columnar.ColumnarRowData columnarRowData$22 =
                (org.apache.flink.table.data.columnar.ColumnarRowData) in1;
        int numRows$26 = columnarRowData$22.getNumRows();
        for (int i = 0; i < numRows$26; i++) {
            columnarRowData$22.setRowId(i);

            // evaluate the required expr in advance

            // generate join key for probe side
            boolean isNull$23 = columnarRowData$22.isNullAt(0);
            long field$23 = isNull$23 ? -1L : (columnarRowData$22.getLong(0));
            // find matches from hash table
            org.apache.flink.table.runtime.util.RowIterator buildIter$32 =
                    isNull$23 ? null : hashTable$21.get(field$23);
            if (buildIter$32 != null) {
                while (buildIter$32.advanceNext()) {
                    org.apache.flink.table.data.RowData buildRow$28 = buildIter$32.getRow();
                    {

                        // evaluate the required expr in advance

                        // evaluate the required expr in advance

                        // project key from input

                        // wrap variable to row

                        currentKeyWriter$35.reset();

                        boolean isNull$25 = columnarRowData$22.isNullAt(2);
                        long field$25 = isNull$25 ? -1L : (columnarRowData$22.getLong(2));
                        if (isNull$25) {
                            currentKeyWriter$35.setNullAt(0);
                        } else {
                            currentKeyWriter$35.writeLong(0, field$25);
                        }

                        boolean isNull$30 = buildRow$28.isNullAt(1);
                        int field$30 = isNull$30 ? -1 : (buildRow$28.getInt(1));
                        if (isNull$30) {
                            currentKeyWriter$35.setNullAt(1);
                        } else {
                            currentKeyWriter$35.writeInt(1, field$30);
                        }

                        boolean isNull$31 = buildRow$28.isNullAt(2);
                        org.apache.flink.table.data.binary.BinaryStringData field$31 =
                                isNull$31
                                        ? org.apache.flink.table.data.binary.BinaryStringData
                                                .EMPTY_UTF8
                                        : (((org.apache.flink.table.data.binary.BinaryStringData)
                                                buildRow$28.getString(2)));
                        if (isNull$31) {
                            currentKeyWriter$35.setNullAt(2);
                        } else {
                            currentKeyWriter$35.writeString(2, field$31);
                        }

                        currentKeyWriter$35.complete();

                        // look up output buffer using current group key
                        org.apache.flink.table.runtime.util.collections.binary.BytesMap.LookupInfo
                                lookupInfo$36 =
                                        (org.apache.flink.table.runtime.util.collections.binary
                                                        .BytesMap.LookupInfo)
                                                aggregateMap$44.lookup(currentKey$35);
                        org.apache.flink.table.data.binary.BinaryRowData currentAggBuffer$36 =
                                (org.apache.flink.table.data.binary.BinaryRowData)
                                        lookupInfo$36.getValue();

                        if (!lookupInfo$36.isFound()) {

                            // append empty agg buffer into aggregate map for current group key
                            try {
                                currentAggBuffer$36 =
                                        aggregateMap$44.append(lookupInfo$36, emptyAggBuffer$38);
                            } catch (java.io.EOFException exp) {
                                LOG.info(
                                        "BytesHashMap out of memory with {} entries, output directly.",
                                        aggregateMap$44.getNumElements());
                                // hash map out of memory, output directly

                                org.apache.flink.table.runtime.util.KeyValueIterator<
                                                org.apache.flink.table.data.RowData,
                                                org.apache.flink.table.data.RowData>
                                        iterator$48 = null;
                                aggregateMap$44.getEntryIterator(
                                        false); // reuse key/value during iterating
                                while (iterator$48.advanceNext()) {
                                    // set result and output
                                    reuseAggMapKey$46 =
                                            (org.apache.flink.table.data.RowData)
                                                    iterator$48.getKey();
                                    reuseAggBuffer$47 =
                                            (org.apache.flink.table.data.RowData)
                                                    iterator$48.getValue();

                                    // consume the row of agg produce
                                    localagg_consume$53(
                                            hashAggOutput$45.replace(
                                                    reuseAggMapKey$46, reuseAggBuffer$47));
                                }

                                // retry append

                                // reset aggregate map retry append
                                aggregateMap$44.reset();
                                lookupInfo$36 =
                                        (org.apache.flink.table.runtime.util.collections.binary
                                                        .BytesMap.LookupInfo)
                                                aggregateMap$44.lookup(currentKey$35);
                                try {
                                    currentAggBuffer$36 =
                                            aggregateMap$44.append(
                                                    lookupInfo$36, emptyAggBuffer$38);
                                } catch (java.io.EOFException e) {
                                    throw new OutOfMemoryError("BytesHashMap Out of Memory.");
                                }
                            }
                        }
                        // aggregate buffer fields access
                        boolean isNull$40 = currentAggBuffer$36.isNullAt(0);
                        double field$40 = isNull$40 ? -1.0d : (currentAggBuffer$36.getDouble(0));
                        // do aggregate and update agg buffer
                        boolean isNull$24 = columnarRowData$22.isNullAt(1);
                        double field$24 = isNull$24 ? -1.0d : (columnarRowData$22.getDouble(1));
                        double result$43 = -1.0d;
                        boolean isNull$43;
                        if (isNull$24) {

                            // --- Cast section generated by
                            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                            // --- End cast section

                            isNull$43 = isNull$40;
                            if (!isNull$43) {
                                result$43 = field$40;
                            }
                        } else {
                            double result$42 = -1.0d;
                            boolean isNull$42;
                            if (isNull$40) {

                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$42 = isNull$24;
                                if (!isNull$42) {
                                    result$42 = field$24;
                                }
                            } else {

                                boolean isNull$41 = isNull$40 || isNull$24;
                                double result$41 = -1.0d;
                                if (!isNull$41) {

                                    result$41 = (double) (field$40 + field$24);
                                }

                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$42 = isNull$41;
                                if (!isNull$42) {
                                    result$42 = result$41;
                                }
                            }
                            // --- Cast section generated by
                            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                            // --- End cast section

                            isNull$43 = isNull$42;
                            if (!isNull$43) {
                                result$43 = result$42;
                            }
                        }
                        if (isNull$43) {
                            currentAggBuffer$36.setNullAt(0);
                        } else {
                            currentAggBuffer$36.setDouble(0, result$43);
                        }
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

        localagg_withKeyEndInput$54();
        // call downstream endInput

    }

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.binary.BinaryRowData currentKey$35 =
            new org.apache.flink.table.data.binary.BinaryRowData(3);
    org.apache.flink.table.data.writer.BinaryRowWriter currentKeyWriter$35 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(currentKey$35);
    org.apache.flink.table.data.binary.BinaryRowData emptyAggBuffer$38 =
            new org.apache.flink.table.data.binary.BinaryRowData(1);
    org.apache.flink.table.data.writer.BinaryRowWriter emptyAggBufferWriterTerm$39 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(emptyAggBuffer$38);
    org.apache.flink.table.data.utils.JoinedRowData hashAggOutput$45 =
            new org.apache.flink.table.data.utils.JoinedRowData();
    private transient org.apache.flink.table.data.RowData reuseAggMapKey$46;
    private transient org.apache.flink.table.data.RowData reuseAggBuffer$47;

    private void localagg_consume$53(org.apache.flink.table.data.RowData hashAggOutput$45)
            throws Exception {

        // evaluate the required expr in advance

        output.collect(outElement.replace(hashAggOutput$45));
    }

    private void localagg_withKeyEndInput$54() throws Exception {

        org.apache.flink.table.runtime.util.KeyValueIterator<
                        org.apache.flink.table.data.RowData, org.apache.flink.table.data.RowData>
                iterator$48 = null;
        aggregateMap$44.getEntryIterator(false); // reuse key/value during iterating
        while (iterator$48.advanceNext()) {
            // set result and output
            reuseAggMapKey$46 = (org.apache.flink.table.data.RowData) iterator$48.getKey();
            reuseAggBuffer$47 = (org.apache.flink.table.data.RowData) iterator$48.getValue();

            // consume the row of agg produce
            localagg_consume$53(hashAggOutput$45.replace(reuseAggMapKey$46, reuseAggBuffer$47));
        }
    }

    private transient org.apache.flink.table.types.logical.LogicalType[] groupKeyTypes$56;
    private transient org.apache.flink.table.types.logical.LogicalType[] aggBufferTypes$56;
    private transient org.apache.flink.table.runtime.util.collections.binary.BytesHashMap
            aggregateMap$44;

    private void localagg_init$59(Object[] references) throws Exception {
        groupKeyTypes$56 = (((org.apache.flink.table.types.logical.LogicalType[]) references[0]));
        aggBufferTypes$56 = (((org.apache.flink.table.types.logical.LogicalType[]) references[1]));
    }

    private transient java.lang.Object[] localagg_Refs$60;

    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$62;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$63;
    Projection$64 buildToBinaryRow$72;
    private transient java.lang.Object[] buildProjRefs$73;
    Projection$68 probeToBinaryRow$72;
    private transient java.lang.Object[] probeProjRefs$74;
    LongHashTable$75 hashTable$21;

    private void hj_init$78(Object[] references) throws Exception {
        buildSer$62 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$63 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$73 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$72 = new Projection$64(buildProjRefs$73);
        probeProjRefs$74 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$72 = new Projection$68(probeProjRefs$74);
    }

    private transient java.lang.Object[] hj_Refs$79;
    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$82;

    public BatchMultipleInputStreamOperator$83(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        localagg_Refs$60 = (((java.lang.Object[]) references[0]));
        localagg_init$59(localagg_Refs$60);
        hj_Refs$79 = (((java.lang.Object[]) references[1]));
        hj_init$78(hj_Refs$79);
        inputSpecRefs$82 = (((java.util.ArrayList) references[2]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$82);
    }

    @Override
    public void open() throws Exception {
        super.open();

        // wrap variable to row

        emptyAggBufferWriterTerm$39.reset();

        if (true) {
            emptyAggBufferWriterTerm$39.setNullAt(0);
        } else {
            emptyAggBufferWriterTerm$39.writeDouble(0, ((double) -1.0d));
        }

        emptyAggBufferWriterTerm$39.complete();

        long memorySize$57 = computeMemorySize(0.3248730964467005);
        aggregateMap$44 =
                new org.apache.flink.table.runtime.util.collections.binary.BytesHashMap(
                        getContainingTask(),
                        getContainingTask().getEnvironment().getMemoryManager(),
                        memorySize$57,
                        groupKeyTypes$56,
                        aggBufferTypes$56);

        long memorySize$76 = computeMemorySize(0.6751269035532995);
        hashTable$21 = new LongHashTable$75(memorySize$76);
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

        aggregateMap$44.free();
    }

    public class Projection$64
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(3);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$64(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$65 = in1.isNullAt(0);
            long field$65 = isNull$65 ? -1L : (in1.getLong(0));
            if (isNull$65) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$65);
            }

            boolean isNull$66 = in1.isNullAt(1);
            int field$66 = isNull$66 ? -1 : (in1.getInt(1));
            if (isNull$66) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeInt(1, field$66);
            }

            boolean isNull$67 = in1.isNullAt(2);
            org.apache.flink.table.data.binary.BinaryStringData field$67 =
                    isNull$67
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(2)));
            if (isNull$67) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeString(2, field$67);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$68
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(3);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$68(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$69 = in1.isNullAt(0);
            long field$69 = isNull$69 ? -1L : (in1.getLong(0));
            if (isNull$69) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$69);
            }

            boolean isNull$70 = in1.isNullAt(1);
            double field$70 = isNull$70 ? -1.0d : (in1.getDouble(1));
            if (isNull$70) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeDouble(1, field$70);
            }

            boolean isNull$71 = in1.isNullAt(2);
            long field$71 = isNull$71 ? -1L : (in1.getLong(2));
            if (isNull$71) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeLong(2, field$71);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$75
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$75(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$62,
                    probeSer$63,
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
                return probeToBinaryRow$72.apply(row);
            }
        }
    }
}
