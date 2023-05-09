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

public final class BatchMultipleInputStreamOperator$1030
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

        boolean anyNull$964 = false;
        boolean isNull$960 = in2.isNullAt(0);
        long field$960 = isNull$960 ? -1L : (in2.getLong(0));

        anyNull$964 |= isNull$960;

        if (!anyNull$964) {

            hashTable$965.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in2);
        }
    }

    public void processInput1(org.apache.flink.table.data.RowData in1) throws Exception {

        // evaluate the required expr in advance

        // generate join key for probe side
        boolean isNull$966 = in1.isNullAt(0);
        long field$966 = isNull$966 ? -1L : (in1.getLong(0));
        // find matches from hash table
        org.apache.flink.table.runtime.util.RowIterator buildIter$974 =
                isNull$966 ? null : hashTable$965.get(field$966);
        if (buildIter$974 != null) {
            while (buildIter$974.advanceNext()) {
                org.apache.flink.table.data.RowData buildRow$969 = buildIter$974.getRow();
                {

                    // evaluate the required expr in advance

                    // evaluate the required expr in advance

                    // project key from input

                    // wrap variable to row

                    currentKeyWriter$978.reset();

                    boolean isNull$971 = buildRow$969.isNullAt(1);
                    int field$971 = isNull$971 ? -1 : (buildRow$969.getInt(1));
                    if (isNull$971) {
                        currentKeyWriter$978.setNullAt(0);
                    } else {
                        currentKeyWriter$978.writeInt(0, field$971);
                    }

                    boolean isNull$972 = buildRow$969.isNullAt(2);
                    int field$972 = isNull$972 ? -1 : (buildRow$969.getInt(2));
                    if (isNull$972) {
                        currentKeyWriter$978.setNullAt(1);
                    } else {
                        currentKeyWriter$978.writeInt(1, field$972);
                    }

                    boolean isNull$973 = buildRow$969.isNullAt(3);
                    int field$973 = isNull$973 ? -1 : (buildRow$969.getInt(3));
                    if (isNull$973) {
                        currentKeyWriter$978.setNullAt(2);
                    } else {
                        currentKeyWriter$978.writeInt(2, field$973);
                    }

                    currentKeyWriter$978.complete();

                    // look up output buffer using current group key
                    org.apache.flink.table.runtime.util.collections.binary.BytesMap.LookupInfo
                            lookupInfo$979 =
                                    (org.apache.flink.table.runtime.util.collections.binary.BytesMap
                                                    .LookupInfo)
                                            aggregateMap$989.lookup(currentKey$978);
                    org.apache.flink.table.data.binary.BinaryRowData currentAggBuffer$979 =
                            (org.apache.flink.table.data.binary.BinaryRowData)
                                    lookupInfo$979.getValue();

                    if (!lookupInfo$979.isFound()) {

                        // append empty agg buffer into aggregate map for current group key
                        try {
                            currentAggBuffer$979 =
                                    aggregateMap$989.append(lookupInfo$979, emptyAggBuffer$981);
                        } catch (java.io.EOFException exp) {
                            LOG.info(
                                    "BytesHashMap out of memory with {} entries, output directly.",
                                    aggregateMap$989.getNumElements());
                            // hash map out of memory, output directly

                            org.apache.flink.table.runtime.util.KeyValueIterator<
                                            org.apache.flink.table.data.RowData,
                                            org.apache.flink.table.data.RowData>
                                    iterator$993 = null;
                            aggregateMap$989.getEntryIterator(
                                    false); // reuse key/value during iterating
                            while (iterator$993.advanceNext()) {
                                // set result and output
                                reuseAggMapKey$991 =
                                        (org.apache.flink.table.data.RowData) iterator$993.getKey();
                                reuseAggBuffer$992 =
                                        (org.apache.flink.table.data.RowData)
                                                iterator$993.getValue();

                                // consume the row of agg produce
                                localagg_consume$999(
                                        hashAggOutput$990.replace(
                                                reuseAggMapKey$991, reuseAggBuffer$992));
                            }

                            // retry append

                            // reset aggregate map retry append
                            aggregateMap$989.reset();
                            lookupInfo$979 =
                                    (org.apache.flink.table.runtime.util.collections.binary.BytesMap
                                                    .LookupInfo)
                                            aggregateMap$989.lookup(currentKey$978);
                            try {
                                currentAggBuffer$979 =
                                        aggregateMap$989.append(lookupInfo$979, emptyAggBuffer$981);
                            } catch (java.io.EOFException e) {
                                throw new OutOfMemoryError("BytesHashMap Out of Memory.");
                            }
                        }
                    }
                    // aggregate buffer fields access
                    boolean isNull$983 = currentAggBuffer$979.isNullAt(0);
                    double field$983 = isNull$983 ? -1.0d : (currentAggBuffer$979.getDouble(0));
                    boolean isNull$984 = currentAggBuffer$979.isNullAt(1);
                    long field$984 = isNull$984 ? -1L : (currentAggBuffer$979.getLong(1));
                    // do aggregate and update agg buffer
                    boolean isNull$967 = in1.isNullAt(1);
                    int field$967 = isNull$967 ? -1 : (in1.getInt(1));
                    boolean isNull$968 = in1.isNullAt(2);
                    double field$968 = isNull$968 ? -1.0d : (in1.getDouble(2));
                    boolean isNull$976 = isNull$967 || isNull$968;
                    double result$976 = -1.0d;
                    if (!isNull$976) {

                        result$976 = (double) (((double) (field$967)) * field$968);
                    }

                    double result$987 = -1.0d;
                    boolean isNull$987;
                    if (isNull$976) {

                        // --- Cast section generated by
                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                        // --- End cast section

                        isNull$987 = isNull$983;
                        if (!isNull$987) {
                            result$987 = field$983;
                        }
                    } else {
                        double result$986 = -1.0d;
                        boolean isNull$986;
                        if (isNull$983) {

                            // --- Cast section generated by
                            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                            // --- End cast section

                            isNull$986 = isNull$976;
                            if (!isNull$986) {
                                result$986 = result$976;
                            }
                        } else {

                            boolean isNull$985 = isNull$983 || isNull$976;
                            double result$985 = -1.0d;
                            if (!isNull$985) {

                                result$985 = (double) (field$983 + result$976);
                            }

                            // --- Cast section generated by
                            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                            // --- End cast section

                            isNull$986 = isNull$985;
                            if (!isNull$986) {
                                result$986 = result$985;
                            }
                        }
                        // --- Cast section generated by
                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                        // --- End cast section

                        isNull$987 = isNull$986;
                        if (!isNull$987) {
                            result$987 = result$986;
                        }
                    }
                    if (isNull$987) {
                        currentAggBuffer$979.setNullAt(0);
                    } else {
                        currentAggBuffer$979.setDouble(0, result$987);
                    }
                    boolean isNull$988 = isNull$984 || false;
                    long result$988 = -1L;
                    if (!isNull$988) {

                        result$988 = (long) (field$984 + ((long) 1L));
                    }

                    if (isNull$988) {
                        currentAggBuffer$979.setNullAt(1);
                    } else {
                        currentAggBuffer$979.setLong(1, result$988);
                    }
                }
            }
        }
    }

    public void endInput2() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$965.endBuild();
    }

    public void endInput1() throws Exception {

        localagg_withKeyEndInput$1000();
        // call downstream endInput

    }

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.binary.BinaryRowData currentKey$978 =
            new org.apache.flink.table.data.binary.BinaryRowData(3);
    org.apache.flink.table.data.writer.BinaryRowWriter currentKeyWriter$978 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(currentKey$978);
    org.apache.flink.table.data.binary.BinaryRowData emptyAggBuffer$981 =
            new org.apache.flink.table.data.binary.BinaryRowData(2);
    org.apache.flink.table.data.writer.BinaryRowWriter emptyAggBufferWriterTerm$982 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(emptyAggBuffer$981);
    org.apache.flink.table.data.utils.JoinedRowData hashAggOutput$990 =
            new org.apache.flink.table.data.utils.JoinedRowData();
    private transient org.apache.flink.table.data.RowData reuseAggMapKey$991;
    private transient org.apache.flink.table.data.RowData reuseAggBuffer$992;

    private void localagg_consume$999(org.apache.flink.table.data.RowData hashAggOutput$990)
            throws Exception {

        // evaluate the required expr in advance

        output.collect(outElement.replace(hashAggOutput$990));
    }

    private void localagg_withKeyEndInput$1000() throws Exception {

        org.apache.flink.table.runtime.util.KeyValueIterator<
                        org.apache.flink.table.data.RowData, org.apache.flink.table.data.RowData>
                iterator$993 = null;
        aggregateMap$989.getEntryIterator(false); // reuse key/value during iterating
        while (iterator$993.advanceNext()) {
            // set result and output
            reuseAggMapKey$991 = (org.apache.flink.table.data.RowData) iterator$993.getKey();
            reuseAggBuffer$992 = (org.apache.flink.table.data.RowData) iterator$993.getValue();

            // consume the row of agg produce
            localagg_consume$999(hashAggOutput$990.replace(reuseAggMapKey$991, reuseAggBuffer$992));
        }
    }

    private transient org.apache.flink.table.types.logical.LogicalType[] groupKeyTypes$1002;
    private transient org.apache.flink.table.types.logical.LogicalType[] aggBufferTypes$1002;
    private transient org.apache.flink.table.runtime.util.collections.binary.BytesHashMap
            aggregateMap$989;

    private void localagg_init$1005(Object[] references) throws Exception {
        groupKeyTypes$1002 = (((org.apache.flink.table.types.logical.LogicalType[]) references[0]));
        aggBufferTypes$1002 =
                (((org.apache.flink.table.types.logical.LogicalType[]) references[1]));
    }

    private transient java.lang.Object[] localagg_Refs$1006;

    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer
            buildSer$1008;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer
            probeSer$1009;
    Projection$1010 buildToBinaryRow$1019;
    private transient java.lang.Object[] buildProjRefs$1020;
    Projection$1015 probeToBinaryRow$1019;
    private transient java.lang.Object[] probeProjRefs$1021;
    LongHashTable$1022 hashTable$965;

    private void hj_init$1025(Object[] references) throws Exception {
        buildSer$1008 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$1009 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$1020 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$1019 = new Projection$1010(buildProjRefs$1020);
        probeProjRefs$1021 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$1019 = new Projection$1015(probeProjRefs$1021);
    }

    private transient java.lang.Object[] hj_Refs$1026;
    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$1029;

    public BatchMultipleInputStreamOperator$1030(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        localagg_Refs$1006 = (((java.lang.Object[]) references[0]));
        localagg_init$1005(localagg_Refs$1006);
        hj_Refs$1026 = (((java.lang.Object[]) references[1]));
        hj_init$1025(hj_Refs$1026);
        inputSpecRefs$1029 = (((java.util.ArrayList) references[2]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$1029);
    }

    @Override
    public void open() throws Exception {
        super.open();

        // wrap variable to row

        emptyAggBufferWriterTerm$982.reset();

        if (true) {
            emptyAggBufferWriterTerm$982.setNullAt(0);
        } else {
            emptyAggBufferWriterTerm$982.writeDouble(0, ((double) -1.0d));
        }

        if (false) {
            emptyAggBufferWriterTerm$982.setNullAt(1);
        } else {
            emptyAggBufferWriterTerm$982.writeLong(1, ((long) 0L));
        }

        emptyAggBufferWriterTerm$982.complete();

        long memorySize$1003 = computeMemorySize(0.3248730964467005);
        aggregateMap$989 =
                new org.apache.flink.table.runtime.util.collections.binary.BytesHashMap(
                        getContainingTask(),
                        getContainingTask().getEnvironment().getMemoryManager(),
                        memorySize$1003,
                        groupKeyTypes$1002,
                        aggBufferTypes$1002);

        long memorySize$1023 = computeMemorySize(0.6751269035532995);
        hashTable$965 = new LongHashTable$1022(memorySize$1023);
    }

    @Override
    public java.util.List getInputs() {
        return java.util.Arrays.asList(
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 1) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`ws_item_sk` BIGINT NOT NULL, `ws_quantity` INT,
                        // `ws_list_price` DOUBLE>
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

        if (this.hashTable$965 != null) {
            this.hashTable$965.close();
            this.hashTable$965.free();
            this.hashTable$965 = null;
        }

        aggregateMap$989.free();
    }

    public class Projection$1010
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(4);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$1010(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$1011 = in1.isNullAt(0);
            long field$1011 = isNull$1011 ? -1L : (in1.getLong(0));
            if (isNull$1011) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$1011);
            }

            boolean isNull$1012 = in1.isNullAt(1);
            int field$1012 = isNull$1012 ? -1 : (in1.getInt(1));
            if (isNull$1012) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeInt(1, field$1012);
            }

            boolean isNull$1013 = in1.isNullAt(2);
            int field$1013 = isNull$1013 ? -1 : (in1.getInt(2));
            if (isNull$1013) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeInt(2, field$1013);
            }

            boolean isNull$1014 = in1.isNullAt(3);
            int field$1014 = isNull$1014 ? -1 : (in1.getInt(3));
            if (isNull$1014) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeInt(3, field$1014);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$1022
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$1022(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$1008,
                    probeSer$1009,
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
                return probeToBinaryRow$1019.apply(row);
            }
        }
    }

    public class Projection$1015
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(3);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$1015(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$1016 = in1.isNullAt(0);
            long field$1016 = isNull$1016 ? -1L : (in1.getLong(0));
            if (isNull$1016) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$1016);
            }

            boolean isNull$1017 = in1.isNullAt(1);
            int field$1017 = isNull$1017 ? -1 : (in1.getInt(1));
            if (isNull$1017) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeInt(1, field$1017);
            }

            boolean isNull$1018 = in1.isNullAt(2);
            double field$1018 = isNull$1018 ? -1.0d : (in1.getDouble(2));
            if (isNull$1018) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeDouble(2, field$1018);
            }

            outWriter.complete();

            return out;
        }
    }
}
