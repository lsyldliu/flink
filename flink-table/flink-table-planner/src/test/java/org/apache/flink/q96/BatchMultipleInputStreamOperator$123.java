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

package org.apache.flink.q96;

public final class BatchMultipleInputStreamOperator$123
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

        boolean anyNull$30 = false;
        boolean isNull$29 = in1.isNullAt(0);
        long field$29 = isNull$29 ? -1L : (in1.getLong(0));

        anyNull$30 |= isNull$29;

        if (!anyNull$30) {

            hashTable$31.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in1);
        }
    }

    public void processInput2(org.apache.flink.table.data.RowData in2) throws Exception {

        // evaluate the required expr in advance

        boolean anyNull$33 = false;
        boolean isNull$32 = in2.isNullAt(0);
        long field$32 = isNull$32 ? -1L : (in2.getLong(0));

        anyNull$33 |= isNull$32;

        if (!anyNull$33) {

            hashTable$34.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in2);
        }
    }

    public void processInput3(org.apache.flink.table.data.RowData in3) throws Exception {

        // evaluate the required expr in advance

        boolean anyNull$36 = false;
        boolean isNull$35 = in3.isNullAt(0);
        long field$35 = isNull$35 ? -1L : (in3.getLong(0));

        anyNull$36 |= isNull$35;

        if (!anyNull$36) {

            hashTable$37.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in3);
        }
    }

    public void processInput4(org.apache.flink.table.data.RowData in4) throws Exception {
        // for loop to iterate the ColumnarRowData
        org.apache.flink.table.data.columnar.ColumnarRowData columnarRowData$38 =
                (org.apache.flink.table.data.columnar.ColumnarRowData) in4;
        int numRows$42 = columnarRowData$38.getNumRows();
        for (int i = 0; i < numRows$42; i++) {
            columnarRowData$38.setRowId(i);

            // evaluate the required expr in advance

            boolean isNull$39 = columnarRowData$38.isNullAt(0);
            long field$39 = isNull$39 ? -1L : (columnarRowData$38.getLong(0));
            boolean result$44 = !isNull$39;
            if (result$44) {

                // evaluate the required expr in advance

                // generate join key for probe side

                // find matches from hash table
                org.apache.flink.table.runtime.util.RowIterator buildIter$48 =
                        isNull$39 ? null : hashTable$37.get(field$39);
                if (buildIter$48 != null) {
                    while (buildIter$48.advanceNext()) {
                        org.apache.flink.table.data.RowData buildRow$46 = buildIter$48.getRow();
                        {

                            // evaluate the required expr in advance

                            // evaluate the required expr in advance

                            // generate join key for probe side
                            boolean isNull$40 = columnarRowData$38.isNullAt(1);
                            long field$40 = isNull$40 ? -1L : (columnarRowData$38.getLong(1));
                            // find matches from hash table
                            org.apache.flink.table.runtime.util.RowIterator buildIter$53 =
                                    isNull$40 ? null : hashTable$34.get(field$40);
                            if (buildIter$53 != null) {
                                while (buildIter$53.advanceNext()) {
                                    org.apache.flink.table.data.RowData buildRow$51 =
                                            buildIter$53.getRow();
                                    {

                                        // evaluate the required expr in advance

                                        // evaluate the required expr in advance

                                        // generate join key for probe side
                                        boolean isNull$41 = columnarRowData$38.isNullAt(2);
                                        long field$41 =
                                                isNull$41 ? -1L : (columnarRowData$38.getLong(2));
                                        // find matches from hash table
                                        org.apache.flink.table.runtime.util.RowIterator
                                                buildIter$58 =
                                                        isNull$41
                                                                ? null
                                                                : hashTable$31.get(field$41);
                                        if (buildIter$58 != null) {
                                            while (buildIter$58.advanceNext()) {
                                                org.apache.flink.table.data.RowData buildRow$56 =
                                                        buildIter$58.getRow();
                                                {

                                                    // evaluate the required expr in advance

                                                    // evaluate the required expr in advance

                                                    if (!hasInput$63) {
                                                        hasInput$63 = true;
                                                        // init agg buffer
                                                        localagg$61_agg0_count1IsNull = false;
                                                        localagg$61_agg0_count1 = ((long) 0L);
                                                    }
                                                    // update agg buffer to do aggregate

                                                    boolean isNull$62 =
                                                            localagg$61_agg0_count1IsNull || false;
                                                    long result$62 = -1L;
                                                    if (!isNull$62) {

                                                        result$62 =
                                                                (long)
                                                                        (localagg$61_agg0_count1
                                                                                + ((long) 1L));
                                                    }

                                                    localagg$61_agg0_count1IsNull = isNull$62;
                                                    if (!isNull$62) {
                                                        // copy result term
                                                        localagg$61_agg0_count1 = result$62;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public void endInput1() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$31.endBuild();
    }

    public void endInput2() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$34.endBuild();
    }

    public void endInput3() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$37.endBuild();
    }

    public void endInput4() throws Exception {

        localagg_withoutKeyEndInput$65();
        // call downstream endInput

    }

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    long localagg$61_agg0_count1;
    boolean localagg$61_agg0_count1IsNull;
    private boolean hasInput$63 = false;
    org.apache.flink.table.data.binary.BinaryRowData valueRow$64 =
            new org.apache.flink.table.data.binary.BinaryRowData(1);
    org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
            new org.apache.flink.table.data.writer.BinaryRowWriter(valueRow$64);

    private void localagg_withoutKeyEndInput$65() throws Exception {
        if (hasInput$63) {

            // wrap variable to row

            outWriter.reset();

            if (localagg$61_agg0_count1IsNull) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, localagg$61_agg0_count1);
            }

            outWriter.complete();

            // evaluate the required expr in advance

            output.collect(outElement.replace(valueRow$64));
        }
    }

    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$70;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$71;
    Projection$72 buildToBinaryRow$76;
    private transient java.lang.Object[] buildProjRefs$77;
    Projection$74 probeToBinaryRow$76;
    private transient java.lang.Object[] probeProjRefs$78;
    LongHashTable$79 hashTable$31;

    private void hj_init$82(Object[] references) throws Exception {
        buildSer$70 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$71 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$77 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$76 = new Projection$72(buildProjRefs$77);
        probeProjRefs$78 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$76 = new Projection$74(probeProjRefs$78);
    }

    private transient java.lang.Object[] hj_Refs$83;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$85;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$86;
    Projection$87 buildToBinaryRow$92;
    private transient java.lang.Object[] buildProjRefs$93;
    Projection$89 probeToBinaryRow$92;
    private transient java.lang.Object[] probeProjRefs$94;
    LongHashTable$95 hashTable$34;

    private void hj_init$98(Object[] references) throws Exception {
        buildSer$85 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$86 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$93 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$92 = new Projection$87(buildProjRefs$93);
        probeProjRefs$94 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$92 = new Projection$89(probeProjRefs$94);
    }

    private transient java.lang.Object[] hj_Refs$99;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$101;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$102;
    Projection$103 buildToBinaryRow$109;
    private transient java.lang.Object[] buildProjRefs$110;
    Projection$105 probeToBinaryRow$109;
    private transient java.lang.Object[] probeProjRefs$111;
    LongHashTable$112 hashTable$37;

    private void hj_init$115(Object[] references) throws Exception {
        buildSer$101 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$102 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$110 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$109 = new Projection$103(buildProjRefs$110);
        probeProjRefs$111 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$109 = new Projection$105(probeProjRefs$111);
    }

    private transient java.lang.Object[] hj_Refs$116;
    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$122;

    public BatchMultipleInputStreamOperator$123(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 4);
        this.parameters = parameters;
        hj_Refs$83 = (((java.lang.Object[]) references[0]));
        hj_init$82(hj_Refs$83);
        hj_Refs$99 = (((java.lang.Object[]) references[1]));
        hj_init$98(hj_Refs$99);
        hj_Refs$116 = (((java.lang.Object[]) references[2]));
        hj_init$115(hj_Refs$116);
        inputSpecRefs$122 = (((java.util.ArrayList) references[3]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$122);
    }

    @Override
    public void open() throws Exception {
        super.open();

        long memorySize$80 = computeMemorySize(0.3333333333333333);
        hashTable$31 = new LongHashTable$79(memorySize$80);
        long memorySize$96 = computeMemorySize(0.3333333333333333);
        hashTable$34 = new LongHashTable$95(memorySize$96);
        long memorySize$113 = computeMemorySize(0.3333333333333333);
        hashTable$37 = new LongHashTable$112(memorySize$113);
    }

    @Override
    public java.util.List getInputs() {
        return java.util.Arrays.asList(
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 1) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`s_store_sk` BIGINT NOT NULL>
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
                        // InputType: ROW<`hd_demo_sk` BIGINT NOT NULL>
                        org.apache.flink.table.data.RowData in2 =
                                (org.apache.flink.table.data.RowData) element.getValue();
                        processInput2(in2);
                    }
                },
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 3) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`t_time_sk` BIGINT NOT NULL>
                        org.apache.flink.table.data.RowData in3 =
                                (org.apache.flink.table.data.RowData) element.getValue();
                        processInput3(in3);
                    }
                },
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 4) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`ss_sold_time_sk` BIGINT, `ss_hdemo_sk` BIGINT,
                        // `ss_store_sk` BIGINT>
                        org.apache.flink.table.data.RowData in4 =
                                (org.apache.flink.table.data.RowData) element.getValue();
                        processInput4(in4);
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

            case 3:
                endInput3();
                break;

            case 4:
                endInput4();
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

        if (this.hashTable$37 != null) {
            this.hashTable$37.close();
            this.hashTable$37.free();
            this.hashTable$37 = null;
        }

        if (this.hashTable$34 != null) {
            this.hashTable$34.close();
            this.hashTable$34.free();
            this.hashTable$34 = null;
        }

        if (this.hashTable$31 != null) {
            this.hashTable$31.close();
            this.hashTable$31.free();
            this.hashTable$31 = null;
        }
    }

    public class Projection$105
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(3);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$105(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$106 = in1.isNullAt(0);
            long field$106 = isNull$106 ? -1L : (in1.getLong(0));
            if (isNull$106) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$106);
            }

            boolean isNull$107 = in1.isNullAt(1);
            long field$107 = isNull$107 ? -1L : (in1.getLong(1));
            if (isNull$107) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$107);
            }

            boolean isNull$108 = in1.isNullAt(2);
            long field$108 = isNull$108 ? -1L : (in1.getLong(2));
            if (isNull$108) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeLong(2, field$108);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$112
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$112(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$101,
                    probeSer$102,
                    getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    getContainingTask().getEnvironment().getIOManager(),
                    8,
                    1769L / getRuntimeContext().getNumberOfParallelSubtasks());
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
                return probeToBinaryRow$109.apply(row);
            }
        }
    }

    public class Projection$103
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$103(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$104 = in1.isNullAt(0);
            long field$104 = isNull$104 ? -1L : (in1.getLong(0));
            if (isNull$104) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$104);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$95
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$95(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$85,
                    probeSer$86,
                    getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    getContainingTask().getEnvironment().getIOManager(),
                    8,
                    720L / getRuntimeContext().getNumberOfParallelSubtasks());
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
                return probeToBinaryRow$92.apply(row);
            }
        }
    }

    public class Projection$87
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$87(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$88 = in1.isNullAt(0);
            long field$88 = isNull$88 ? -1L : (in1.getLong(0));
            if (isNull$88) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$88);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$89
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(2);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$89(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$90 = in1.isNullAt(0);
            long field$90 = isNull$90 ? -1L : (in1.getLong(0));
            if (isNull$90) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$90);
            }

            boolean isNull$91 = in1.isNullAt(1);
            long field$91 = isNull$91 ? -1L : (in1.getLong(1));
            if (isNull$91) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$91);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$72
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$72(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$73 = in1.isNullAt(0);
            long field$73 = isNull$73 ? -1L : (in1.getLong(0));
            if (isNull$73) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$73);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$79
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$79(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$70,
                    probeSer$71,
                    getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    getContainingTask().getEnvironment().getIOManager(),
                    8,
                    225L / getRuntimeContext().getNumberOfParallelSubtasks());
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
                return probeToBinaryRow$76.apply(row);
            }
        }
    }

    public class Projection$74
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$74(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$75 = in1.isNullAt(0);
            long field$75 = isNull$75 ? -1L : (in1.getLong(0));
            if (isNull$75) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$75);
            }

            outWriter.complete();

            return out;
        }
    }
}
