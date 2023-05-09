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

public final class BatchMultipleInputStreamOperator$120
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

        // evaluate the required expr in advance

        boolean isNull$38 = in4.isNullAt(0);
        long field$38 = isNull$38 ? -1L : (in4.getLong(0));
        boolean result$41 = !isNull$38;
        if (result$41) {

            // evaluate the required expr in advance

            // generate join key for probe side

            // find matches from hash table
            org.apache.flink.table.runtime.util.RowIterator buildIter$45 =
                    isNull$38 ? null : hashTable$37.get(field$38);
            if (buildIter$45 != null) {
                while (buildIter$45.advanceNext()) {
                    org.apache.flink.table.data.RowData buildRow$43 = buildIter$45.getRow();
                    {

                        // evaluate the required expr in advance

                        // evaluate the required expr in advance

                        // generate join key for probe side
                        boolean isNull$39 = in4.isNullAt(1);
                        long field$39 = isNull$39 ? -1L : (in4.getLong(1));
                        // find matches from hash table
                        org.apache.flink.table.runtime.util.RowIterator buildIter$50 =
                                isNull$39 ? null : hashTable$34.get(field$39);
                        if (buildIter$50 != null) {
                            while (buildIter$50.advanceNext()) {
                                org.apache.flink.table.data.RowData buildRow$48 =
                                        buildIter$50.getRow();
                                {

                                    // evaluate the required expr in advance

                                    // evaluate the required expr in advance

                                    // generate join key for probe side
                                    boolean isNull$40 = in4.isNullAt(2);
                                    long field$40 = isNull$40 ? -1L : (in4.getLong(2));
                                    // find matches from hash table
                                    org.apache.flink.table.runtime.util.RowIterator buildIter$55 =
                                            isNull$40 ? null : hashTable$31.get(field$40);
                                    if (buildIter$55 != null) {
                                        while (buildIter$55.advanceNext()) {
                                            org.apache.flink.table.data.RowData buildRow$53 =
                                                    buildIter$55.getRow();
                                            {

                                                // evaluate the required expr in advance

                                                // evaluate the required expr in advance

                                                if (!hasInput$60) {
                                                    hasInput$60 = true;
                                                    // init agg buffer
                                                    localagg$58_agg0_count1IsNull = false;
                                                    localagg$58_agg0_count1 = ((long) 0L);
                                                }
                                                // update agg buffer to do aggregate

                                                boolean isNull$59 =
                                                        localagg$58_agg0_count1IsNull || false;
                                                long result$59 = -1L;
                                                if (!isNull$59) {

                                                    result$59 =
                                                            (long)
                                                                    (localagg$58_agg0_count1
                                                                            + ((long) 1L));
                                                }

                                                localagg$58_agg0_count1IsNull = isNull$59;
                                                if (!isNull$59) {
                                                    // copy result term
                                                    localagg$58_agg0_count1 = result$59;
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

        localagg_withoutKeyEndInput$62();
        // call downstream endInput

    }

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    long localagg$58_agg0_count1;
    boolean localagg$58_agg0_count1IsNull;
    private boolean hasInput$60 = false;
    org.apache.flink.table.data.binary.BinaryRowData valueRow$61 =
            new org.apache.flink.table.data.binary.BinaryRowData(1);
    org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
            new org.apache.flink.table.data.writer.BinaryRowWriter(valueRow$61);

    private void localagg_withoutKeyEndInput$62() throws Exception {
        if (hasInput$60) {

            // wrap variable to row

            outWriter.reset();

            if (localagg$58_agg0_count1IsNull) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, localagg$58_agg0_count1);
            }

            outWriter.complete();

            // evaluate the required expr in advance

            output.collect(outElement.replace(valueRow$61));
        }
    }

    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$67;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$68;
    Projection$69 buildToBinaryRow$73;
    private transient java.lang.Object[] buildProjRefs$74;
    Projection$71 probeToBinaryRow$73;
    private transient java.lang.Object[] probeProjRefs$75;
    LongHashTable$76 hashTable$31;

    private void hj_init$79(Object[] references) throws Exception {
        buildSer$67 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$68 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$74 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$73 = new Projection$69(buildProjRefs$74);
        probeProjRefs$75 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$73 = new Projection$71(probeProjRefs$75);
    }

    private transient java.lang.Object[] hj_Refs$80;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$82;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$83;
    Projection$84 buildToBinaryRow$89;
    private transient java.lang.Object[] buildProjRefs$90;
    Projection$86 probeToBinaryRow$89;
    private transient java.lang.Object[] probeProjRefs$91;
    LongHashTable$92 hashTable$34;

    private void hj_init$95(Object[] references) throws Exception {
        buildSer$82 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$83 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$90 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$89 = new Projection$84(buildProjRefs$90);
        probeProjRefs$91 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$89 = new Projection$86(probeProjRefs$91);
    }

    private transient java.lang.Object[] hj_Refs$96;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$98;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$99;
    Projection$100 buildToBinaryRow$106;
    private transient java.lang.Object[] buildProjRefs$107;
    Projection$102 probeToBinaryRow$106;
    private transient java.lang.Object[] probeProjRefs$108;
    LongHashTable$109 hashTable$37;

    private void hj_init$112(Object[] references) throws Exception {
        buildSer$98 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$99 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$107 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$106 = new Projection$100(buildProjRefs$107);
        probeProjRefs$108 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$106 = new Projection$102(probeProjRefs$108);
    }

    private transient java.lang.Object[] hj_Refs$113;
    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$119;

    public BatchMultipleInputStreamOperator$120(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 4);
        this.parameters = parameters;
        hj_Refs$80 = (((java.lang.Object[]) references[0]));
        hj_init$79(hj_Refs$80);
        hj_Refs$96 = (((java.lang.Object[]) references[1]));
        hj_init$95(hj_Refs$96);
        hj_Refs$113 = (((java.lang.Object[]) references[2]));
        hj_init$112(hj_Refs$113);
        inputSpecRefs$119 = (((java.util.ArrayList) references[3]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$119);
    }

    @Override
    public void open() throws Exception {
        super.open();

        long memorySize$77 = computeMemorySize(0.3333333333333333);
        hashTable$31 = new LongHashTable$76(memorySize$77);
        long memorySize$93 = computeMemorySize(0.3333333333333333);
        hashTable$34 = new LongHashTable$92(memorySize$93);
        long memorySize$110 = computeMemorySize(0.3333333333333333);
        hashTable$37 = new LongHashTable$109(memorySize$110);
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

    public class LongHashTable$109
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$109(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$98,
                    probeSer$99,
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
                return probeToBinaryRow$106.apply(row);
            }
        }
    }

    public class Projection$100
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$100(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$101 = in1.isNullAt(0);
            long field$101 = isNull$101 ? -1L : (in1.getLong(0));
            if (isNull$101) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$101);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$102
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(3);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$102(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$103 = in1.isNullAt(0);
            long field$103 = isNull$103 ? -1L : (in1.getLong(0));
            if (isNull$103) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$103);
            }

            boolean isNull$104 = in1.isNullAt(1);
            long field$104 = isNull$104 ? -1L : (in1.getLong(1));
            if (isNull$104) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$104);
            }

            boolean isNull$105 = in1.isNullAt(2);
            long field$105 = isNull$105 ? -1L : (in1.getLong(2));
            if (isNull$105) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeLong(2, field$105);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$92
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$92(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$82,
                    probeSer$83,
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
                return probeToBinaryRow$89.apply(row);
            }
        }
    }

    public class Projection$84
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$84(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$85 = in1.isNullAt(0);
            long field$85 = isNull$85 ? -1L : (in1.getLong(0));
            if (isNull$85) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$85);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$86
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(2);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$86(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$87 = in1.isNullAt(0);
            long field$87 = isNull$87 ? -1L : (in1.getLong(0));
            if (isNull$87) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$87);
            }

            boolean isNull$88 = in1.isNullAt(1);
            long field$88 = isNull$88 ? -1L : (in1.getLong(1));
            if (isNull$88) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$88);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$69
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$69(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$70 = in1.isNullAt(0);
            long field$70 = isNull$70 ? -1L : (in1.getLong(0));
            if (isNull$70) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$70);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$71
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$71(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$72 = in1.isNullAt(0);
            long field$72 = isNull$72 ? -1L : (in1.getLong(0));
            if (isNull$72) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$72);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$76
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$76(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$67,
                    probeSer$68,
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
                return probeToBinaryRow$73.apply(row);
            }
        }
    }
}
