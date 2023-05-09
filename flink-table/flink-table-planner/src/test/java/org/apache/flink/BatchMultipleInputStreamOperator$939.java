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

package org.apache.flink;

/** q14b left semi join */
public final class BatchMultipleInputStreamOperator$939
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

        boolean anyNull$875 = false;
        boolean isNull$873 = in1.isNullAt(0);
        long field$873 = isNull$873 ? -1L : (in1.getLong(0));

        anyNull$875 |= isNull$873;

        if (!anyNull$875) {

            hashTable$876.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in1);
        }
    }

    public void processInput2(org.apache.flink.table.data.RowData in2) throws Exception {

        // evaluate the required expr in advance

        boolean anyNull$878 = false;
        boolean isNull$877 = in2.isNullAt(0);
        long field$877 = isNull$877 ? -1L : (in2.getLong(0));

        anyNull$878 |= isNull$877;

        if (!anyNull$878) {

            hashTable$879.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in2);
        }
    }

    public void processInput3(org.apache.flink.table.data.RowData in3) throws Exception {

        // evaluate the required expr in advance

        boolean isNull$883 = in3.isNullAt(3);
        long field$883 = isNull$883 ? -1L : (in3.getLong(3));
        boolean result$884 = !isNull$883;
        if (result$884) {

            // evaluate the required expr in advance

            // generate join key for probe side
            boolean isNull$880 = in3.isNullAt(0);
            long field$880 = isNull$880 ? -1L : (in3.getLong(0));
            // find matches from hash table
            org.apache.flink.table.runtime.util.RowIterator buildIter$888 =
                    isNull$880 ? null : hashTable$879.get(field$880);
            if (buildIter$888 != null) {
                while (buildIter$888.advanceNext()) {
                    {

                        // evaluate the required expr in advance

                        // generate join key for probe side

                        // find matches from hash table
                        org.apache.flink.table.runtime.util.RowIterator buildIter$893 =
                                isNull$883 ? null : hashTable$876.get(field$883);
                        if (buildIter$893 != null) {
                            while (buildIter$893.advanceNext()) {
                                org.apache.flink.table.data.RowData buildRow$890 =
                                        buildIter$893.getRow();
                                {

                                    // evaluate the required expr in advance

                                    // evaluate the required expr in advance

                                    // wrap variable to row

                                    out_inputRow$868.setLong(0, field$880);

                                    boolean isNull$881 = in3.isNullAt(1);
                                    int field$881 = isNull$881 ? -1 : (in3.getInt(1));
                                    if (isNull$881) {
                                        out_inputRow$868.setNullAt(1);
                                    } else {
                                        out_inputRow$868.setInt(1, field$881);
                                    }

                                    boolean isNull$882 = in3.isNullAt(2);
                                    double field$882 = isNull$882 ? -1.0d : (in3.getDouble(2));
                                    if (isNull$882) {
                                        out_inputRow$868.setNullAt(2);
                                    } else {
                                        out_inputRow$868.setDouble(2, field$882);
                                    }

                                    boolean isNull$892 = buildRow$890.isNullAt(1);
                                    int field$892 = isNull$892 ? -1 : (buildRow$890.getInt(1));
                                    if (isNull$892) {
                                        out_inputRow$868.setNullAt(3);
                                    } else {
                                        out_inputRow$868.setInt(3, field$892);
                                    }

                                    output.collect(outElement.replace(out_inputRow$868));
                                }
                            }
                        }

                        break;
                    }
                }
            }
        }
    }

    public void endInput1() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$876.endBuild();
    }

    public void endInput2() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$879.endBuild();
    }

    public void endInput3() throws Exception {}

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.BoxedWrapperRowData out_inputRow$868 =
            new org.apache.flink.table.data.BoxedWrapperRowData(4);
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$899;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$900;
    Projection$901 buildToBinaryRow$909;
    private transient java.lang.Object[] buildProjRefs$910;
    Projection$904 probeToBinaryRow$909;
    private transient java.lang.Object[] probeProjRefs$911;
    LongHashTable$912 hashTable$876;

    private void hj_init$915(Object[] references) throws Exception {
        buildSer$899 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$900 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$910 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$909 = new Projection$901(buildProjRefs$910);
        probeProjRefs$911 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$909 = new Projection$904(probeProjRefs$911);
    }

    private transient java.lang.Object[] hj_Refs$916;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$917;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$918;
    Projection$919 buildToBinaryRow$926;
    private transient java.lang.Object[] buildProjRefs$927;
    Projection$921 probeToBinaryRow$926;
    private transient java.lang.Object[] probeProjRefs$928;
    LongHashTable$929 hashTable$879;

    private void hj_init$932(Object[] references) throws Exception {
        buildSer$917 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$918 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$927 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$926 = new Projection$919(buildProjRefs$927);
        probeProjRefs$928 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$926 = new Projection$921(probeProjRefs$928);
    }

    private transient java.lang.Object[] hj_Refs$933;

    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$938;

    public BatchMultipleInputStreamOperator$939(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 3);
        this.parameters = parameters;
        hj_Refs$916 = (((java.lang.Object[]) references[0]));
        hj_init$915(hj_Refs$916);
        hj_Refs$933 = (((java.lang.Object[]) references[1]));
        hj_init$932(hj_Refs$933);
        inputSpecRefs$938 = (((java.util.ArrayList) references[2]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$938);
    }

    @Override
    public void open() throws Exception {
        super.open();

        long memorySize$913 = computeMemorySize(0.5);
        hashTable$876 = new LongHashTable$912(memorySize$913);
        long memorySize$930 = computeMemorySize(0.5);
        hashTable$879 = new LongHashTable$929(memorySize$930);
    }

    @Override
    public java.util.List getInputs() {
        return java.util.Arrays.asList(
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 1) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`d_date_sk` BIGINT NOT NULL, `d_week_seq` INT>
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
                        // InputType: ROW<`ss_item_sk` BIGINT NOT NULL>
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
                        // InputType: ROW<`ss_item_sk` BIGINT NOT NULL, `ss_quantity` INT,
                        // `ss_list_price` DOUBLE, `ss_sold_date_sk` BIGINT>
                        org.apache.flink.table.data.RowData in3 =
                                (org.apache.flink.table.data.RowData) element.getValue();
                        processInput3(in3);
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

        if (this.hashTable$879 != null) {
            this.hashTable$879.close();
            this.hashTable$879.free();
            this.hashTable$879 = null;
        }

        if (this.hashTable$876 != null) {
            this.hashTable$876.close();
            this.hashTable$876.free();
            this.hashTable$876 = null;
        }
    }

    public class LongHashTable$912
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$912(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$899,
                    probeSer$900,
                    getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    getContainingTask().getEnvironment().getIOManager(),
                    16,
                    73049L / getRuntimeContext().getNumberOfParallelSubtasks());
        }

        @Override
        public long getBuildLongKey(org.apache.flink.table.data.RowData row) {
            return row.getLong(0);
        }

        @Override
        public long getProbeLongKey(org.apache.flink.table.data.RowData row) {
            return row.getLong(3);
        }

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData probeToBinary(
                org.apache.flink.table.data.RowData row) {
            if (row instanceof org.apache.flink.table.data.binary.BinaryRowData) {
                return (org.apache.flink.table.data.binary.BinaryRowData) row;
            } else {
                return probeToBinaryRow$909.apply(row);
            }
        }
    }

    public class Projection$901
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(2);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$901(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$902 = in1.isNullAt(0);
            long field$902 = isNull$902 ? -1L : (in1.getLong(0));
            if (isNull$902) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$902);
            }

            boolean isNull$903 = in1.isNullAt(1);
            int field$903 = isNull$903 ? -1 : (in1.getInt(1));
            if (isNull$903) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeInt(1, field$903);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$904
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(4);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$904(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$905 = in1.isNullAt(0);
            long field$905 = isNull$905 ? -1L : (in1.getLong(0));
            if (isNull$905) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$905);
            }

            boolean isNull$906 = in1.isNullAt(1);
            int field$906 = isNull$906 ? -1 : (in1.getInt(1));
            if (isNull$906) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeInt(1, field$906);
            }

            boolean isNull$907 = in1.isNullAt(2);
            double field$907 = isNull$907 ? -1.0d : (in1.getDouble(2));
            if (isNull$907) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeDouble(2, field$907);
            }

            boolean isNull$908 = in1.isNullAt(3);
            long field$908 = isNull$908 ? -1L : (in1.getLong(3));
            if (isNull$908) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeLong(3, field$908);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$919
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$919(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$920 = in1.isNullAt(0);
            long field$920 = isNull$920 ? -1L : (in1.getLong(0));
            if (isNull$920) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$920);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$929
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$929(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$917,
                    probeSer$918,
                    getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    getContainingTask().getEnvironment().getIOManager(),
                    8,
                    242770L / getRuntimeContext().getNumberOfParallelSubtasks());
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
                return probeToBinaryRow$926.apply(row);
            }
        }
    }

    public class Projection$921
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(4);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$921(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$922 = in1.isNullAt(0);
            long field$922 = isNull$922 ? -1L : (in1.getLong(0));
            if (isNull$922) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$922);
            }

            boolean isNull$923 = in1.isNullAt(1);
            int field$923 = isNull$923 ? -1 : (in1.getInt(1));
            if (isNull$923) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeInt(1, field$923);
            }

            boolean isNull$924 = in1.isNullAt(2);
            double field$924 = isNull$924 ? -1.0d : (in1.getDouble(2));
            if (isNull$924) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeDouble(2, field$924);
            }

            boolean isNull$925 = in1.isNullAt(3);
            long field$925 = isNull$925 ? -1L : (in1.getLong(3));
            if (isNull$925) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeLong(3, field$925);
            }

            outWriter.complete();

            return out;
        }
    }
}
