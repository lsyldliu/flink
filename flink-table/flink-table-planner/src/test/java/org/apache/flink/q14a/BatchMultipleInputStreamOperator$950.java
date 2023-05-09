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

public final class BatchMultipleInputStreamOperator$950
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

        boolean anyNull$892 = false;
        boolean isNull$891 = in1.isNullAt(0);
        long field$891 = isNull$891 ? -1L : (in1.getLong(0));

        anyNull$892 |= isNull$891;

        if (!anyNull$892) {

            hashTable$893.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in1);
        }
    }

    public void processInput3(org.apache.flink.table.data.RowData in3) throws Exception {

        // evaluate the required expr in advance

        boolean anyNull$895 = false;
        boolean isNull$894 = in3.isNullAt(0);
        long field$894 = isNull$894 ? -1L : (in3.getLong(0));

        anyNull$895 |= isNull$894;

        if (!anyNull$895) {

            hashTable$896.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in3);
        }
    }

    public void processInput2(org.apache.flink.table.data.RowData in2) throws Exception {

        // evaluate the required expr in advance

        // generate join key for probe side
        boolean isNull$900 = in2.isNullAt(3);
        long field$900 = isNull$900 ? -1L : (in2.getLong(3));
        // find matches from hash table
        org.apache.flink.table.runtime.util.RowIterator buildIter$903 =
                isNull$900 ? null : hashTable$896.get(field$900);
        if (buildIter$903 != null) {
            while (buildIter$903.advanceNext()) {
                org.apache.flink.table.data.RowData buildRow$901 = buildIter$903.getRow();
                {

                    // evaluate the required expr in advance

                    // evaluate the required expr in advance

                    // generate join key for probe side
                    boolean isNull$897 = in2.isNullAt(0);
                    long field$897 = isNull$897 ? -1L : (in2.getLong(0));
                    // find matches from hash table
                    org.apache.flink.table.runtime.util.RowIterator buildIter$908 =
                            isNull$897 ? null : hashTable$893.get(field$897);
                    if (buildIter$908 != null) {
                        while (buildIter$908.advanceNext()) {
                            {

                                // evaluate the required expr in advance

                                // wrap variable to row

                                out_inputRow$887.setLong(0, field$897);

                                boolean isNull$898 = in2.isNullAt(1);
                                int field$898 = isNull$898 ? -1 : (in2.getInt(1));
                                if (isNull$898) {
                                    out_inputRow$887.setNullAt(1);
                                } else {
                                    out_inputRow$887.setInt(1, field$898);
                                }

                                boolean isNull$899 = in2.isNullAt(2);
                                double field$899 = isNull$899 ? -1.0d : (in2.getDouble(2));
                                if (isNull$899) {
                                    out_inputRow$887.setNullAt(2);
                                } else {
                                    out_inputRow$887.setDouble(2, field$899);
                                }

                                output.collect(outElement.replace(out_inputRow$887));

                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    public void endInput1() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$893.endBuild();
    }

    public void endInput3() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$896.endBuild();
    }

    public void endInput2() throws Exception {}

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.BoxedWrapperRowData out_inputRow$887 =
            new org.apache.flink.table.data.BoxedWrapperRowData(3);
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$912;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$913;
    Projection$914 buildToBinaryRow$920;
    private transient java.lang.Object[] buildProjRefs$921;
    Projection$916 probeToBinaryRow$920;
    private transient java.lang.Object[] probeProjRefs$922;
    LongHashTable$923 hashTable$893;

    private void hj_init$926(Object[] references) throws Exception {
        buildSer$912 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$913 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$921 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$920 = new Projection$914(buildProjRefs$921);
        probeProjRefs$922 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$920 = new Projection$916(probeProjRefs$922);
    }

    private transient java.lang.Object[] hj_Refs$927;

    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$929;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$930;
    Projection$931 buildToBinaryRow$938;
    private transient java.lang.Object[] buildProjRefs$939;
    Projection$933 probeToBinaryRow$938;
    private transient java.lang.Object[] probeProjRefs$940;
    LongHashTable$941 hashTable$896;

    private void hj_init$944(Object[] references) throws Exception {
        buildSer$929 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$930 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$939 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$938 = new Projection$931(buildProjRefs$939);
        probeProjRefs$940 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$938 = new Projection$933(probeProjRefs$940);
    }

    private transient java.lang.Object[] hj_Refs$945;
    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$949;

    public BatchMultipleInputStreamOperator$950(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 3);
        this.parameters = parameters;
        hj_Refs$927 = (((java.lang.Object[]) references[0]));
        hj_init$926(hj_Refs$927);
        hj_Refs$945 = (((java.lang.Object[]) references[1]));
        hj_init$944(hj_Refs$945);
        inputSpecRefs$949 = (((java.util.ArrayList) references[2]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$949);
    }

    @Override
    public void open() throws Exception {
        super.open();

        long memorySize$924 = computeMemorySize(0.5);
        hashTable$893 = new LongHashTable$923(memorySize$924);
        long memorySize$942 = computeMemorySize(0.5);
        hashTable$896 = new LongHashTable$941(memorySize$942);
    }

    @Override
    public java.util.List getInputs() {
        return java.util.Arrays.asList(
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 1) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`ss_item_sk` BIGINT NOT NULL>
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
                        // InputType: ROW<`ws_item_sk` BIGINT NOT NULL, `ws_quantity` INT,
                        // `ws_list_price` DOUBLE, `ws_sold_date_sk` BIGINT>
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
                        // InputType: ROW<`d_date_sk` BIGINT NOT NULL>
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

            case 3:
                endInput3();
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

        if (this.hashTable$896 != null) {
            this.hashTable$896.close();
            this.hashTable$896.free();
            this.hashTable$896 = null;
        }

        if (this.hashTable$893 != null) {
            this.hashTable$893.close();
            this.hashTable$893.free();
            this.hashTable$893 = null;
        }
    }

    public class Projection$931
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$931(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$932 = in1.isNullAt(0);
            long field$932 = isNull$932 ? -1L : (in1.getLong(0));
            if (isNull$932) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$932);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$941
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$941(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$929,
                    probeSer$930,
                    getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    getContainingTask().getEnvironment().getIOManager(),
                    8,
                    30L / getRuntimeContext().getNumberOfParallelSubtasks());
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
                return probeToBinaryRow$938.apply(row);
            }
        }
    }

    public class Projection$933
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(4);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$933(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$934 = in1.isNullAt(0);
            long field$934 = isNull$934 ? -1L : (in1.getLong(0));
            if (isNull$934) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$934);
            }

            boolean isNull$935 = in1.isNullAt(1);
            int field$935 = isNull$935 ? -1 : (in1.getInt(1));
            if (isNull$935) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeInt(1, field$935);
            }

            boolean isNull$936 = in1.isNullAt(2);
            double field$936 = isNull$936 ? -1.0d : (in1.getDouble(2));
            if (isNull$936) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeDouble(2, field$936);
            }

            boolean isNull$937 = in1.isNullAt(3);
            long field$937 = isNull$937 ? -1L : (in1.getLong(3));
            if (isNull$937) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeLong(3, field$937);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$914
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$914(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$915 = in1.isNullAt(0);
            long field$915 = isNull$915 ? -1L : (in1.getLong(0));
            if (isNull$915) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$915);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$916
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(3);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$916(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$917 = in1.isNullAt(0);
            long field$917 = isNull$917 ? -1L : (in1.getLong(0));
            if (isNull$917) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$917);
            }

            boolean isNull$918 = in1.isNullAt(1);
            int field$918 = isNull$918 ? -1 : (in1.getInt(1));
            if (isNull$918) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeInt(1, field$918);
            }

            boolean isNull$919 = in1.isNullAt(2);
            double field$919 = isNull$919 ? -1.0d : (in1.getDouble(2));
            if (isNull$919) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeDouble(2, field$919);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$923
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$923(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$912,
                    probeSer$913,
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
                return probeToBinaryRow$920.apply(row);
            }
        }
    }
}
