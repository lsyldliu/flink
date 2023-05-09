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

package org.apache.flink.q4;

public final class BatchMultipleInputStreamOperator$743
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

        boolean anyNull$702 = false;
        boolean isNull$701 = in1.isNullAt(0);
        long field$701 = isNull$701 ? -1L : (in1.getLong(0));

        anyNull$702 |= isNull$701;

        if (!anyNull$702) {

            hashTable$703.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in1);
        }
    }

    public void processInput2(org.apache.flink.table.data.RowData in2) throws Exception {

        // evaluate the required expr in advance

        boolean isNull$709 = in2.isNullAt(5);
        long field$709 = isNull$709 ? -1L : (in2.getLong(5));
        boolean result$710 = !isNull$709;
        if (result$710) {

            // evaluate the required expr in advance

            // generate join key for probe side

            // find matches from hash table
            org.apache.flink.table.runtime.util.RowIterator buildIter$714 =
                    isNull$709 ? null : hashTable$703.get(field$709);
            if (buildIter$714 != null) {
                while (buildIter$714.advanceNext()) {
                    org.apache.flink.table.data.RowData buildRow$712 = buildIter$714.getRow();
                    {

                        // evaluate the required expr in advance

                        // evaluate the required expr in advance

                        // wrap variable to row

                        boolean isNull$704 = in2.isNullAt(0);
                        long field$704 = isNull$704 ? -1L : (in2.getLong(0));
                        if (isNull$704) {
                            out_inputRow$697.setNullAt(0);
                        } else {
                            out_inputRow$697.setLong(0, field$704);
                        }

                        boolean isNull$705 = in2.isNullAt(1);
                        double field$705 = isNull$705 ? -1.0d : (in2.getDouble(1));
                        if (isNull$705) {
                            out_inputRow$697.setNullAt(1);
                        } else {
                            out_inputRow$697.setDouble(1, field$705);
                        }

                        boolean isNull$706 = in2.isNullAt(2);
                        double field$706 = isNull$706 ? -1.0d : (in2.getDouble(2));
                        if (isNull$706) {
                            out_inputRow$697.setNullAt(2);
                        } else {
                            out_inputRow$697.setDouble(2, field$706);
                        }

                        boolean isNull$707 = in2.isNullAt(3);
                        double field$707 = isNull$707 ? -1.0d : (in2.getDouble(3));
                        if (isNull$707) {
                            out_inputRow$697.setNullAt(3);
                        } else {
                            out_inputRow$697.setDouble(3, field$707);
                        }

                        boolean isNull$708 = in2.isNullAt(4);
                        double field$708 = isNull$708 ? -1.0d : (in2.getDouble(4));
                        if (isNull$708) {
                            out_inputRow$697.setNullAt(4);
                        } else {
                            out_inputRow$697.setDouble(4, field$708);
                        }

                        output.collect(outElement.replace(out_inputRow$697));
                    }
                }
            }
        }
    }

    public void endInput1() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$703.endBuild();
    }

    public void endInput2() throws Exception {}

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.BoxedWrapperRowData out_inputRow$697 =
            new org.apache.flink.table.data.BoxedWrapperRowData(5);
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$720;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$721;
    Projection$722 buildToBinaryRow$731;
    private transient java.lang.Object[] buildProjRefs$732;
    Projection$724 probeToBinaryRow$731;
    private transient java.lang.Object[] probeProjRefs$733;
    LongHashTable$734 hashTable$703;

    private void hj_init$737(Object[] references) throws Exception {
        buildSer$720 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$721 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$732 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$731 = new Projection$722(buildProjRefs$732);
        probeProjRefs$733 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$731 = new Projection$724(probeProjRefs$733);
    }

    private transient java.lang.Object[] hj_Refs$738;

    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$742;

    public BatchMultipleInputStreamOperator$743(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        hj_Refs$738 = (((java.lang.Object[]) references[0]));
        hj_init$737(hj_Refs$738);
        inputSpecRefs$742 = (((java.util.ArrayList) references[1]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$742);
    }

    @Override
    public void open() throws Exception {
        super.open();

        long memorySize$735 = computeMemorySize(1.0);
        hashTable$703 = new LongHashTable$734(memorySize$735);
    }

    @Override
    public java.util.List getInputs() {
        return java.util.Arrays.asList(
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 1) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`d_date_sk` BIGINT NOT NULL>
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
                        // InputType: ROW<`cs_bill_customer_sk` BIGINT, `cs_ext_discount_amt`
                        // DOUBLE, `cs_ext_sales_price` DOUBLE, `cs_ext_wholesale_cost` DOUBLE,
                        // `cs_ext_list_price` DOUBLE, `cs_sold_date_sk` BIGINT>
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

        if (this.hashTable$703 != null) {
            this.hashTable$703.close();
            this.hashTable$703.free();
            this.hashTable$703 = null;
        }
    }

    public class Projection$722
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$722(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$723 = in1.isNullAt(0);
            long field$723 = isNull$723 ? -1L : (in1.getLong(0));
            if (isNull$723) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$723);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$724
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(6);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$724(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$725 = in1.isNullAt(0);
            long field$725 = isNull$725 ? -1L : (in1.getLong(0));
            if (isNull$725) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$725);
            }

            boolean isNull$726 = in1.isNullAt(1);
            double field$726 = isNull$726 ? -1.0d : (in1.getDouble(1));
            if (isNull$726) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeDouble(1, field$726);
            }

            boolean isNull$727 = in1.isNullAt(2);
            double field$727 = isNull$727 ? -1.0d : (in1.getDouble(2));
            if (isNull$727) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeDouble(2, field$727);
            }

            boolean isNull$728 = in1.isNullAt(3);
            double field$728 = isNull$728 ? -1.0d : (in1.getDouble(3));
            if (isNull$728) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeDouble(3, field$728);
            }

            boolean isNull$729 = in1.isNullAt(4);
            double field$729 = isNull$729 ? -1.0d : (in1.getDouble(4));
            if (isNull$729) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeDouble(4, field$729);
            }

            boolean isNull$730 = in1.isNullAt(5);
            long field$730 = isNull$730 ? -1L : (in1.getLong(5));
            if (isNull$730) {
                outWriter.setNullAt(5);
            } else {
                outWriter.writeLong(5, field$730);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$734
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$734(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$720,
                    probeSer$721,
                    getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    getContainingTask().getEnvironment().getIOManager(),
                    8,
                    367L / getRuntimeContext().getNumberOfParallelSubtasks());
        }

        @Override
        public long getBuildLongKey(org.apache.flink.table.data.RowData row) {
            return row.getLong(0);
        }

        @Override
        public long getProbeLongKey(org.apache.flink.table.data.RowData row) {
            return row.getLong(5);
        }

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData probeToBinary(
                org.apache.flink.table.data.RowData row) {
            if (row instanceof org.apache.flink.table.data.binary.BinaryRowData) {
                return (org.apache.flink.table.data.binary.BinaryRowData) row;
            } else {
                return probeToBinaryRow$731.apply(row);
            }
        }
    }
}
