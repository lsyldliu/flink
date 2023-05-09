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

public final class BatchMultipleInputStreamOperator$814
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

        boolean anyNull$760 = false;
        boolean isNull$752 = in2.isNullAt(0);
        long field$752 = isNull$752 ? -1L : (in2.getLong(0));

        anyNull$760 |= isNull$752;

        if (!anyNull$760) {

            hashTable$761.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in2);
        }
    }

    public void processInput1(org.apache.flink.table.data.RowData in1) throws Exception {

        // evaluate the required expr in advance

        // generate join key for probe side
        boolean isNull$762 = in1.isNullAt(0);
        long field$762 = isNull$762 ? -1L : (in1.getLong(0));
        // find matches from hash table
        org.apache.flink.table.runtime.util.RowIterator buildIter$776 =
                isNull$762 ? null : hashTable$761.get(field$762);
        if (buildIter$776 != null) {
            while (buildIter$776.advanceNext()) {
                org.apache.flink.table.data.RowData buildRow$767 = buildIter$776.getRow();
                {

                    // evaluate the required expr in advance

                    // evaluate the required expr in advance

                    // wrap variable to row

                    boolean isNull$769 = buildRow$767.isNullAt(1);
                    org.apache.flink.table.data.binary.BinaryStringData field$769 =
                            isNull$769
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$767.getString(1)));
                    out_inputRow$749.setNonPrimitiveValue(0, field$769);

                    boolean isNull$770 = buildRow$767.isNullAt(2);
                    org.apache.flink.table.data.binary.BinaryStringData field$770 =
                            isNull$770
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$767.getString(2)));
                    if (isNull$770) {
                        out_inputRow$749.setNullAt(1);
                    } else {
                        out_inputRow$749.setNonPrimitiveValue(1, field$770);
                    }

                    boolean isNull$771 = buildRow$767.isNullAt(3);
                    org.apache.flink.table.data.binary.BinaryStringData field$771 =
                            isNull$771
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$767.getString(3)));
                    if (isNull$771) {
                        out_inputRow$749.setNullAt(2);
                    } else {
                        out_inputRow$749.setNonPrimitiveValue(2, field$771);
                    }

                    boolean isNull$772 = buildRow$767.isNullAt(4);
                    org.apache.flink.table.data.binary.BinaryStringData field$772 =
                            isNull$772
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$767.getString(4)));
                    if (isNull$772) {
                        out_inputRow$749.setNullAt(3);
                    } else {
                        out_inputRow$749.setNonPrimitiveValue(3, field$772);
                    }

                    boolean isNull$773 = buildRow$767.isNullAt(5);
                    org.apache.flink.table.data.binary.BinaryStringData field$773 =
                            isNull$773
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$767.getString(5)));
                    if (isNull$773) {
                        out_inputRow$749.setNullAt(4);
                    } else {
                        out_inputRow$749.setNonPrimitiveValue(4, field$773);
                    }

                    boolean isNull$774 = buildRow$767.isNullAt(6);
                    org.apache.flink.table.data.binary.BinaryStringData field$774 =
                            isNull$774
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$767.getString(6)));
                    if (isNull$774) {
                        out_inputRow$749.setNullAt(5);
                    } else {
                        out_inputRow$749.setNonPrimitiveValue(5, field$774);
                    }

                    boolean isNull$775 = buildRow$767.isNullAt(7);
                    org.apache.flink.table.data.binary.BinaryStringData field$775 =
                            isNull$775
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$767.getString(7)));
                    if (isNull$775) {
                        out_inputRow$749.setNullAt(6);
                    } else {
                        out_inputRow$749.setNonPrimitiveValue(6, field$775);
                    }

                    // --- Cast section generated by
                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                    // --- End cast section

                    if (false) {
                        out_inputRow$749.setNullAt(7);
                    } else {
                        out_inputRow$749.setInt(7, ((int) 2002));
                    }

                    boolean isNull$766 = in1.isNullAt(4);
                    double field$766 = isNull$766 ? -1.0d : (in1.getDouble(4));
                    boolean isNull$765 = in1.isNullAt(3);
                    double field$765 = isNull$765 ? -1.0d : (in1.getDouble(3));
                    boolean isNull$778 = isNull$766 || isNull$765;
                    double result$778 = -1.0d;
                    if (!isNull$778) {

                        result$778 = (double) (field$766 - field$765);
                    }

                    boolean isNull$763 = in1.isNullAt(1);
                    double field$763 = isNull$763 ? -1.0d : (in1.getDouble(1));
                    boolean isNull$779 = isNull$778 || isNull$763;
                    double result$779 = -1.0d;
                    if (!isNull$779) {

                        result$779 = (double) (result$778 - field$763);
                    }

                    boolean isNull$764 = in1.isNullAt(2);
                    double field$764 = isNull$764 ? -1.0d : (in1.getDouble(2));
                    boolean isNull$780 = isNull$779 || isNull$764;
                    double result$780 = -1.0d;
                    if (!isNull$780) {

                        result$780 = (double) (result$779 + field$764);
                    }

                    boolean isNull$781 = isNull$780 || false;
                    double result$781 = -1.0d;
                    if (!isNull$781) {

                        result$781 = (double) (result$780 / ((double) (((int) 2))));
                    }

                    if (isNull$781) {
                        out_inputRow$749.setNullAt(8);
                    } else {
                        out_inputRow$749.setDouble(8, result$781);
                    }

                    output.collect(outElement.replace(out_inputRow$749));
                }
            }
        }
    }

    public void endInput2() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$761.endBuild();
    }

    public void endInput1() throws Exception {}

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.BoxedWrapperRowData out_inputRow$749 =
            new org.apache.flink.table.data.BoxedWrapperRowData(9);
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$786;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$787;
    Projection$788 buildToBinaryRow$803;
    private transient java.lang.Object[] buildProjRefs$804;
    Projection$797 probeToBinaryRow$803;
    private transient java.lang.Object[] probeProjRefs$805;
    LongHashTable$806 hashTable$761;

    private void hj_init$809(Object[] references) throws Exception {
        buildSer$786 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$787 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$804 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$803 = new Projection$788(buildProjRefs$804);
        probeProjRefs$805 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$803 = new Projection$797(probeProjRefs$805);
    }

    private transient java.lang.Object[] hj_Refs$810;

    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$813;

    public BatchMultipleInputStreamOperator$814(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        hj_Refs$810 = (((java.lang.Object[]) references[0]));
        hj_init$809(hj_Refs$810);
        inputSpecRefs$813 = (((java.util.ArrayList) references[1]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$813);
    }

    @Override
    public void open() throws Exception {
        super.open();

        long memorySize$807 = computeMemorySize(1.0);
        hashTable$761 = new LongHashTable$806(memorySize$807);
    }

    @Override
    public java.util.List getInputs() {
        return java.util.Arrays.asList(
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 1) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`cs_bill_customer_sk` BIGINT, `cs_ext_discount_amt`
                        // DOUBLE, `cs_ext_sales_price` DOUBLE, `cs_ext_wholesale_cost` DOUBLE,
                        // `cs_ext_list_price` DOUBLE>
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
                        // InputType: ROW<`c_customer_sk` BIGINT NOT NULL, `c_customer_id`
                        // VARCHAR(2147483647) NOT NULL, `c_first_name` VARCHAR(2147483647),
                        // `c_last_name` VARCHAR(2147483647), `c_preferred_cust_flag`
                        // VARCHAR(2147483647), `c_birth_country` VARCHAR(2147483647), `c_login`
                        // VARCHAR(2147483647), `c_email_address` VARCHAR(2147483647)>
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

        if (this.hashTable$761 != null) {
            this.hashTable$761.close();
            this.hashTable$761.free();
            this.hashTable$761 = null;
        }
    }

    public class Projection$797
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(5);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$797(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$798 = in1.isNullAt(0);
            long field$798 = isNull$798 ? -1L : (in1.getLong(0));
            if (isNull$798) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$798);
            }

            boolean isNull$799 = in1.isNullAt(1);
            double field$799 = isNull$799 ? -1.0d : (in1.getDouble(1));
            if (isNull$799) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeDouble(1, field$799);
            }

            boolean isNull$800 = in1.isNullAt(2);
            double field$800 = isNull$800 ? -1.0d : (in1.getDouble(2));
            if (isNull$800) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeDouble(2, field$800);
            }

            boolean isNull$801 = in1.isNullAt(3);
            double field$801 = isNull$801 ? -1.0d : (in1.getDouble(3));
            if (isNull$801) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeDouble(3, field$801);
            }

            boolean isNull$802 = in1.isNullAt(4);
            double field$802 = isNull$802 ? -1.0d : (in1.getDouble(4));
            if (isNull$802) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeDouble(4, field$802);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$806
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$806(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$786,
                    probeSer$787,
                    getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    getContainingTask().getEnvironment().getIOManager(),
                    71,
                    65000000L / getRuntimeContext().getNumberOfParallelSubtasks());
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
                return probeToBinaryRow$803.apply(row);
            }
        }
    }

    public class Projection$788
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(8);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$788(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$789 = in1.isNullAt(0);
            long field$789 = isNull$789 ? -1L : (in1.getLong(0));
            if (isNull$789) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$789);
            }

            boolean isNull$790 = in1.isNullAt(1);
            org.apache.flink.table.data.binary.BinaryStringData field$790 =
                    isNull$790
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(1)));
            if (isNull$790) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeString(1, field$790);
            }

            boolean isNull$791 = in1.isNullAt(2);
            org.apache.flink.table.data.binary.BinaryStringData field$791 =
                    isNull$791
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(2)));
            if (isNull$791) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeString(2, field$791);
            }

            boolean isNull$792 = in1.isNullAt(3);
            org.apache.flink.table.data.binary.BinaryStringData field$792 =
                    isNull$792
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(3)));
            if (isNull$792) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeString(3, field$792);
            }

            boolean isNull$793 = in1.isNullAt(4);
            org.apache.flink.table.data.binary.BinaryStringData field$793 =
                    isNull$793
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(4)));
            if (isNull$793) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeString(4, field$793);
            }

            boolean isNull$794 = in1.isNullAt(5);
            org.apache.flink.table.data.binary.BinaryStringData field$794 =
                    isNull$794
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(5)));
            if (isNull$794) {
                outWriter.setNullAt(5);
            } else {
                outWriter.writeString(5, field$794);
            }

            boolean isNull$795 = in1.isNullAt(6);
            org.apache.flink.table.data.binary.BinaryStringData field$795 =
                    isNull$795
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(6)));
            if (isNull$795) {
                outWriter.setNullAt(6);
            } else {
                outWriter.writeString(6, field$795);
            }

            boolean isNull$796 = in1.isNullAt(7);
            org.apache.flink.table.data.binary.BinaryStringData field$796 =
                    isNull$796
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(7)));
            if (isNull$796) {
                outWriter.setNullAt(7);
            } else {
                outWriter.writeString(7, field$796);
            }

            outWriter.complete();

            return out;
        }
    }
}
