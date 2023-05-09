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

public final class BatchMultipleInputStreamOperator$587
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

        boolean anyNull$533 = false;
        boolean isNull$525 = in2.isNullAt(0);
        long field$525 = isNull$525 ? -1L : (in2.getLong(0));

        anyNull$533 |= isNull$525;

        if (!anyNull$533) {

            hashTable$534.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in2);
        }
    }

    public void processInput1(org.apache.flink.table.data.RowData in1) throws Exception {

        // evaluate the required expr in advance

        // generate join key for probe side
        boolean isNull$535 = in1.isNullAt(0);
        long field$535 = isNull$535 ? -1L : (in1.getLong(0));
        // find matches from hash table
        org.apache.flink.table.runtime.util.RowIterator buildIter$549 =
                isNull$535 ? null : hashTable$534.get(field$535);
        if (buildIter$549 != null) {
            while (buildIter$549.advanceNext()) {
                org.apache.flink.table.data.RowData buildRow$540 = buildIter$549.getRow();
                {

                    // evaluate the required expr in advance

                    // evaluate the required expr in advance

                    // wrap variable to row

                    boolean isNull$542 = buildRow$540.isNullAt(1);
                    org.apache.flink.table.data.binary.BinaryStringData field$542 =
                            isNull$542
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$540.getString(1)));
                    out_inputRow$522.setNonPrimitiveValue(0, field$542);

                    boolean isNull$543 = buildRow$540.isNullAt(2);
                    org.apache.flink.table.data.binary.BinaryStringData field$543 =
                            isNull$543
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$540.getString(2)));
                    if (isNull$543) {
                        out_inputRow$522.setNullAt(1);
                    } else {
                        out_inputRow$522.setNonPrimitiveValue(1, field$543);
                    }

                    boolean isNull$544 = buildRow$540.isNullAt(3);
                    org.apache.flink.table.data.binary.BinaryStringData field$544 =
                            isNull$544
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$540.getString(3)));
                    if (isNull$544) {
                        out_inputRow$522.setNullAt(2);
                    } else {
                        out_inputRow$522.setNonPrimitiveValue(2, field$544);
                    }

                    boolean isNull$545 = buildRow$540.isNullAt(4);
                    org.apache.flink.table.data.binary.BinaryStringData field$545 =
                            isNull$545
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$540.getString(4)));
                    if (isNull$545) {
                        out_inputRow$522.setNullAt(3);
                    } else {
                        out_inputRow$522.setNonPrimitiveValue(3, field$545);
                    }

                    boolean isNull$546 = buildRow$540.isNullAt(5);
                    org.apache.flink.table.data.binary.BinaryStringData field$546 =
                            isNull$546
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$540.getString(5)));
                    if (isNull$546) {
                        out_inputRow$522.setNullAt(4);
                    } else {
                        out_inputRow$522.setNonPrimitiveValue(4, field$546);
                    }

                    boolean isNull$547 = buildRow$540.isNullAt(6);
                    org.apache.flink.table.data.binary.BinaryStringData field$547 =
                            isNull$547
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$540.getString(6)));
                    if (isNull$547) {
                        out_inputRow$522.setNullAt(5);
                    } else {
                        out_inputRow$522.setNonPrimitiveValue(5, field$547);
                    }

                    boolean isNull$548 = buildRow$540.isNullAt(7);
                    org.apache.flink.table.data.binary.BinaryStringData field$548 =
                            isNull$548
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$540.getString(7)));
                    if (isNull$548) {
                        out_inputRow$522.setNullAt(6);
                    } else {
                        out_inputRow$522.setNonPrimitiveValue(6, field$548);
                    }

                    // --- Cast section generated by
                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                    // --- End cast section

                    if (false) {
                        out_inputRow$522.setNullAt(7);
                    } else {
                        out_inputRow$522.setInt(7, ((int) 2002));
                    }

                    boolean isNull$539 = in1.isNullAt(4);
                    double field$539 = isNull$539 ? -1.0d : (in1.getDouble(4));
                    boolean isNull$538 = in1.isNullAt(3);
                    double field$538 = isNull$538 ? -1.0d : (in1.getDouble(3));
                    boolean isNull$551 = isNull$539 || isNull$538;
                    double result$551 = -1.0d;
                    if (!isNull$551) {

                        result$551 = (double) (field$539 - field$538);
                    }

                    boolean isNull$536 = in1.isNullAt(1);
                    double field$536 = isNull$536 ? -1.0d : (in1.getDouble(1));
                    boolean isNull$552 = isNull$551 || isNull$536;
                    double result$552 = -1.0d;
                    if (!isNull$552) {

                        result$552 = (double) (result$551 - field$536);
                    }

                    boolean isNull$537 = in1.isNullAt(2);
                    double field$537 = isNull$537 ? -1.0d : (in1.getDouble(2));
                    boolean isNull$553 = isNull$552 || isNull$537;
                    double result$553 = -1.0d;
                    if (!isNull$553) {

                        result$553 = (double) (result$552 + field$537);
                    }

                    boolean isNull$554 = isNull$553 || false;
                    double result$554 = -1.0d;
                    if (!isNull$554) {

                        result$554 = (double) (result$553 / ((double) (((int) 2))));
                    }

                    if (isNull$554) {
                        out_inputRow$522.setNullAt(8);
                    } else {
                        out_inputRow$522.setDouble(8, result$554);
                    }

                    output.collect(outElement.replace(out_inputRow$522));
                }
            }
        }
    }

    public void endInput2() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$534.endBuild();
    }

    public void endInput1() throws Exception {}

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.BoxedWrapperRowData out_inputRow$522 =
            new org.apache.flink.table.data.BoxedWrapperRowData(9);
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$559;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$560;
    Projection$561 buildToBinaryRow$576;
    private transient java.lang.Object[] buildProjRefs$577;
    Projection$570 probeToBinaryRow$576;
    private transient java.lang.Object[] probeProjRefs$578;
    LongHashTable$579 hashTable$534;

    private void hj_init$582(Object[] references) throws Exception {
        buildSer$559 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$560 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$577 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$576 = new Projection$561(buildProjRefs$577);
        probeProjRefs$578 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$576 = new Projection$570(probeProjRefs$578);
    }

    private transient java.lang.Object[] hj_Refs$583;

    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$586;

    public BatchMultipleInputStreamOperator$587(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        hj_Refs$583 = (((java.lang.Object[]) references[0]));
        hj_init$582(hj_Refs$583);
        inputSpecRefs$586 = (((java.util.ArrayList) references[1]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$586);
    }

    @Override
    public void open() throws Exception {
        super.open();

        long memorySize$580 = computeMemorySize(1.0);
        hashTable$534 = new LongHashTable$579(memorySize$580);
    }

    @Override
    public java.util.List getInputs() {
        return java.util.Arrays.asList(
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 1) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`ss_customer_sk` BIGINT, `ss_ext_discount_amt` DOUBLE,
                        // `ss_ext_sales_price` DOUBLE, `ss_ext_wholesale_cost` DOUBLE,
                        // `ss_ext_list_price` DOUBLE>
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

        if (this.hashTable$534 != null) {
            this.hashTable$534.close();
            this.hashTable$534.free();
            this.hashTable$534 = null;
        }
    }

    public class LongHashTable$579
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$579(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$559,
                    probeSer$560,
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
                return probeToBinaryRow$576.apply(row);
            }
        }
    }

    public class Projection$570
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(5);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$570(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$571 = in1.isNullAt(0);
            long field$571 = isNull$571 ? -1L : (in1.getLong(0));
            if (isNull$571) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$571);
            }

            boolean isNull$572 = in1.isNullAt(1);
            double field$572 = isNull$572 ? -1.0d : (in1.getDouble(1));
            if (isNull$572) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeDouble(1, field$572);
            }

            boolean isNull$573 = in1.isNullAt(2);
            double field$573 = isNull$573 ? -1.0d : (in1.getDouble(2));
            if (isNull$573) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeDouble(2, field$573);
            }

            boolean isNull$574 = in1.isNullAt(3);
            double field$574 = isNull$574 ? -1.0d : (in1.getDouble(3));
            if (isNull$574) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeDouble(3, field$574);
            }

            boolean isNull$575 = in1.isNullAt(4);
            double field$575 = isNull$575 ? -1.0d : (in1.getDouble(4));
            if (isNull$575) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeDouble(4, field$575);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$561
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(8);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$561(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$562 = in1.isNullAt(0);
            long field$562 = isNull$562 ? -1L : (in1.getLong(0));
            if (isNull$562) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$562);
            }

            boolean isNull$563 = in1.isNullAt(1);
            org.apache.flink.table.data.binary.BinaryStringData field$563 =
                    isNull$563
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(1)));
            if (isNull$563) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeString(1, field$563);
            }

            boolean isNull$564 = in1.isNullAt(2);
            org.apache.flink.table.data.binary.BinaryStringData field$564 =
                    isNull$564
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(2)));
            if (isNull$564) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeString(2, field$564);
            }

            boolean isNull$565 = in1.isNullAt(3);
            org.apache.flink.table.data.binary.BinaryStringData field$565 =
                    isNull$565
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(3)));
            if (isNull$565) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeString(3, field$565);
            }

            boolean isNull$566 = in1.isNullAt(4);
            org.apache.flink.table.data.binary.BinaryStringData field$566 =
                    isNull$566
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(4)));
            if (isNull$566) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeString(4, field$566);
            }

            boolean isNull$567 = in1.isNullAt(5);
            org.apache.flink.table.data.binary.BinaryStringData field$567 =
                    isNull$567
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(5)));
            if (isNull$567) {
                outWriter.setNullAt(5);
            } else {
                outWriter.writeString(5, field$567);
            }

            boolean isNull$568 = in1.isNullAt(6);
            org.apache.flink.table.data.binary.BinaryStringData field$568 =
                    isNull$568
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(6)));
            if (isNull$568) {
                outWriter.setNullAt(6);
            } else {
                outWriter.writeString(6, field$568);
            }

            boolean isNull$569 = in1.isNullAt(7);
            org.apache.flink.table.data.binary.BinaryStringData field$569 =
                    isNull$569
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(7)));
            if (isNull$569) {
                outWriter.setNullAt(7);
            } else {
                outWriter.writeString(7, field$569);
            }

            outWriter.complete();

            return out;
        }
    }
}
