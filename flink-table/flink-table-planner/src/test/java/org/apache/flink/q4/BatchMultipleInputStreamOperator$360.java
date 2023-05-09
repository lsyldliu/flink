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

public final class BatchMultipleInputStreamOperator$360
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

        boolean anyNull$306 = false;
        boolean isNull$298 = in2.isNullAt(0);
        long field$298 = isNull$298 ? -1L : (in2.getLong(0));

        anyNull$306 |= isNull$298;

        if (!anyNull$306) {

            hashTable$307.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in2);
        }
    }

    public void processInput1(org.apache.flink.table.data.RowData in1) throws Exception {

        // evaluate the required expr in advance

        // generate join key for probe side
        boolean isNull$308 = in1.isNullAt(0);
        long field$308 = isNull$308 ? -1L : (in1.getLong(0));
        // find matches from hash table
        org.apache.flink.table.runtime.util.RowIterator buildIter$322 =
                isNull$308 ? null : hashTable$307.get(field$308);
        if (buildIter$322 != null) {
            while (buildIter$322.advanceNext()) {
                org.apache.flink.table.data.RowData buildRow$313 = buildIter$322.getRow();
                {

                    // evaluate the required expr in advance

                    // evaluate the required expr in advance

                    // wrap variable to row

                    boolean isNull$315 = buildRow$313.isNullAt(1);
                    org.apache.flink.table.data.binary.BinaryStringData field$315 =
                            isNull$315
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$313.getString(1)));
                    out_inputRow$295.setNonPrimitiveValue(0, field$315);

                    boolean isNull$316 = buildRow$313.isNullAt(2);
                    org.apache.flink.table.data.binary.BinaryStringData field$316 =
                            isNull$316
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$313.getString(2)));
                    if (isNull$316) {
                        out_inputRow$295.setNullAt(1);
                    } else {
                        out_inputRow$295.setNonPrimitiveValue(1, field$316);
                    }

                    boolean isNull$317 = buildRow$313.isNullAt(3);
                    org.apache.flink.table.data.binary.BinaryStringData field$317 =
                            isNull$317
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$313.getString(3)));
                    if (isNull$317) {
                        out_inputRow$295.setNullAt(2);
                    } else {
                        out_inputRow$295.setNonPrimitiveValue(2, field$317);
                    }

                    boolean isNull$318 = buildRow$313.isNullAt(4);
                    org.apache.flink.table.data.binary.BinaryStringData field$318 =
                            isNull$318
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$313.getString(4)));
                    if (isNull$318) {
                        out_inputRow$295.setNullAt(3);
                    } else {
                        out_inputRow$295.setNonPrimitiveValue(3, field$318);
                    }

                    boolean isNull$319 = buildRow$313.isNullAt(5);
                    org.apache.flink.table.data.binary.BinaryStringData field$319 =
                            isNull$319
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$313.getString(5)));
                    if (isNull$319) {
                        out_inputRow$295.setNullAt(4);
                    } else {
                        out_inputRow$295.setNonPrimitiveValue(4, field$319);
                    }

                    boolean isNull$320 = buildRow$313.isNullAt(6);
                    org.apache.flink.table.data.binary.BinaryStringData field$320 =
                            isNull$320
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$313.getString(6)));
                    if (isNull$320) {
                        out_inputRow$295.setNullAt(5);
                    } else {
                        out_inputRow$295.setNonPrimitiveValue(5, field$320);
                    }

                    boolean isNull$321 = buildRow$313.isNullAt(7);
                    org.apache.flink.table.data.binary.BinaryStringData field$321 =
                            isNull$321
                                    ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                                    : (((org.apache.flink.table.data.binary.BinaryStringData)
                                            buildRow$313.getString(7)));
                    if (isNull$321) {
                        out_inputRow$295.setNullAt(6);
                    } else {
                        out_inputRow$295.setNonPrimitiveValue(6, field$321);
                    }

                    // --- Cast section generated by
                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                    // --- End cast section

                    if (false) {
                        out_inputRow$295.setNullAt(7);
                    } else {
                        out_inputRow$295.setInt(7, ((int) 2001));
                    }

                    boolean isNull$312 = in1.isNullAt(4);
                    double field$312 = isNull$312 ? -1.0d : (in1.getDouble(4));
                    boolean isNull$311 = in1.isNullAt(3);
                    double field$311 = isNull$311 ? -1.0d : (in1.getDouble(3));
                    boolean isNull$324 = isNull$312 || isNull$311;
                    double result$324 = -1.0d;
                    if (!isNull$324) {

                        result$324 = (double) (field$312 - field$311);
                    }

                    boolean isNull$309 = in1.isNullAt(1);
                    double field$309 = isNull$309 ? -1.0d : (in1.getDouble(1));
                    boolean isNull$325 = isNull$324 || isNull$309;
                    double result$325 = -1.0d;
                    if (!isNull$325) {

                        result$325 = (double) (result$324 - field$309);
                    }

                    boolean isNull$310 = in1.isNullAt(2);
                    double field$310 = isNull$310 ? -1.0d : (in1.getDouble(2));
                    boolean isNull$326 = isNull$325 || isNull$310;
                    double result$326 = -1.0d;
                    if (!isNull$326) {

                        result$326 = (double) (result$325 + field$310);
                    }

                    boolean isNull$327 = isNull$326 || false;
                    double result$327 = -1.0d;
                    if (!isNull$327) {

                        result$327 = (double) (result$326 / ((double) (((int) 2))));
                    }

                    if (isNull$327) {
                        out_inputRow$295.setNullAt(8);
                    } else {
                        out_inputRow$295.setDouble(8, result$327);
                    }

                    output.collect(outElement.replace(out_inputRow$295));
                }
            }
        }
    }

    public void endInput2() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$307.endBuild();
    }

    public void endInput1() throws Exception {}

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.BoxedWrapperRowData out_inputRow$295 =
            new org.apache.flink.table.data.BoxedWrapperRowData(9);
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$332;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$333;
    Projection$334 buildToBinaryRow$349;
    private transient java.lang.Object[] buildProjRefs$350;
    Projection$343 probeToBinaryRow$349;
    private transient java.lang.Object[] probeProjRefs$351;
    LongHashTable$352 hashTable$307;

    private void hj_init$355(Object[] references) throws Exception {
        buildSer$332 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$333 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$350 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$349 = new Projection$334(buildProjRefs$350);
        probeProjRefs$351 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$349 = new Projection$343(probeProjRefs$351);
    }

    private transient java.lang.Object[] hj_Refs$356;

    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$359;

    public BatchMultipleInputStreamOperator$360(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        hj_Refs$356 = (((java.lang.Object[]) references[0]));
        hj_init$355(hj_Refs$356);
        inputSpecRefs$359 = (((java.util.ArrayList) references[1]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$359);
    }

    @Override
    public void open() throws Exception {
        super.open();

        long memorySize$353 = computeMemorySize(1.0);
        hashTable$307 = new LongHashTable$352(memorySize$353);
    }

    @Override
    public java.util.List getInputs() {
        return java.util.Arrays.asList(
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 1) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`ws_bill_customer_sk` BIGINT, `ws_ext_discount_amt`
                        // DOUBLE, `ws_ext_sales_price` DOUBLE, `ws_ext_wholesale_cost` DOUBLE,
                        // `ws_ext_list_price` DOUBLE>
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

        if (this.hashTable$307 != null) {
            this.hashTable$307.close();
            this.hashTable$307.free();
            this.hashTable$307 = null;
        }
    }

    public class Projection$343
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(5);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$343(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$344 = in1.isNullAt(0);
            long field$344 = isNull$344 ? -1L : (in1.getLong(0));
            if (isNull$344) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$344);
            }

            boolean isNull$345 = in1.isNullAt(1);
            double field$345 = isNull$345 ? -1.0d : (in1.getDouble(1));
            if (isNull$345) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeDouble(1, field$345);
            }

            boolean isNull$346 = in1.isNullAt(2);
            double field$346 = isNull$346 ? -1.0d : (in1.getDouble(2));
            if (isNull$346) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeDouble(2, field$346);
            }

            boolean isNull$347 = in1.isNullAt(3);
            double field$347 = isNull$347 ? -1.0d : (in1.getDouble(3));
            if (isNull$347) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeDouble(3, field$347);
            }

            boolean isNull$348 = in1.isNullAt(4);
            double field$348 = isNull$348 ? -1.0d : (in1.getDouble(4));
            if (isNull$348) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeDouble(4, field$348);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$334
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(8);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$334(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$335 = in1.isNullAt(0);
            long field$335 = isNull$335 ? -1L : (in1.getLong(0));
            if (isNull$335) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$335);
            }

            boolean isNull$336 = in1.isNullAt(1);
            org.apache.flink.table.data.binary.BinaryStringData field$336 =
                    isNull$336
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(1)));
            if (isNull$336) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeString(1, field$336);
            }

            boolean isNull$337 = in1.isNullAt(2);
            org.apache.flink.table.data.binary.BinaryStringData field$337 =
                    isNull$337
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(2)));
            if (isNull$337) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeString(2, field$337);
            }

            boolean isNull$338 = in1.isNullAt(3);
            org.apache.flink.table.data.binary.BinaryStringData field$338 =
                    isNull$338
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(3)));
            if (isNull$338) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeString(3, field$338);
            }

            boolean isNull$339 = in1.isNullAt(4);
            org.apache.flink.table.data.binary.BinaryStringData field$339 =
                    isNull$339
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(4)));
            if (isNull$339) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeString(4, field$339);
            }

            boolean isNull$340 = in1.isNullAt(5);
            org.apache.flink.table.data.binary.BinaryStringData field$340 =
                    isNull$340
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(5)));
            if (isNull$340) {
                outWriter.setNullAt(5);
            } else {
                outWriter.writeString(5, field$340);
            }

            boolean isNull$341 = in1.isNullAt(6);
            org.apache.flink.table.data.binary.BinaryStringData field$341 =
                    isNull$341
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(6)));
            if (isNull$341) {
                outWriter.setNullAt(6);
            } else {
                outWriter.writeString(6, field$341);
            }

            boolean isNull$342 = in1.isNullAt(7);
            org.apache.flink.table.data.binary.BinaryStringData field$342 =
                    isNull$342
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(7)));
            if (isNull$342) {
                outWriter.setNullAt(7);
            } else {
                outWriter.writeString(7, field$342);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$352
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$352(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$332,
                    probeSer$333,
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
                return probeToBinaryRow$349.apply(row);
            }
        }
    }
}
