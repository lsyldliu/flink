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

package org.apache.flink.q2;

public final class BatchMultipleInputStreamOperator$357
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

        boolean anyNull$314 = false;
        boolean isNull$313 = in2.isNullAt(0);
        int field$313 = isNull$313 ? -1 : (in2.getInt(0));

        anyNull$314 |= isNull$313;

        if (!anyNull$314) {

            hashTable$315.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in2);
        }
    }

    public void processInput1(org.apache.flink.table.data.RowData in1) throws Exception {

        // evaluate the required expr in advance

        // generate join key for probe side
        boolean isNull$316 = in1.isNullAt(0);
        int field$316 = isNull$316 ? -1 : (in1.getInt(0));
        // find matches from hash table
        org.apache.flink.table.runtime.util.RowIterator buildIter$326 =
                isNull$316 ? null : hashTable$315.get(field$316);
        if (buildIter$326 != null) {
            while (buildIter$326.advanceNext()) {
                org.apache.flink.table.data.RowData buildRow$324 = buildIter$326.getRow();
                {

                    // evaluate the required expr in advance

                    // evaluate the required expr in advance

                    // wrap variable to row

                    boolean isNull$317 = in1.isNullAt(1);
                    double field$317 = isNull$317 ? -1.0d : (in1.getDouble(1));
                    if (isNull$317) {
                        out_inputRow$310.setNullAt(0);
                    } else {
                        out_inputRow$310.setDouble(0, field$317);
                    }

                    boolean isNull$318 = in1.isNullAt(2);
                    double field$318 = isNull$318 ? -1.0d : (in1.getDouble(2));
                    if (isNull$318) {
                        out_inputRow$310.setNullAt(1);
                    } else {
                        out_inputRow$310.setDouble(1, field$318);
                    }

                    boolean isNull$319 = in1.isNullAt(3);
                    double field$319 = isNull$319 ? -1.0d : (in1.getDouble(3));
                    if (isNull$319) {
                        out_inputRow$310.setNullAt(2);
                    } else {
                        out_inputRow$310.setDouble(2, field$319);
                    }

                    boolean isNull$320 = in1.isNullAt(4);
                    double field$320 = isNull$320 ? -1.0d : (in1.getDouble(4));
                    if (isNull$320) {
                        out_inputRow$310.setNullAt(3);
                    } else {
                        out_inputRow$310.setDouble(3, field$320);
                    }

                    boolean isNull$321 = in1.isNullAt(5);
                    double field$321 = isNull$321 ? -1.0d : (in1.getDouble(5));
                    if (isNull$321) {
                        out_inputRow$310.setNullAt(4);
                    } else {
                        out_inputRow$310.setDouble(4, field$321);
                    }

                    boolean isNull$322 = in1.isNullAt(6);
                    double field$322 = isNull$322 ? -1.0d : (in1.getDouble(6));
                    if (isNull$322) {
                        out_inputRow$310.setNullAt(5);
                    } else {
                        out_inputRow$310.setDouble(5, field$322);
                    }

                    boolean isNull$323 = in1.isNullAt(7);
                    double field$323 = isNull$323 ? -1.0d : (in1.getDouble(7));
                    if (isNull$323) {
                        out_inputRow$310.setNullAt(6);
                    } else {
                        out_inputRow$310.setDouble(6, field$323);
                    }

                    boolean isNull$328 = isNull$316 || false;
                    int result$328 = -1;
                    if (!isNull$328) {

                        result$328 = (int) (field$316 - ((int) 53));
                    }

                    if (isNull$328) {
                        out_inputRow$310.setNullAt(7);
                    } else {
                        out_inputRow$310.setInt(7, result$328);
                    }

                    output.collect(outElement.replace(out_inputRow$310));
                }
            }
        }
    }

    public void endInput2() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$315.endBuild();
    }

    public void endInput1() throws Exception {}

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.BoxedWrapperRowData out_inputRow$310 =
            new org.apache.flink.table.data.BoxedWrapperRowData(8);
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$333;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$334;
    Projection$335 buildToBinaryRow$346;
    private transient java.lang.Object[] buildProjRefs$347;
    Projection$337 probeToBinaryRow$346;
    private transient java.lang.Object[] probeProjRefs$348;
    LongHashTable$349 hashTable$315;

    private void hj_init$352(Object[] references) throws Exception {
        buildSer$333 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$334 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$347 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$346 = new Projection$335(buildProjRefs$347);
        probeProjRefs$348 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$346 = new Projection$337(probeProjRefs$348);
    }

    private transient java.lang.Object[] hj_Refs$353;

    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$356;

    public BatchMultipleInputStreamOperator$357(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        hj_Refs$353 = (((java.lang.Object[]) references[0]));
        hj_init$352(hj_Refs$353);
        inputSpecRefs$356 = (((java.util.ArrayList) references[1]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$356);
    }

    @Override
    public void open() throws Exception {
        super.open();

        long memorySize$350 = computeMemorySize(1.0);
        hashTable$315 = new LongHashTable$349(memorySize$350);
    }

    @Override
    public java.util.List getInputs() {
        return java.util.Arrays.asList(
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 1) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`d_week_seq` INT, `sun_sales` DOUBLE, `mon_sales` DOUBLE,
                        // `tue_sales` DOUBLE, `wed_sales` DOUBLE, `thu_sales` DOUBLE, `fri_sales`
                        // DOUBLE, `sat_sales` DOUBLE>
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
                        // InputType: ROW<`d_week_seq` INT>
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

        if (this.hashTable$315 != null) {
            this.hashTable$315.close();
            this.hashTable$315.free();
            this.hashTable$315 = null;
        }
    }

    public class Projection$335
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$335(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$336 = in1.isNullAt(0);
            int field$336 = isNull$336 ? -1 : (in1.getInt(0));
            if (isNull$336) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeInt(0, field$336);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$337
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(8);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$337(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$338 = in1.isNullAt(0);
            int field$338 = isNull$338 ? -1 : (in1.getInt(0));
            if (isNull$338) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeInt(0, field$338);
            }

            boolean isNull$339 = in1.isNullAt(1);
            double field$339 = isNull$339 ? -1.0d : (in1.getDouble(1));
            if (isNull$339) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeDouble(1, field$339);
            }

            boolean isNull$340 = in1.isNullAt(2);
            double field$340 = isNull$340 ? -1.0d : (in1.getDouble(2));
            if (isNull$340) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeDouble(2, field$340);
            }

            boolean isNull$341 = in1.isNullAt(3);
            double field$341 = isNull$341 ? -1.0d : (in1.getDouble(3));
            if (isNull$341) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeDouble(3, field$341);
            }

            boolean isNull$342 = in1.isNullAt(4);
            double field$342 = isNull$342 ? -1.0d : (in1.getDouble(4));
            if (isNull$342) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeDouble(4, field$342);
            }

            boolean isNull$343 = in1.isNullAt(5);
            double field$343 = isNull$343 ? -1.0d : (in1.getDouble(5));
            if (isNull$343) {
                outWriter.setNullAt(5);
            } else {
                outWriter.writeDouble(5, field$343);
            }

            boolean isNull$344 = in1.isNullAt(6);
            double field$344 = isNull$344 ? -1.0d : (in1.getDouble(6));
            if (isNull$344) {
                outWriter.setNullAt(6);
            } else {
                outWriter.writeDouble(6, field$344);
            }

            boolean isNull$345 = in1.isNullAt(7);
            double field$345 = isNull$345 ? -1.0d : (in1.getDouble(7));
            if (isNull$345) {
                outWriter.setNullAt(7);
            } else {
                outWriter.writeDouble(7, field$345);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$349
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$349(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$333,
                    probeSer$334,
                    getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    getContainingTask().getEnvironment().getIOManager(),
                    8,
                    367L / getRuntimeContext().getNumberOfParallelSubtasks());
        }

        @Override
        public long getBuildLongKey(org.apache.flink.table.data.RowData row) {
            return row.getInt(0);
        }

        @Override
        public long getProbeLongKey(org.apache.flink.table.data.RowData row) {
            return row.getInt(0);
        }

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData probeToBinary(
                org.apache.flink.table.data.RowData row) {
            if (row instanceof org.apache.flink.table.data.binary.BinaryRowData) {
                return (org.apache.flink.table.data.binary.BinaryRowData) row;
            } else {
                return probeToBinaryRow$346.apply(row);
            }
        }
    }
}
