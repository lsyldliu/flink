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

public final class BatchMultipleInputStreamOperator$303
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

        boolean anyNull$261 = false;
        boolean isNull$260 = in2.isNullAt(0);
        int field$260 = isNull$260 ? -1 : (in2.getInt(0));

        anyNull$261 |= isNull$260;

        if (!anyNull$261) {

            hashTable$262.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in2);
        }
    }

    public void processInput1(org.apache.flink.table.data.RowData in1) throws Exception {

        // evaluate the required expr in advance

        // generate join key for probe side
        boolean isNull$263 = in1.isNullAt(0);
        int field$263 = isNull$263 ? -1 : (in1.getInt(0));
        // find matches from hash table
        org.apache.flink.table.runtime.util.RowIterator buildIter$273 =
                isNull$263 ? null : hashTable$262.get(field$263);
        if (buildIter$273 != null) {
            while (buildIter$273.advanceNext()) {
                org.apache.flink.table.data.RowData buildRow$271 = buildIter$273.getRow();
                {

                    // evaluate the required expr in advance

                    // evaluate the required expr in advance

                    // wrap variable to row

                    if (isNull$263) {
                        out_inputRow$257.setNullAt(0);
                    } else {
                        out_inputRow$257.setInt(0, field$263);
                    }

                    boolean isNull$264 = in1.isNullAt(1);
                    double field$264 = isNull$264 ? -1.0d : (in1.getDouble(1));
                    if (isNull$264) {
                        out_inputRow$257.setNullAt(1);
                    } else {
                        out_inputRow$257.setDouble(1, field$264);
                    }

                    boolean isNull$265 = in1.isNullAt(2);
                    double field$265 = isNull$265 ? -1.0d : (in1.getDouble(2));
                    if (isNull$265) {
                        out_inputRow$257.setNullAt(2);
                    } else {
                        out_inputRow$257.setDouble(2, field$265);
                    }

                    boolean isNull$266 = in1.isNullAt(3);
                    double field$266 = isNull$266 ? -1.0d : (in1.getDouble(3));
                    if (isNull$266) {
                        out_inputRow$257.setNullAt(3);
                    } else {
                        out_inputRow$257.setDouble(3, field$266);
                    }

                    boolean isNull$267 = in1.isNullAt(4);
                    double field$267 = isNull$267 ? -1.0d : (in1.getDouble(4));
                    if (isNull$267) {
                        out_inputRow$257.setNullAt(4);
                    } else {
                        out_inputRow$257.setDouble(4, field$267);
                    }

                    boolean isNull$268 = in1.isNullAt(5);
                    double field$268 = isNull$268 ? -1.0d : (in1.getDouble(5));
                    if (isNull$268) {
                        out_inputRow$257.setNullAt(5);
                    } else {
                        out_inputRow$257.setDouble(5, field$268);
                    }

                    boolean isNull$269 = in1.isNullAt(6);
                    double field$269 = isNull$269 ? -1.0d : (in1.getDouble(6));
                    if (isNull$269) {
                        out_inputRow$257.setNullAt(6);
                    } else {
                        out_inputRow$257.setDouble(6, field$269);
                    }

                    boolean isNull$270 = in1.isNullAt(7);
                    double field$270 = isNull$270 ? -1.0d : (in1.getDouble(7));
                    if (isNull$270) {
                        out_inputRow$257.setNullAt(7);
                    } else {
                        out_inputRow$257.setDouble(7, field$270);
                    }

                    output.collect(outElement.replace(out_inputRow$257));
                }
            }
        }
    }

    public void endInput2() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$262.endBuild();
    }

    public void endInput1() throws Exception {}

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.BoxedWrapperRowData out_inputRow$257 =
            new org.apache.flink.table.data.BoxedWrapperRowData(8);
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$279;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$280;
    Projection$281 buildToBinaryRow$292;
    private transient java.lang.Object[] buildProjRefs$293;
    Projection$283 probeToBinaryRow$292;
    private transient java.lang.Object[] probeProjRefs$294;
    LongHashTable$295 hashTable$262;

    private void hj_init$298(Object[] references) throws Exception {
        buildSer$279 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$280 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$293 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$292 = new Projection$281(buildProjRefs$293);
        probeProjRefs$294 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$292 = new Projection$283(probeProjRefs$294);
    }

    private transient java.lang.Object[] hj_Refs$299;

    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$302;

    public BatchMultipleInputStreamOperator$303(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 2);
        this.parameters = parameters;
        hj_Refs$299 = (((java.lang.Object[]) references[0]));
        hj_init$298(hj_Refs$299);
        inputSpecRefs$302 = (((java.util.ArrayList) references[1]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$302);
    }

    @Override
    public void open() throws Exception {
        super.open();

        long memorySize$296 = computeMemorySize(1.0);
        hashTable$262 = new LongHashTable$295(memorySize$296);
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

        if (this.hashTable$262 != null) {
            this.hashTable$262.close();
            this.hashTable$262.free();
            this.hashTable$262 = null;
        }
    }

    public class Projection$281
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$281(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$282 = in1.isNullAt(0);
            int field$282 = isNull$282 ? -1 : (in1.getInt(0));
            if (isNull$282) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeInt(0, field$282);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$283
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(8);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$283(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$284 = in1.isNullAt(0);
            int field$284 = isNull$284 ? -1 : (in1.getInt(0));
            if (isNull$284) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeInt(0, field$284);
            }

            boolean isNull$285 = in1.isNullAt(1);
            double field$285 = isNull$285 ? -1.0d : (in1.getDouble(1));
            if (isNull$285) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeDouble(1, field$285);
            }

            boolean isNull$286 = in1.isNullAt(2);
            double field$286 = isNull$286 ? -1.0d : (in1.getDouble(2));
            if (isNull$286) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeDouble(2, field$286);
            }

            boolean isNull$287 = in1.isNullAt(3);
            double field$287 = isNull$287 ? -1.0d : (in1.getDouble(3));
            if (isNull$287) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeDouble(3, field$287);
            }

            boolean isNull$288 = in1.isNullAt(4);
            double field$288 = isNull$288 ? -1.0d : (in1.getDouble(4));
            if (isNull$288) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeDouble(4, field$288);
            }

            boolean isNull$289 = in1.isNullAt(5);
            double field$289 = isNull$289 ? -1.0d : (in1.getDouble(5));
            if (isNull$289) {
                outWriter.setNullAt(5);
            } else {
                outWriter.writeDouble(5, field$289);
            }

            boolean isNull$290 = in1.isNullAt(6);
            double field$290 = isNull$290 ? -1.0d : (in1.getDouble(6));
            if (isNull$290) {
                outWriter.setNullAt(6);
            } else {
                outWriter.writeDouble(6, field$290);
            }

            boolean isNull$291 = in1.isNullAt(7);
            double field$291 = isNull$291 ? -1.0d : (in1.getDouble(7));
            if (isNull$291) {
                outWriter.setNullAt(7);
            } else {
                outWriter.writeDouble(7, field$291);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$295
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$295(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$279,
                    probeSer$280,
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
                return probeToBinaryRow$292.apply(row);
            }
        }
    }
}
