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

package org.apache.flink.table.runtime.operators.multipleinput.join;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.StringDataSerializer;

/** ship_mode join catalog_sales. */
public final class ShipModeHashJoinOperator {
    public static org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(ShipModeHashJoinOperator.class);

    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$168;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$169;
    Projection$170 buildToBinaryRow;
    Projection$173 probeToBinaryRow;
    ConditionFunction$138 condFunc;
    org.apache.flink.table.data.utils.JoinedRowData joinedRow =
            new org.apache.flink.table.data.utils.JoinedRowData();
    LongHashTable$167 table;

    private transient boolean buildEnd$187 = false;
    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);

    private final WarehouseHashJoinOperator warehouseHashJoinOperator;

    public ShipModeHashJoinOperator(
            StreamOperatorParameters<RowData> parameters,
            RuntimeContext runtimeContext,
            OperatorMetricGroup operatorMetricGroup,
            long memorySize,
            WarehouseHashJoinOperator warehouseHashJoinOperator)
            throws Exception {
        buildSer$168 = new BinaryRowDataSerializer(2);
        probeSer$169 = new BinaryRowDataSerializer(5);
        buildToBinaryRow = new Projection$170();
        probeToBinaryRow = new Projection$173();
        condFunc = new ConditionFunction$138();

        condFunc.setRuntimeContext(runtimeContext);
        condFunc.open(new org.apache.flink.configuration.Configuration());
        operatorMetricGroup.gauge(
                "memoryUsedSizeInBytes",
                new org.apache.flink.metrics.Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return table.getUsedMemoryInBytes();
                    }
                });
        operatorMetricGroup.gauge(
                "numSpillFiles",
                new org.apache.flink.metrics.Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return table.getNumSpillFiles();
                    }
                });
        operatorMetricGroup.gauge(
                "spillInBytes",
                new org.apache.flink.metrics.Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return table.getSpillInBytes();
                    }
                });

        table =
                new LongHashTable$167(
                        parameters, memorySize, runtimeContext.getNumberOfParallelSubtasks());
        this.warehouseHashJoinOperator = warehouseHashJoinOperator;
        LOG.info("Initializing ship_mode table join operator successfully.");
    }

    public void processBuild(org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
            throws Exception {
        org.apache.flink.table.data.RowData row =
                (org.apache.flink.table.data.RowData) element.getValue();

        boolean anyNull$185 = false;
        anyNull$185 |= row.isNullAt(0);

        if (!anyNull$185) {
            table.putBuildRow(
                    row instanceof org.apache.flink.table.data.binary.BinaryRowData
                            ? (org.apache.flink.table.data.binary.BinaryRowData) row
                            : buildToBinaryRow.apply(row));
        }
    }

    public void processProbe(org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
            throws Exception {
        org.apache.flink.table.data.RowData row =
                (org.apache.flink.table.data.RowData) element.getValue();

        boolean anyNull$186 = false;
        anyNull$186 |= row.isNullAt(1);

        if (!anyNull$186) {
            if (table.tryProbe(row)) {
                joinWithNextKey();
            }
        }
    }

    private void endInput1() throws Exception {
        LOG.info("Finish ship_mode table build phase.");
        table.endBuild();
        buildEnd$187 = true;
    }

    private void endInput2() throws Exception {
        LOG.info("Finish probe phase.");
        while (this.table.nextMatching()) {
            joinWithNextKey();
        }
        LOG.info("Finish rebuild phase.");
    }

    public void endInput(int inputId) throws Exception {
        switch (inputId) {
            case 1:
                endInput1();
                break;
            case 2:
                endInput2();
                break;
        }
    }

    public void close() throws Exception {
        condFunc.close();
        if (this.table != null) {
            this.table.close();
            this.table.free();
            this.table = null;
        }
        LOG.info("Close ship_mode hashtable successfully.");
    }

    private void joinWithNextKey() throws Exception {
        org.apache.flink.table.runtime.hashtable.LongHashPartition.MatchIterator buildIter =
                table.getBuildSideIterator();
        org.apache.flink.table.data.RowData probeRow = table.getCurrentProbeRow();
        if (probeRow == null) {
            throw new RuntimeException("ProbeRow should not be null");
        }

        while (buildIter.advanceNext()) {
            if (condFunc.apply(probeRow, buildIter.getRow())) {
                processShipModeJoinCalc(
                        outElement.replace(joinedRow.replace(probeRow, buildIter.getRow())));
            }
        }
    }

    public class LongHashTable$167
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$167(
                StreamOperatorParameters<RowData> parameters, long memorySize, long parallelTasks) {
            super(
                    parameters.getContainingTask().getJobConfiguration(),
                    parameters.getContainingTask(),
                    buildSer$168,
                    probeSer$169,
                    parameters.getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    parameters.getContainingTask().getEnvironment().getIOManager(),
                    15,
                    20L / parallelTasks);
        }

        @Override
        public long getBuildLongKey(org.apache.flink.table.data.RowData row) {
            return row.getLong(0);
        }

        @Override
        public long getProbeLongKey(org.apache.flink.table.data.RowData row) {
            return row.getLong(1);
        }

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData probeToBinary(
                org.apache.flink.table.data.RowData row) {
            if (row instanceof org.apache.flink.table.data.binary.BinaryRowData) {
                return (org.apache.flink.table.data.binary.BinaryRowData) row;
            } else {
                return probeToBinaryRow.apply(row);
            }
        }
    }

    public class Projection$173
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(5);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {
            long field$174;
            boolean isNull$174;
            long field$175;
            boolean isNull$175;
            long field$176;
            boolean isNull$176;
            long field$177;
            boolean isNull$177;
            org.apache.flink.table.data.binary.BinaryStringData field$178;
            boolean isNull$178;

            outWriter.reset();

            isNull$174 = in1.isNullAt(0);
            field$174 = -1L;
            if (!isNull$174) {
                field$174 = in1.getLong(0);
            }
            if (isNull$174) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$174);
            }

            isNull$175 = in1.isNullAt(1);
            field$175 = -1L;
            if (!isNull$175) {
                field$175 = in1.getLong(1);
            }
            if (isNull$175) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$175);
            }

            isNull$176 = in1.isNullAt(2);
            field$176 = -1L;
            if (!isNull$176) {
                field$176 = in1.getLong(2);
            }
            if (isNull$176) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeLong(2, field$176);
            }

            isNull$177 = in1.isNullAt(3);
            field$177 = -1L;
            if (!isNull$177) {
                field$177 = in1.getLong(3);
            }
            if (isNull$177) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeLong(3, field$177);
            }

            isNull$178 = in1.isNullAt(4);
            field$178 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
            if (!isNull$178) {
                field$178 =
                        ((org.apache.flink.table.data.binary.BinaryStringData) in1.getString(4));
            }
            if (isNull$178) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeString(4, field$178);
            }

            outWriter.complete();

            return out;
        }
    }

    public class ConditionFunction$138
            extends org.apache.flink.api.common.functions.AbstractRichFunction
            implements org.apache.flink.table.runtime.generated.JoinCondition {

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters)
                throws Exception {}

        @Override
        public boolean apply(
                org.apache.flink.table.data.RowData in1, org.apache.flink.table.data.RowData in2) {
            return true;
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }

    public class Projection$170
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(2);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {
            long field$171;
            boolean isNull$171;
            org.apache.flink.table.data.binary.BinaryStringData field$172;
            boolean isNull$172;

            outWriter.reset();

            isNull$171 = in1.isNullAt(0);
            field$171 = -1L;
            if (!isNull$171) {
                field$171 = in1.getLong(0);
            }
            if (isNull$171) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$171);
            }

            isNull$172 = in1.isNullAt(1);
            field$172 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
            if (!isNull$172) {
                field$172 =
                        ((org.apache.flink.table.data.binary.BinaryStringData) in1.getString(1));
            }
            if (isNull$172) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeString(1, field$172);
            }

            outWriter.complete();

            return out;
        }
    }

    private transient org.apache.flink.table.runtime.typeutils.StringDataSerializer
            typeSerializer$193 = StringDataSerializer.INSTANCE;
    org.apache.flink.table.data.BoxedWrapperRowData out =
            new org.apache.flink.table.data.BoxedWrapperRowData(5);

    /** The projection after catalog_sales join ship_mode. */
    public void processShipModeJoinCalc(StreamRecord element) throws Exception {
        org.apache.flink.table.data.RowData in1 =
                (org.apache.flink.table.data.RowData) element.getValue();

        long field$189;
        boolean isNull$189;
        long field$190;
        boolean isNull$190;
        long field$191;
        boolean isNull$191;
        org.apache.flink.table.data.binary.BinaryStringData field$192;
        boolean isNull$192;
        org.apache.flink.table.data.binary.BinaryStringData field$194;
        org.apache.flink.table.data.binary.BinaryStringData field$195;
        boolean isNull$195;
        org.apache.flink.table.data.binary.BinaryStringData field$196;

        isNull$195 = in1.isNullAt(6);
        field$195 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
        if (!isNull$195) {
            field$195 = ((org.apache.flink.table.data.binary.BinaryStringData) in1.getString(6));
        }
        field$196 = field$195;
        if (!isNull$195) {
            field$196 =
                    (org.apache.flink.table.data.binary.BinaryStringData)
                            (typeSerializer$193.copy(field$196));
        }

        isNull$190 = in1.isNullAt(2);
        field$190 = -1L;
        if (!isNull$190) {
            field$190 = in1.getLong(2);
        }
        isNull$189 = in1.isNullAt(0);
        field$189 = -1L;
        if (!isNull$189) {
            field$189 = in1.getLong(0);
        }

        isNull$192 = in1.isNullAt(4);
        field$192 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
        if (!isNull$192) {
            field$192 = ((org.apache.flink.table.data.binary.BinaryStringData) in1.getString(4));
        }
        field$194 = field$192;
        if (!isNull$192) {
            field$194 =
                    (org.apache.flink.table.data.binary.BinaryStringData)
                            (typeSerializer$193.copy(field$194));
        }

        isNull$191 = in1.isNullAt(3);
        field$191 = -1L;
        if (!isNull$191) {
            field$191 = in1.getLong(3);
        }

        if (isNull$189) {
            out.setNullAt(0);
        } else {
            out.setLong(0, field$189);
        }

        if (isNull$190) {
            out.setNullAt(1);
        } else {
            out.setLong(1, field$190);
        }

        if (isNull$191) {
            out.setNullAt(2);
        } else {
            out.setLong(2, field$191);
        }

        if (isNull$192) {
            out.setNullAt(3);
        } else {
            out.setNonPrimitiveValue(3, field$194);
        }

        if (isNull$195) {
            out.setNullAt(4);
        } else {
            out.setNonPrimitiveValue(4, field$196);
        }

        // call warehouse bhj probe
        warehouseHashJoinOperator.processProbe(outElement.replace(out));
    }
}
