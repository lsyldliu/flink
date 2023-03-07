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
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.StringDataSerializer;

/** call_center join catalog_sales. */
public final class CallCenterHashJoinOperator {

    public static org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(CallCenterHashJoinOperator.class);

    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$109;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$110;
    Projection$111 buildToBinaryRow;
    Projection$114 probeToBinaryRow;
    ConditionFunction$79 condFunc;
    org.apache.flink.table.data.utils.JoinedRowData joinedRow =
            new org.apache.flink.table.data.utils.JoinedRowData();
    LongHashTable$108 table;

    private transient boolean buildEnd$128 = false;
    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);

    private final ShipModeHashJoinOperator shipModeHashJoinOperator;

    public CallCenterHashJoinOperator(
            StreamOperatorParameters<RowData> parameters,
            RuntimeContext runtimeContext,
            OperatorMetricGroup operatorMetricGroup,
            long memorySize,
            ShipModeHashJoinOperator shipModeHashJoinOperator)
            throws Exception {
        buildSer$109 = new BinaryRowDataSerializer(2);
        probeSer$110 = new BinaryRowDataSerializer(5);
        buildToBinaryRow = new Projection$111();
        probeToBinaryRow = new Projection$114();
        condFunc = new ConditionFunction$79();

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
                new LongHashTable$108(
                        parameters, memorySize, runtimeContext.getNumberOfParallelSubtasks());
        this.shipModeHashJoinOperator = shipModeHashJoinOperator;
        LOG.info("Initializing call_center table join operator successfully.");
    }

    public void processBuild(org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
            throws Exception {
        org.apache.flink.table.data.RowData row =
                (org.apache.flink.table.data.RowData) element.getValue();

        boolean anyNull$126 = false;
        anyNull$126 |= row.isNullAt(0);

        if (!anyNull$126) {
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

        boolean anyNull$127 = false;
        anyNull$127 |= row.isNullAt(1);

        if (!anyNull$127) {
            if (table.tryProbe(row)) {
                joinWithNextKey();
            }
        }
    }

    private void endInput1() throws Exception {
        LOG.info("Finish call_center table build phase.");
        table.endBuild();
        buildEnd$128 = true;
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
        LOG.info("Close call_center hashtable successfully.");
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
                // call calc to projection five fields
                processCallCenterJoinCalc(
                        outElement.replace(joinedRow.replace(probeRow, buildIter.getRow())));
            }
        }
    }

    public class LongHashTable$108
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$108(
                StreamOperatorParameters<RowData> parameters, long memorySize, long parallelTasks) {
            super(
                    parameters.getContainingTask(),
                    ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED.defaultValue(),
                    (int)
                            ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE
                                    .defaultValue()
                                    .getBytes(),
                    buildSer$109,
                    probeSer$110,
                    parameters.getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    parameters.getContainingTask().getEnvironment().getIOManager(),
                    21,
                    54L / parallelTasks);
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

    public class ConditionFunction$79
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

    public class Projection$111
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
            long field$112;
            boolean isNull$112;
            org.apache.flink.table.data.binary.BinaryStringData field$113;
            boolean isNull$113;

            outWriter.reset();

            isNull$112 = in1.isNullAt(0);
            field$112 = -1L;
            if (!isNull$112) {
                field$112 = in1.getLong(0);
            }
            if (isNull$112) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$112);
            }

            isNull$113 = in1.isNullAt(1);
            field$113 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
            if (!isNull$113) {
                field$113 =
                        ((org.apache.flink.table.data.binary.BinaryStringData) in1.getString(1));
            }
            if (isNull$113) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeString(1, field$113);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$114
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
            long field$115;
            boolean isNull$115;
            long field$116;
            boolean isNull$116;
            long field$117;
            boolean isNull$117;
            long field$118;
            boolean isNull$118;
            long field$119;
            boolean isNull$119;

            outWriter.reset();

            isNull$115 = in1.isNullAt(0);
            field$115 = -1L;
            if (!isNull$115) {
                field$115 = in1.getLong(0);
            }
            if (isNull$115) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$115);
            }

            isNull$116 = in1.isNullAt(1);
            field$116 = -1L;
            if (!isNull$116) {
                field$116 = in1.getLong(1);
            }
            if (isNull$116) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$116);
            }

            isNull$117 = in1.isNullAt(2);
            field$117 = -1L;
            if (!isNull$117) {
                field$117 = in1.getLong(2);
            }
            if (isNull$117) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeLong(2, field$117);
            }

            isNull$118 = in1.isNullAt(3);
            field$118 = -1L;
            if (!isNull$118) {
                field$118 = in1.getLong(3);
            }
            if (isNull$118) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeLong(3, field$118);
            }

            isNull$119 = in1.isNullAt(4);
            field$119 = -1L;
            if (!isNull$119) {
                field$119 = in1.getLong(4);
            }
            if (isNull$119) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeLong(4, field$119);
            }

            outWriter.complete();

            return out;
        }
    }

    private transient org.apache.flink.table.runtime.typeutils.StringDataSerializer
            typeSerializer$135 = StringDataSerializer.INSTANCE;

    org.apache.flink.table.data.BoxedWrapperRowData out =
            new org.apache.flink.table.data.BoxedWrapperRowData(5);

    /** The projection after catalog_sales join call_center. */
    public void processCallCenterJoinCalc(StreamRecord element) throws Exception {
        org.apache.flink.table.data.RowData in1 =
                (org.apache.flink.table.data.RowData) element.getValue();

        long field$130;
        boolean isNull$130;
        long field$131;
        boolean isNull$131;
        long field$132;
        boolean isNull$132;
        long field$133;
        boolean isNull$133;
        org.apache.flink.table.data.binary.BinaryStringData field$134;
        boolean isNull$134;
        org.apache.flink.table.data.binary.BinaryStringData field$136;

        isNull$134 = in1.isNullAt(6);
        field$134 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
        if (!isNull$134) {
            field$134 = ((org.apache.flink.table.data.binary.BinaryStringData) in1.getString(6));
        }
        field$136 = field$134;
        if (!isNull$134) {
            field$136 =
                    (org.apache.flink.table.data.binary.BinaryStringData)
                            (typeSerializer$135.copy(field$136));
        }

        isNull$131 = in1.isNullAt(2);
        field$131 = -1L;
        if (!isNull$131) {
            field$131 = in1.getLong(2);
        }
        isNull$130 = in1.isNullAt(0);
        field$130 = -1L;
        if (!isNull$130) {
            field$130 = in1.getLong(0);
        }
        isNull$133 = in1.isNullAt(4);
        field$133 = -1L;
        if (!isNull$133) {
            field$133 = in1.getLong(4);
        }
        isNull$132 = in1.isNullAt(3);
        field$132 = -1L;
        if (!isNull$132) {
            field$132 = in1.getLong(3);
        }

        if (isNull$130) {
            out.setNullAt(0);
        } else {
            out.setLong(0, field$130);
        }

        if (isNull$131) {
            out.setNullAt(1);
        } else {
            out.setLong(1, field$131);
        }

        if (isNull$132) {
            out.setNullAt(2);
        } else {
            out.setLong(2, field$132);
        }

        if (isNull$133) {
            out.setNullAt(3);
        } else {
            out.setLong(3, field$133);
        }

        if (isNull$134) {
            out.setNullAt(4);
        } else {
            out.setNonPrimitiveValue(4, field$136);
        }

        // call ship_mode bhj probe
        shipModeHashJoinOperator.processProbe(outElement.replace(out));
    }
}
