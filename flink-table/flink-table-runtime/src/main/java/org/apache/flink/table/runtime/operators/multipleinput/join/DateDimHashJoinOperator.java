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

/** date_dim join catalog_sales. */
public final class DateDimHashJoinOperator {

    public static org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(DateDimHashJoinOperator.class);

    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$53;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$54;
    Projection$55 buildToBinaryRow;
    Projection$57 probeToBinaryRow;
    ConditionFunction$23 condFunc;
    org.apache.flink.table.data.utils.JoinedRowData joinedRow =
            new org.apache.flink.table.data.utils.JoinedRowData();
    LongHashTable$52 table;

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);

    private final CallCenterHashJoinOperator callCenterHashJoinOperator;

    public DateDimHashJoinOperator(
            StreamOperatorParameters<RowData> parameters,
            RuntimeContext runtimeContext,
            OperatorMetricGroup operatorMetricGroup,
            long memorySize,
            CallCenterHashJoinOperator callCenterHashJoinOperator)
            throws Exception {
        buildSer$53 = new BinaryRowDataSerializer(1);
        probeSer$54 = new BinaryRowDataSerializer(5);
        buildToBinaryRow = new Projection$55();
        probeToBinaryRow = new Projection$57();
        condFunc = new ConditionFunction$23();

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
                new LongHashTable$52(
                        parameters, memorySize, runtimeContext.getNumberOfParallelSubtasks());
        this.callCenterHashJoinOperator = callCenterHashJoinOperator;
        LOG.info("Initializing date_dim table join operator successfully.");
    }

    public void processBuild(org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
            throws Exception {
        org.apache.flink.table.data.RowData row =
                (org.apache.flink.table.data.RowData) element.getValue();

        boolean anyNull$69 = false;
        anyNull$69 |= row.isNullAt(0);

        if (!anyNull$69) {
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

        boolean anyNull$70 = false;
        anyNull$70 |= row.isNullAt(0);

        if (!anyNull$70) {
            if (table.tryProbe(row)) {
                joinWithNextKey();
            }
        }
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
                // call the calc to projection fields
                processDateDimJoinCalc(
                        outElement.replace(joinedRow.replace(probeRow, buildIter.getRow())));
            }
        }
    }

    public void endInput1() throws Exception {
        LOG.info("Finish date_dim table build phase.");
        table.endBuild();
    }

    private void endInput2() throws Exception {
        LOG.info("Finish probe phase.");
        while (this.table.nextMatching()) {
            joinWithNextKey();
        }
        LOG.info("Finish rebuild phase.");
    }

    public void close() throws Exception {
        condFunc.close();
        if (this.table != null) {
            this.table.close();
            this.table.free();
            this.table = null;
        }
        LOG.info("Close date_dim hashtable successfully.");
    }

    public class LongHashTable$52
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$52(
                StreamOperatorParameters<RowData> parameters, long memorySize, long parallelTasks) {
            super(
                    parameters.getContainingTask(),
                    ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED.defaultValue(),
                    (int)
                            ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE
                                    .defaultValue()
                                    .getBytes(),
                    buildSer$53,
                    probeSer$54,
                    parameters.getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    parameters.getContainingTask().getEnvironment().getIOManager(),
                    8,
                    334L / parallelTasks);
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
                return probeToBinaryRow.apply(row);
            }
        }
    }

    public class ConditionFunction$23
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

    public class Projection$57
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
            long field$58;
            boolean isNull$58;
            long field$59;
            boolean isNull$59;
            long field$60;
            boolean isNull$60;
            long field$61;
            boolean isNull$61;
            long field$62;
            boolean isNull$62;

            outWriter.reset();

            isNull$58 = in1.isNullAt(0);
            field$58 = -1L;
            if (!isNull$58) {
                field$58 = in1.getLong(0);
            }
            if (isNull$58) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$58);
            }

            isNull$59 = in1.isNullAt(1);
            field$59 = -1L;
            if (!isNull$59) {
                field$59 = in1.getLong(1);
            }
            if (isNull$59) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$59);
            }

            isNull$60 = in1.isNullAt(2);
            field$60 = -1L;
            if (!isNull$60) {
                field$60 = in1.getLong(2);
            }
            if (isNull$60) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeLong(2, field$60);
            }

            isNull$61 = in1.isNullAt(3);
            field$61 = -1L;
            if (!isNull$61) {
                field$61 = in1.getLong(3);
            }
            if (isNull$61) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeLong(3, field$61);
            }

            isNull$62 = in1.isNullAt(4);
            field$62 = -1L;
            if (!isNull$62) {
                field$62 = in1.getLong(4);
            }
            if (isNull$62) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeLong(4, field$62);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$55
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {
            long field$56;
            boolean isNull$56;

            outWriter.reset();

            isNull$56 = in1.isNullAt(0);
            field$56 = -1L;
            if (!isNull$56) {
                field$56 = in1.getLong(0);
            }
            if (isNull$56) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$56);
            }

            outWriter.complete();

            return out;
        }
    }

    org.apache.flink.table.data.BoxedWrapperRowData out =
            new org.apache.flink.table.data.BoxedWrapperRowData(5);

    /** The projection after catalog_sales join date_dim. */
    public void processDateDimJoinCalc(StreamRecord element) throws Exception {
        org.apache.flink.table.data.RowData in1 =
                (org.apache.flink.table.data.RowData) element.getValue();

        long field$73;
        boolean isNull$73;
        long field$74;
        boolean isNull$74;
        long field$75;
        boolean isNull$75;
        long field$76;
        boolean isNull$76;
        long field$77;
        boolean isNull$77;

        isNull$75 = in1.isNullAt(2);
        field$75 = -1L;
        if (!isNull$75) {
            field$75 = in1.getLong(2);
        }
        isNull$73 = in1.isNullAt(0);
        field$73 = -1L;
        if (!isNull$73) {
            field$73 = in1.getLong(0);
        }
        isNull$77 = in1.isNullAt(4);
        field$77 = -1L;
        if (!isNull$77) {
            field$77 = in1.getLong(4);
        }
        isNull$74 = in1.isNullAt(1);
        field$74 = -1L;
        if (!isNull$74) {
            field$74 = in1.getLong(1);
        }
        isNull$76 = in1.isNullAt(3);
        field$76 = -1L;
        if (!isNull$76) {
            field$76 = in1.getLong(3);
        }

        if (isNull$73) {
            out.setNullAt(0);
        } else {
            out.setLong(0, field$73);
        }

        if (isNull$74) {
            out.setNullAt(1);
        } else {
            out.setLong(1, field$74);
        }

        if (isNull$75) {
            out.setNullAt(2);
        } else {
            out.setLong(2, field$75);
        }

        if (isNull$76) {
            out.setNullAt(3);
        } else {
            out.setLong(3, field$76);
        }

        if (isNull$77) {
            out.setNullAt(4);
        } else {
            out.setLong(4, field$77);
        }

        // call call_center join probe
        callCenterHashJoinOperator.processProbe(outElement.replace(out));
    }
}
