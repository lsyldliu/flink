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
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;

/** warehouse join catalog_sales. */
public final class WarehouseHashJoinOperator {

    public static org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(WarehouseHashJoinOperator.class);

    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$228;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$229;
    Projection$230 buildToBinaryRow;
    Projection$233 probeToBinaryRow;
    ConditionFunction$198 condFunc;
    org.apache.flink.table.data.utils.JoinedRowData joinedRow =
            new org.apache.flink.table.data.utils.JoinedRowData();
    LongHashTable$227 table;

    private transient boolean buildEnd$247 = false;
    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);

    private final Output<StreamRecord<RowData>> output;

    public WarehouseHashJoinOperator(
            StreamOperatorParameters<RowData> parameters,
            RuntimeContext runtimeContext,
            OperatorMetricGroup operatorMetricGroup,
            long memorySize,
            Output<StreamRecord<RowData>> output)
            throws Exception {
        buildSer$228 = new BinaryRowDataSerializer(2);
        probeSer$229 = new BinaryRowDataSerializer(5);
        buildToBinaryRow = new Projection$230();
        probeToBinaryRow = new Projection$233();
        condFunc = new ConditionFunction$198();

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
                new LongHashTable$227(
                        parameters, memorySize, runtimeContext.getNumberOfParallelSubtasks());
        this.output = output;
        LOG.info("Initializing warehouse table join operator successfully.");
    }

    public void processBuild(org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
            throws Exception {
        org.apache.flink.table.data.RowData row =
                (org.apache.flink.table.data.RowData) element.getValue();

        boolean anyNull$245 = false;
        anyNull$245 |= row.isNullAt(0);

        if (!anyNull$245) {
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

        boolean anyNull$246 = false;
        anyNull$246 |= row.isNullAt(1);

        if (!anyNull$246) {
            if (table.tryProbe(row)) {
                joinWithNextKey();
            }
        }
    }

    private void endInput1() throws Exception {
        LOG.info("Finish build phase.");
        table.endBuild();
        buildEnd$247 = true;
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
        LOG.info("Close warehouse hashtable successfully.");
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
                // call collect output join result to downstream operator
                output.collect(outElement.replace(joinedRow.replace(probeRow, buildIter.getRow())));
            }
        }
    }

    public class Projection$233
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
            long field$234;
            boolean isNull$234;
            long field$235;
            boolean isNull$235;
            long field$236;
            boolean isNull$236;
            org.apache.flink.table.data.binary.BinaryStringData field$237;
            boolean isNull$237;
            org.apache.flink.table.data.binary.BinaryStringData field$238;
            boolean isNull$238;

            outWriter.reset();

            isNull$234 = in1.isNullAt(0);
            field$234 = -1L;
            if (!isNull$234) {
                field$234 = in1.getLong(0);
            }
            if (isNull$234) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$234);
            }

            isNull$235 = in1.isNullAt(1);
            field$235 = -1L;
            if (!isNull$235) {
                field$235 = in1.getLong(1);
            }
            if (isNull$235) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$235);
            }

            isNull$236 = in1.isNullAt(2);
            field$236 = -1L;
            if (!isNull$236) {
                field$236 = in1.getLong(2);
            }
            if (isNull$236) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeLong(2, field$236);
            }

            isNull$237 = in1.isNullAt(3);
            field$237 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
            if (!isNull$237) {
                field$237 =
                        ((org.apache.flink.table.data.binary.BinaryStringData) in1.getString(3));
            }
            if (isNull$237) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeString(3, field$237);
            }

            isNull$238 = in1.isNullAt(4);
            field$238 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
            if (!isNull$238) {
                field$238 =
                        ((org.apache.flink.table.data.binary.BinaryStringData) in1.getString(4));
            }
            if (isNull$238) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeString(4, field$238);
            }

            outWriter.complete();

            return out;
        }
    }

    public class ConditionFunction$198
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

    public class Projection$230
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
            long field$231;
            boolean isNull$231;
            org.apache.flink.table.data.binary.BinaryStringData field$232;
            boolean isNull$232;

            outWriter.reset();

            isNull$231 = in1.isNullAt(0);
            field$231 = -1L;
            if (!isNull$231) {
                field$231 = in1.getLong(0);
            }
            if (isNull$231) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$231);
            }

            isNull$232 = in1.isNullAt(1);
            field$232 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
            if (!isNull$232) {
                field$232 =
                        ((org.apache.flink.table.data.binary.BinaryStringData) in1.getString(1));
            }
            if (isNull$232) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeString(1, field$232);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$227
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$227(
                StreamOperatorParameters<RowData> parameters, long memorySize, long parallelTasks) {
            super(
                    parameters.getContainingTask(),
                    ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED.defaultValue(),
                    (int)
                            ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE
                                    .defaultValue()
                                    .getBytes(),
                    buildSer$228,
                    probeSer$229,
                    parameters.getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    parameters.getContainingTask().getEnvironment().getIOManager(),
                    23,
                    25L / parallelTasks);
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
}
