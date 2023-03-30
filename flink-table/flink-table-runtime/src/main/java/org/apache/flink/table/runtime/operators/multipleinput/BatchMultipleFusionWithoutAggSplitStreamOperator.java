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

package org.apache.flink.table.runtime.operators.multipleinput;

import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.columnar.ColumnarRowData;
import org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler;
import org.apache.flink.table.runtime.operators.multipleinput.input.InputSpec;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.RowIterator;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** A code split {@link MultipleInputStreamOperatorBase} to handle batch operators. */
public final class BatchMultipleFusionWithoutAggSplitStreamOperator
        extends AbstractStreamOperatorV2<RowData>
        implements MultipleInputStreamOperator<RowData>, BoundedMultiInput, InputSelectable {

    org.apache.flink.table.data.binary.BinaryStringData field$240;
    boolean isNull$240;
    org.apache.flink.table.data.binary.BinaryStringData field$242;
    boolean isNull$243;
    org.apache.flink.table.data.binary.BinaryStringData result$244;
    org.apache.flink.table.data.binary.BinaryStringData field$245;
    boolean isNull$245;
    org.apache.flink.table.data.binary.BinaryStringData field$246;
    org.apache.flink.table.data.binary.BinaryStringData field$247;
    boolean isNull$247;
    org.apache.flink.table.data.binary.BinaryStringData field$248;
    long field$249;
    boolean isNull$249;
    long field$250;
    boolean isNull$250;
    boolean isNull$251;
    long result$252;
    boolean isNull$253;
    boolean result$254;
    boolean isNull$256;
    long result$257;
    boolean isNull$258;
    boolean result$259;
    boolean isNull$260;
    long result$261;
    boolean isNull$262;
    boolean result$263;
    boolean isNull$266;
    long result$267;
    boolean isNull$268;
    boolean result$269;
    boolean isNull$270;
    long result$271;
    boolean isNull$272;
    boolean result$273;
    boolean isNull$276;
    long result$277;
    boolean isNull$278;
    boolean result$279;
    boolean isNull$280;
    long result$281;
    boolean isNull$282;
    boolean result$283;
    boolean isNull$286;
    long result$287;
    boolean isNull$288;
    boolean result$289;
    int result$255;
    boolean isNull$255;
    boolean result$264;
    boolean isNull$264;
    int result$265;
    boolean isNull$265;
    boolean result$274;
    boolean isNull$274;
    int result$275;
    boolean isNull$275;
    boolean result$284;
    boolean isNull$284;
    int result$285;
    boolean isNull$285;
    int result$290;
    boolean isNull$290;

    private static final long serialVersionUID = 1L;

    private final StreamOperatorParameters<RowData> parameters;

    private final InputSelectionHandler inputSelectionHandler;

    // Join date_dim related info
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$44;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$45;
    private transient Projection$46 dateDimBuildToBinaryRow;
    private transient Projection$48 dateDimProbeToBinaryRow;
    private transient LongHashTable$43 dateDimHashTable;

    // Join call_center related info
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$100;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$101;
    private transient Projection$102 callCenterBuildToBinaryRow;
    private transient Projection$105 callCenterProbeToBinaryRow;
    private transient LongHashTable$99 callCenterHashTable;

    // Join ship_mode related info
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$159;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$160;
    private transient Projection$161 shipModeBuildToBinaryRow;
    private transient Projection$164 shipModeProbeToBinaryRow;
    private transient LongHashTable$158 shipModeHashTable;

    // Join warehouse related info
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$219;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$220;
    private transient Projection$221 warehouseBuildToBinaryRow;
    private transient Projection$224 warehouseProbeToBinaryRow;
    private transient LongHashTable$218 warehouseHashTable;

    org.apache.flink.table.data.BoxedWrapperRowData out =
            new org.apache.flink.table.data.BoxedWrapperRowData(8);
    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);

    public BatchMultipleFusionWithoutAggSplitStreamOperator(
            StreamOperatorParameters<RowData> parameters, List<InputSpec> inputSpecs) {
        super(parameters, inputSpecs.size());
        this.parameters = parameters;
        this.inputSelectionHandler =
                new InputSelectionHandler(
                        inputSpecs.stream()
                                .map(InputSpec::getMultipleInputSpec)
                                .collect(Collectors.toList()));
    }

    @Override
    public void open() throws Exception {
        super.open();
        long memorySize = computeMemorySize(parameters);
        long hashTableMemorySize = (long) (memorySize * 0.25);

        // initialize date_dim table
        buildSer$44 = new BinaryRowDataSerializer(1);
        probeSer$45 = new BinaryRowDataSerializer(5);
        dateDimBuildToBinaryRow = new Projection$46();
        dateDimProbeToBinaryRow = new Projection$48();
        dateDimHashTable = new LongHashTable$43(parameters, hashTableMemorySize);

        // initialize call_center table
        buildSer$100 = new BinaryRowDataSerializer(2);
        probeSer$101 = new BinaryRowDataSerializer(5);
        callCenterBuildToBinaryRow = new Projection$102();
        callCenterProbeToBinaryRow = new Projection$105();
        callCenterHashTable = new LongHashTable$99(parameters, hashTableMemorySize);

        // initialize ship_mode table
        buildSer$159 = new BinaryRowDataSerializer(2);
        probeSer$160 = new BinaryRowDataSerializer(5);
        shipModeBuildToBinaryRow = new Projection$161();
        shipModeProbeToBinaryRow = new Projection$164();
        shipModeHashTable = new LongHashTable$158(parameters, hashTableMemorySize);

        // initialize warehouse table
        buildSer$219 = new BinaryRowDataSerializer(2);
        probeSer$220 = new BinaryRowDataSerializer(5);
        warehouseBuildToBinaryRow = new Projection$221();
        warehouseProbeToBinaryRow = new Projection$224();
        warehouseHashTable = new LongHashTable$218(parameters, hashTableMemorySize);

        LOG.info(
                "Open all join operator in multiple node successfully, operator memory size is {}.",
                memorySize);
    }

    @Override
    public List<Input> getInputs() {
        return Arrays.asList(
                new AbstractInput(this, 1) {
                    @Override
                    public void processElement(StreamRecord element) throws Exception {
                        processCatalogSalesJoin(element);
                    }
                },
                new AbstractInput(this, 2) {
                    @Override
                    public void processElement(StreamRecord element) throws Exception {
                        org.apache.flink.table.data.RowData row =
                                (org.apache.flink.table.data.RowData) element.getValue();

                        boolean anyNull$60 = false;
                        anyNull$60 |= row.isNullAt(0);
                        if (!anyNull$60) {
                            dateDimHashTable.putBuildRow(
                                    row instanceof org.apache.flink.table.data.binary.BinaryRowData
                                            ? (org.apache.flink.table.data.binary.BinaryRowData) row
                                            : dateDimBuildToBinaryRow.apply(row));
                        }
                    }
                },
                new AbstractInput(this, 3) {
                    @Override
                    public void processElement(StreamRecord element) throws Exception {
                        org.apache.flink.table.data.RowData row =
                                (org.apache.flink.table.data.RowData) element.getValue();

                        boolean anyNull$117 = false;
                        anyNull$117 |= row.isNullAt(0);
                        if (!anyNull$117) {
                            callCenterHashTable.putBuildRow(
                                    row instanceof org.apache.flink.table.data.binary.BinaryRowData
                                            ? (org.apache.flink.table.data.binary.BinaryRowData) row
                                            : callCenterBuildToBinaryRow.apply(row));
                        }
                    }
                },
                new AbstractInput(this, 4) {
                    @Override
                    public void processElement(StreamRecord element) throws Exception {
                        org.apache.flink.table.data.RowData row =
                                (org.apache.flink.table.data.RowData) element.getValue();

                        boolean anyNull$176 = false;
                        anyNull$176 |= row.isNullAt(0);
                        if (!anyNull$176) {
                            shipModeHashTable.putBuildRow(
                                    row instanceof org.apache.flink.table.data.binary.BinaryRowData
                                            ? (org.apache.flink.table.data.binary.BinaryRowData) row
                                            : shipModeBuildToBinaryRow.apply(row));
                        }
                    }
                },
                new AbstractInput(this, 5) {
                    @Override
                    public void processElement(StreamRecord element) throws Exception {
                        org.apache.flink.table.data.RowData row =
                                (org.apache.flink.table.data.RowData) element.getValue();

                        boolean anyNull$236 = false;
                        anyNull$236 |= row.isNullAt(0);
                        if (!anyNull$236) {
                            warehouseHashTable.putBuildRow(
                                    row instanceof org.apache.flink.table.data.binary.BinaryRowData
                                            ? (org.apache.flink.table.data.binary.BinaryRowData) row
                                            : warehouseBuildToBinaryRow.apply(row));
                        }
                    }
                });
    }

    @Override
    public void endInput(int inputId) throws Exception {
        inputSelectionHandler.endInput(inputId);
        switch (inputId) {
            case 1:
                LOG.info("Ending catalog_sales probe and local hash agg.");
                break;
            case 2:
                dateDimHashTable.endBuild();
                LOG.info("Ending date_dim build.");
                break;
            case 3:
                callCenterHashTable.endBuild();
                LOG.info("Ending call_center build.");
                break;
            case 4:
                shipModeHashTable.endBuild();
                LOG.info("Ending ship_mode build.");
                break;
            case 5:
                warehouseHashTable.endBuild();
                LOG.info("Ending warehouse build.");
                break;
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (this.dateDimHashTable != null) {
            this.dateDimHashTable.close();
            this.dateDimHashTable.free();
            this.dateDimHashTable = null;
        }

        if (this.callCenterHashTable != null) {
            this.callCenterHashTable.close();
            this.callCenterHashTable.free();
            this.callCenterHashTable = null;
        }

        if (this.shipModeHashTable != null) {
            this.shipModeHashTable.close();
            this.shipModeHashTable.free();
            this.shipModeHashTable = null;
        }

        if (this.warehouseHashTable != null) {
            this.warehouseHashTable.close();
            this.warehouseHashTable.free();
            this.warehouseHashTable = null;
        }
        LOG.info("Close all BHJ hash table.");
    }

    @Override
    public InputSelection nextSelection() {
        return inputSelectionHandler.getInputSelection();
    }

    /** Compute memory size from memory faction. */
    private long computeMemorySize(StreamOperatorParameters<RowData> parameters) {
        final Environment environment = parameters.getContainingTask().getEnvironment();
        return environment
                .getMemoryManager()
                .computeMemorySize(
                        getOperatorConfig()
                                .getManagedMemoryFractionOperatorUseCaseOfSlot(
                                        ManagedMemoryUseCase.OPERATOR,
                                        environment.getTaskManagerInfo().getConfiguration(),
                                        environment.getUserCodeClassLoader().asClassLoader()));
    }

    private void processCatalogSalesJoin(
            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element) throws Exception {
        ColumnarRowData row = (ColumnarRowData) element.getValue();
        ColumnarRowData columnarRowData = (ColumnarRowData) element.getValue();
        int numRows = columnarRowData.getNumRows();
        for (int i = 0; i < numRows; i++) {
            // set rowId in ColumnarRowData
            columnarRowData.setRowId(i);

            // join date_dim, get cs_ship_date_sk from index 0
            boolean isNullShipDateSk = row.isNullAt(0);
            if (isNullShipDateSk) {
                continue;
            }
            long shipDateSk = row.getLong(0);

            // join call_center, get cs_call_center_sk from index 1
            boolean isNullCallCenterSk = row.isNullAt(1);
            if (isNullCallCenterSk) {
                continue;
            }
            long callCenterSk = row.getLong(1);

            // join ship_mode, get cs_ship_mode_sk from index 2
            boolean isNullShipModeSk = row.isNullAt(2);
            if (isNullShipModeSk) {
                continue;
            }
            long shipModeSk = row.getLong(2);

            // join warehouse, get cs_warehouse_sk from index 3
            boolean isNullWarehouseSk = row.isNullAt(3);
            if (isNullWarehouseSk) {
                continue;
            }
            long warehouseSk = row.getLong(3);

            // join date_dim first
            RowIterator<BinaryRowData> dateDimIter = dateDimHashTable.get(shipDateSk);
            while (dateDimIter.advanceNext()) {
                // join call_center
                RowIterator<BinaryRowData> callCenterIter = callCenterHashTable.get(callCenterSk);
                RowData callCenterBuildRow = callCenterIter.getRow();
                while (callCenterIter.advanceNext()) {
                    // join ship_mode
                    RowIterator<BinaryRowData> shipModeIter = shipModeHashTable.get(shipModeSk);
                    RowData shipModeBuildRow = shipModeIter.getRow();
                    while (shipModeIter.advanceNext()) {
                        // join warehouse
                        RowIterator<BinaryRowData> warehouseIter =
                                warehouseHashTable.get(warehouseSk);
                        while (warehouseIter.advanceNext()) {
                            RowData warehouseBuildRow = warehouseIter.getRow();
                            // cs_sold_date_sk, get from catalog_sales
                            boolean isNullSoldDateSk = row.isNullAt(4);
                            long soldDateSk = -1L;
                            if (!isNullSoldDateSk) {
                                soldDateSk = row.getLong(4);
                            }

                            // cc_name, get from call_center
                            boolean isNullCcName = callCenterBuildRow.isNullAt(1);
                            BinaryStringData ccName = BinaryStringData.EMPTY_UTF8;
                            if (!isNullCcName) {
                                ccName = (BinaryStringData) callCenterBuildRow.getString(1);
                            }

                            // sm_type, get from ship_mode
                            boolean isNullSmType = shipModeBuildRow.isNullAt(1);
                            BinaryStringData smType = BinaryStringData.EMPTY_UTF8;
                            if (!isNullSmType) {
                                smType = (BinaryStringData) shipModeBuildRow.getString(1);
                            }

                            // w_warehouse_name, get from warehouse table
                            boolean isNullWarehouseName = warehouseBuildRow.isNullAt(1);
                            BinaryStringData warehouseName = BinaryStringData.EMPTY_UTF8;
                            if (!isNullWarehouseName) {
                                warehouseName = (BinaryStringData) warehouseBuildRow.getString(1);
                            }

                            // process case when expression
                            processCaseWhenExpr(
                                    isNullShipDateSk,
                                    shipDateSk,
                                    isNullSoldDateSk,
                                    soldDateSk,
                                    isNullCcName,
                                    ccName,
                                    isNullSmType,
                                    smType,
                                    isNullWarehouseName,
                                    warehouseName);
                        }
                    }
                }
            }
        }
    }

    /**
     * This method just need five fields, process the case when expression.
     *
     * @param isNullShipDateSk
     * @param shipDateSk
     * @param isNullSoldDateSk
     * @param soldDateSk
     * @param isNullCcName
     * @param ccName
     * @param isNullSmType
     * @param smType
     * @param isNullWarehouseName
     * @param warehouseName
     * @throws Exception
     */
    public void processCaseWhenExpr(
            boolean isNullShipDateSk,
            long shipDateSk,
            boolean isNullSoldDateSk,
            long soldDateSk,
            boolean isNullCcName,
            BinaryStringData ccName,
            boolean isNullSmType,
            BinaryStringData smType,
            boolean isNullWarehouseName,
            BinaryStringData warehouseName)
            throws Exception {

        processCaseWhenExpr_split37(
                isNullShipDateSk,
                shipDateSk,
                isNullSoldDateSk,
                soldDateSk,
                isNullCcName,
                ccName,
                isNullSmType,
                smType,
                isNullWarehouseName,
                warehouseName);

        processCaseWhenExpr_split38(
                isNullShipDateSk,
                shipDateSk,
                isNullSoldDateSk,
                soldDateSk,
                isNullCcName,
                ccName,
                isNullSmType,
                smType,
                isNullWarehouseName,
                warehouseName);

        processCaseWhenExpr_split39(
                isNullShipDateSk,
                shipDateSk,
                isNullSoldDateSk,
                soldDateSk,
                isNullCcName,
                ccName,
                isNullSmType,
                smType,
                isNullWarehouseName,
                warehouseName);
    }

    void processCaseWhenExpr_split37(
            boolean isNullShipDateSk,
            long shipDateSk,
            boolean isNullSoldDateSk,
            long soldDateSk,
            boolean isNullCcName,
            BinaryStringData ccName,
            boolean isNullSmType,
            BinaryStringData smType,
            boolean isNullWarehouseName,
            BinaryStringData warehouseName)
            throws Exception {

        isNull$249 = isNullShipDateSk;
        field$249 = shipDateSk;
        isNull$250 = isNullSoldDateSk;
        field$250 = soldDateSk;
        isNull$240 = isNullWarehouseName;
        field$240 = warehouseName;
        field$242 = field$240;
        isNull$245 = isNullSmType;
        field$245 = smType;
        field$246 = field$245;
        isNull$247 = isNullCcName;
        field$247 = ccName;
        field$248 = field$247;
        isNull$243 = isNull$240 || false || false;
        result$244 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
        if (!isNull$243) {
            result$244 =
                    org.apache.flink.table.data.binary.BinaryStringDataUtil.substringSQL(
                            field$242, ((int) 1), ((int) 20));

            isNull$243 = (result$244 == null);
        }
        isNull$251 = isNull$249 || isNull$250;
        result$252 = -1L;
        if (!isNull$251) {

            result$252 = (long) (field$249 - field$250);
        }
        isNull$253 = isNull$251 || false;
        result$254 = false;
        if (!isNull$253) {

            result$254 = result$252 <= ((int) 30);
        }
        result$255 = -1;
        if (result$254) {

            // --- Cast section generated by
            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

            // --- End cast section

            isNull$255 = false;
            if (!isNull$255) {
                result$255 = ((int) 1);
            }
        } else {

            // --- Cast section generated by
            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

            // --- End cast section

            isNull$255 = false;
            if (!isNull$255) {
                result$255 = ((int) 0);
            }
        }
        isNull$256 = isNull$249 || isNull$250;
        result$257 = -1L;
        if (!isNull$256) {

            result$257 = (long) (field$249 - field$250);
        }
        isNull$258 = isNull$256 || false;
        result$259 = false;
        if (!isNull$258) {

            result$259 = result$257 > ((int) 30);
        }
        result$264 = false;
        isNull$264 = false;
        if (!isNull$258 && !result$259) {
            // left expr is false, skip right expr
        } else {

            isNull$260 = isNull$249 || isNull$250;
            result$261 = -1L;
            if (!isNull$260) {

                result$261 = (long) (field$249 - field$250);
            }

            isNull$262 = isNull$260 || false;
            result$263 = false;
            if (!isNull$262) {

                result$263 = result$261 <= ((int) 60);
            }

            if (!isNull$258 && !isNull$262) {
                result$264 = result$259 && result$263;
                isNull$264 = false;
            } else if (!isNull$258 && result$259 && isNull$262) {
                result$264 = false;
                isNull$264 = true;
            } else if (!isNull$258 && !result$259 && isNull$262) {
                result$264 = false;
                isNull$264 = false;
            } else if (isNull$258 && !isNull$262 && result$263) {
                result$264 = false;
                isNull$264 = true;
            } else if (isNull$258 && !isNull$262 && !result$263) {
                result$264 = false;
                isNull$264 = false;
            } else {
                result$264 = false;
                isNull$264 = true;
            }
        }
        result$265 = -1;
        if (result$264) {

            // --- Cast section generated by
            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

            // --- End cast section

            isNull$265 = false;
            if (!isNull$265) {
                result$265 = ((int) 1);
            }
        } else {

            // --- Cast section generated by
            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

            // --- End cast section

            isNull$265 = false;
            if (!isNull$265) {
                result$265 = ((int) 0);
            }
        }
        isNull$266 = isNull$249 || isNull$250;
        result$267 = -1L;
        if (!isNull$266) {

            result$267 = (long) (field$249 - field$250);
        }
    }

    void processCaseWhenExpr_split38(
            boolean isNullShipDateSk,
            long shipDateSk,
            boolean isNullSoldDateSk,
            long soldDateSk,
            boolean isNullCcName,
            BinaryStringData ccName,
            boolean isNullSmType,
            BinaryStringData smType,
            boolean isNullWarehouseName,
            BinaryStringData warehouseName)
            throws Exception {
        isNull$268 = isNull$266 || false;
        result$269 = false;
        if (!isNull$268) {

            result$269 = result$267 > ((int) 60);
        }
        result$274 = false;
        isNull$274 = false;
        if (!isNull$268 && !result$269) {
            // left expr is false, skip right expr
        } else {

            isNull$270 = isNull$249 || isNull$250;
            result$271 = -1L;
            if (!isNull$270) {

                result$271 = (long) (field$249 - field$250);
            }

            isNull$272 = isNull$270 || false;
            result$273 = false;
            if (!isNull$272) {

                result$273 = result$271 <= ((int) 90);
            }

            if (!isNull$268 && !isNull$272) {
                result$274 = result$269 && result$273;
                isNull$274 = false;
            } else if (!isNull$268 && result$269 && isNull$272) {
                result$274 = false;
                isNull$274 = true;
            } else if (!isNull$268 && !result$269 && isNull$272) {
                result$274 = false;
                isNull$274 = false;
            } else if (isNull$268 && !isNull$272 && result$273) {
                result$274 = false;
                isNull$274 = true;
            } else if (isNull$268 && !isNull$272 && !result$273) {
                result$274 = false;
                isNull$274 = false;
            } else {
                result$274 = false;
                isNull$274 = true;
            }
        }
        result$275 = -1;
        if (result$274) {

            // --- Cast section generated by
            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

            // --- End cast section

            isNull$275 = false;
            if (!isNull$275) {
                result$275 = ((int) 1);
            }
        } else {

            // --- Cast section generated by
            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

            // --- End cast section

            isNull$275 = false;
            if (!isNull$275) {
                result$275 = ((int) 0);
            }
        }
        isNull$276 = isNull$249 || isNull$250;
        result$277 = -1L;
        if (!isNull$276) {

            result$277 = (long) (field$249 - field$250);
        }
        isNull$278 = isNull$276 || false;
        result$279 = false;
        if (!isNull$278) {

            result$279 = result$277 > ((int) 90);
        }
        result$284 = false;
        isNull$284 = false;
        if (!isNull$278 && !result$279) {
            // left expr is false, skip right expr
        } else {

            isNull$280 = isNull$249 || isNull$250;
            result$281 = -1L;
            if (!isNull$280) {

                result$281 = (long) (field$249 - field$250);
            }

            isNull$282 = isNull$280 || false;
            result$283 = false;
            if (!isNull$282) {
                result$283 = result$281 <= ((int) 120);
            }

            if (!isNull$278 && !isNull$282) {
                result$284 = result$279 && result$283;
                isNull$284 = false;
            } else if (!isNull$278 && result$279 && isNull$282) {
                result$284 = false;
                isNull$284 = true;
            } else if (!isNull$278 && !result$279 && isNull$282) {
                result$284 = false;
                isNull$284 = false;
            } else if (isNull$278 && !isNull$282 && result$283) {
                result$284 = false;
                isNull$284 = true;
            } else if (isNull$278 && !isNull$282 && !result$283) {
                result$284 = false;
                isNull$284 = false;
            } else {
                result$284 = false;
                isNull$284 = true;
            }
        }
        result$285 = -1;
    }

    void processCaseWhenExpr_split39(
            boolean isNullShipDateSk,
            long shipDateSk,
            boolean isNullSoldDateSk,
            long soldDateSk,
            boolean isNullCcName,
            BinaryStringData ccName,
            boolean isNullSmType,
            BinaryStringData smType,
            boolean isNullWarehouseName,
            BinaryStringData warehouseName)
            throws Exception {
        if (result$284) {

            // --- Cast section generated by
            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

            // --- End cast section

            isNull$285 = false;
            if (!isNull$285) {
                result$285 = ((int) 1);
            }
        } else {

            // --- Cast section generated by
            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

            // --- End cast section

            isNull$285 = false;
            if (!isNull$285) {
                result$285 = ((int) 0);
            }
        }
        isNull$286 = isNull$249 || isNull$250;
        result$287 = -1L;
        if (!isNull$286) {

            result$287 = (long) (field$249 - field$250);
        }
        isNull$288 = isNull$286 || false;
        result$289 = false;
        if (!isNull$288) {

            result$289 = result$287 > ((int) 120);
        }
        result$290 = -1;
        if (result$289) {

            // --- Cast section generated by
            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

            // --- End cast section

            isNull$290 = false;
            if (!isNull$290) {
                result$290 = ((int) 1);
            }
        } else {

            // --- Cast section generated by
            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

            // --- End cast section

            isNull$290 = false;
            if (!isNull$290) {
                result$290 = ((int) 0);
            }
        }

        if (isNull$243) {
            out.setNullAt(0);
        } else {
            out.setNonPrimitiveValue(0, result$244);
        }
        if (isNull$245) {
            out.setNullAt(1);
        } else {
            out.setNonPrimitiveValue(1, field$246);
        }
        if (isNull$247) {
            out.setNullAt(2);
        } else {
            out.setNonPrimitiveValue(2, field$248);
        }
        out.setInt(3, result$255);
        out.setInt(4, result$265);
        out.setInt(5, result$275);
        out.setInt(6, result$285);
        out.setInt(7, result$290);
        // output element
        output.collect(outElement.replace(out));
    }

    /** Join date_dim table. */
    public class LongHashTable$43
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$43(StreamOperatorParameters<RowData> parameters, long memorySize) {
            super(
                    parameters.getContainingTask(),
                    true,
                    65536,
                    buildSer$44,
                    probeSer$45,
                    parameters.getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    parameters.getContainingTask().getEnvironment().getIOManager(),
                    8,
                    334L / getRuntimeContext().getNumberOfParallelSubtasks());
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
                return dateDimProbeToBinaryRow.apply(row);
            }
        }
    }

    public class Projection$46
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
            long field$47;
            boolean isNull$47;

            outWriter.reset();

            isNull$47 = in1.isNullAt(0);
            field$47 = -1L;
            if (!isNull$47) {
                field$47 = in1.getLong(0);
            }
            if (isNull$47) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$47);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$48
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
            long field$49;
            boolean isNull$49;
            long field$50;
            boolean isNull$50;
            long field$51;
            boolean isNull$51;
            long field$52;
            boolean isNull$52;
            long field$53;
            boolean isNull$53;

            outWriter.reset();

            isNull$49 = in1.isNullAt(0);
            field$49 = -1L;
            if (!isNull$49) {
                field$49 = in1.getLong(0);
            }
            if (isNull$49) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$49);
            }

            isNull$50 = in1.isNullAt(1);
            field$50 = -1L;
            if (!isNull$50) {
                field$50 = in1.getLong(1);
            }
            if (isNull$50) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$50);
            }

            isNull$51 = in1.isNullAt(2);
            field$51 = -1L;
            if (!isNull$51) {
                field$51 = in1.getLong(2);
            }
            if (isNull$51) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeLong(2, field$51);
            }

            isNull$52 = in1.isNullAt(3);
            field$52 = -1L;
            if (!isNull$52) {
                field$52 = in1.getLong(3);
            }
            if (isNull$52) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeLong(3, field$52);
            }

            isNull$53 = in1.isNullAt(4);
            field$53 = -1L;
            if (!isNull$53) {
                field$53 = in1.getLong(4);
            }
            if (isNull$53) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeLong(4, field$53);
            }

            outWriter.complete();

            return out;
        }
    }

    /** join call_center table. */
    public class LongHashTable$99
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$99(StreamOperatorParameters<RowData> parameters, long memorySize) {
            super(
                    parameters.getContainingTask(),
                    true,
                    65536,
                    buildSer$100,
                    probeSer$101,
                    parameters.getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    parameters.getContainingTask().getEnvironment().getIOManager(),
                    21,
                    54L / getRuntimeContext().getNumberOfParallelSubtasks());
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
                return callCenterProbeToBinaryRow.apply(row);
            }
        }
    }

    public class Projection$105
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
            long field$106;
            boolean isNull$106;
            long field$107;
            boolean isNull$107;
            long field$108;
            boolean isNull$108;
            long field$109;
            boolean isNull$109;
            long field$110;
            boolean isNull$110;

            outWriter.reset();

            isNull$106 = in1.isNullAt(0);
            field$106 = -1L;
            if (!isNull$106) {
                field$106 = in1.getLong(0);
            }
            if (isNull$106) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$106);
            }

            isNull$107 = in1.isNullAt(1);
            field$107 = -1L;
            if (!isNull$107) {
                field$107 = in1.getLong(1);
            }
            if (isNull$107) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$107);
            }

            isNull$108 = in1.isNullAt(2);
            field$108 = -1L;
            if (!isNull$108) {
                field$108 = in1.getLong(2);
            }
            if (isNull$108) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeLong(2, field$108);
            }

            isNull$109 = in1.isNullAt(3);
            field$109 = -1L;
            if (!isNull$109) {
                field$109 = in1.getLong(3);
            }
            if (isNull$109) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeLong(3, field$109);
            }

            isNull$110 = in1.isNullAt(4);
            field$110 = -1L;
            if (!isNull$110) {
                field$110 = in1.getLong(4);
            }
            if (isNull$110) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeLong(4, field$110);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$102
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
            long field$103;
            boolean isNull$103;
            org.apache.flink.table.data.binary.BinaryStringData field$104;
            boolean isNull$104;

            outWriter.reset();

            isNull$103 = in1.isNullAt(0);
            field$103 = -1L;
            if (!isNull$103) {
                field$103 = in1.getLong(0);
            }
            if (isNull$103) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$103);
            }

            isNull$104 = in1.isNullAt(1);
            field$104 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
            if (!isNull$104) {
                field$104 =
                        ((org.apache.flink.table.data.binary.BinaryStringData) in1.getString(1));
            }
            if (isNull$104) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeString(1, field$104);
            }

            outWriter.complete();

            return out;
        }
    }

    /** Join ship_mode table. */
    public class LongHashTable$158
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$158(StreamOperatorParameters<RowData> parameters, long memorySize) {
            super(
                    parameters.getContainingTask(),
                    true,
                    65536,
                    buildSer$159,
                    probeSer$160,
                    parameters.getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    parameters.getContainingTask().getEnvironment().getIOManager(),
                    15,
                    20L / getRuntimeContext().getNumberOfParallelSubtasks());
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
                return shipModeProbeToBinaryRow.apply(row);
            }
        }
    }

    public class Projection$161
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
            long field$162;
            boolean isNull$162;
            org.apache.flink.table.data.binary.BinaryStringData field$163;
            boolean isNull$163;

            outWriter.reset();

            isNull$162 = in1.isNullAt(0);
            field$162 = -1L;
            if (!isNull$162) {
                field$162 = in1.getLong(0);
            }
            if (isNull$162) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$162);
            }

            isNull$163 = in1.isNullAt(1);
            field$163 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
            if (!isNull$163) {
                field$163 =
                        ((org.apache.flink.table.data.binary.BinaryStringData) in1.getString(1));
            }
            if (isNull$163) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeString(1, field$163);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$164
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
            long field$165;
            boolean isNull$165;
            long field$166;
            boolean isNull$166;
            long field$167;
            boolean isNull$167;
            long field$168;
            boolean isNull$168;
            org.apache.flink.table.data.binary.BinaryStringData field$169;
            boolean isNull$169;

            outWriter.reset();

            isNull$165 = in1.isNullAt(0);
            field$165 = -1L;
            if (!isNull$165) {
                field$165 = in1.getLong(0);
            }
            if (isNull$165) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$165);
            }

            isNull$166 = in1.isNullAt(1);
            field$166 = -1L;
            if (!isNull$166) {
                field$166 = in1.getLong(1);
            }
            if (isNull$166) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$166);
            }

            isNull$167 = in1.isNullAt(2);
            field$167 = -1L;
            if (!isNull$167) {
                field$167 = in1.getLong(2);
            }
            if (isNull$167) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeLong(2, field$167);
            }

            isNull$168 = in1.isNullAt(3);
            field$168 = -1L;
            if (!isNull$168) {
                field$168 = in1.getLong(3);
            }
            if (isNull$168) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeLong(3, field$168);
            }

            isNull$169 = in1.isNullAt(4);
            field$169 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
            if (!isNull$169) {
                field$169 =
                        ((org.apache.flink.table.data.binary.BinaryStringData) in1.getString(4));
            }
            if (isNull$169) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeString(4, field$169);
            }

            outWriter.complete();

            return out;
        }
    }

    /** Join warehouse table. */
    public class LongHashTable$218
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$218(StreamOperatorParameters<RowData> parameters, long memorySize) {
            super(
                    parameters.getContainingTask(),
                    true,
                    65536,
                    buildSer$219,
                    probeSer$220,
                    parameters.getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    parameters.getContainingTask().getEnvironment().getIOManager(),
                    23,
                    25L / getRuntimeContext().getNumberOfParallelSubtasks());
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
                return warehouseProbeToBinaryRow.apply(row);
            }
        }
    }

    public class Projection$221
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
            long field$222;
            boolean isNull$222;
            org.apache.flink.table.data.binary.BinaryStringData field$223;
            boolean isNull$223;

            outWriter.reset();

            isNull$222 = in1.isNullAt(0);
            field$222 = -1L;
            if (!isNull$222) {
                field$222 = in1.getLong(0);
            }
            if (isNull$222) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$222);
            }

            isNull$223 = in1.isNullAt(1);
            field$223 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
            if (!isNull$223) {
                field$223 =
                        ((org.apache.flink.table.data.binary.BinaryStringData) in1.getString(1));
            }
            if (isNull$223) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeString(1, field$223);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$224
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
            long field$225;
            boolean isNull$225;
            long field$226;
            boolean isNull$226;
            long field$227;
            boolean isNull$227;
            org.apache.flink.table.data.binary.BinaryStringData field$228;
            boolean isNull$228;
            org.apache.flink.table.data.binary.BinaryStringData field$229;
            boolean isNull$229;

            outWriter.reset();

            isNull$225 = in1.isNullAt(0);
            field$225 = -1L;
            if (!isNull$225) {
                field$225 = in1.getLong(0);
            }
            if (isNull$225) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$225);
            }

            isNull$226 = in1.isNullAt(1);
            field$226 = -1L;
            if (!isNull$226) {
                field$226 = in1.getLong(1);
            }
            if (isNull$226) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$226);
            }

            isNull$227 = in1.isNullAt(2);
            field$227 = -1L;
            if (!isNull$227) {
                field$227 = in1.getLong(2);
            }
            if (isNull$227) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeLong(2, field$227);
            }

            isNull$228 = in1.isNullAt(3);
            field$228 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
            if (!isNull$228) {
                field$228 =
                        ((org.apache.flink.table.data.binary.BinaryStringData) in1.getString(3));
            }
            if (isNull$228) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeString(3, field$228);
            }

            isNull$229 = in1.isNullAt(4);
            field$229 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
            if (!isNull$229) {
                field$229 =
                        ((org.apache.flink.table.data.binary.BinaryStringData) in1.getString(4));
            }
            if (isNull$229) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeString(4, field$229);
            }

            outWriter.complete();

            return out;
        }
    }
}
