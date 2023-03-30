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
import org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler;
import org.apache.flink.table.runtime.operators.multipleinput.input.InputSpec;
import org.apache.flink.table.runtime.operators.multipleinput.join.CallCenterHashJoinOperator;
import org.apache.flink.table.runtime.operators.multipleinput.join.DateDimHashJoinOperator;
import org.apache.flink.table.runtime.operators.multipleinput.join.ShipModeHashJoinOperator;
import org.apache.flink.table.runtime.operators.multipleinput.join.WarehouseHashJoinOperator;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** A {@link MultipleInputStreamOperatorBase} to handle batch operators. */
public final class BatchBHJMultipleInputStreamOperator extends AbstractStreamOperatorV2<RowData>
        implements MultipleInputStreamOperator<RowData>, BoundedMultiInput, InputSelectable {

    private static final long serialVersionUID = 1L;

    private final StreamOperatorParameters<RowData> parameters;

    private final InputSelectionHandler inputSelectionHandler;

    private DateDimHashJoinOperator dateDimHashJoinOperator;
    private CallCenterHashJoinOperator callCenterHashJoinOperator;
    private ShipModeHashJoinOperator shipModeHashJoinOperator;
    private WarehouseHashJoinOperator warehouseHashJoinOperator;

    public BatchBHJMultipleInputStreamOperator(
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
        long memorySize = computeMemorySize(parameters) / 4;
        this.warehouseHashJoinOperator =
                new WarehouseHashJoinOperator(
                        parameters, getRuntimeContext(), getMetricGroup(), memorySize, output);
        this.shipModeHashJoinOperator =
                new ShipModeHashJoinOperator(
                        parameters,
                        getRuntimeContext(),
                        getMetricGroup(),
                        memorySize,
                        warehouseHashJoinOperator);
        this.callCenterHashJoinOperator =
                new CallCenterHashJoinOperator(
                        parameters,
                        getRuntimeContext(),
                        getMetricGroup(),
                        memorySize,
                        shipModeHashJoinOperator);
        this.dateDimHashJoinOperator =
                new DateDimHashJoinOperator(
                        parameters,
                        getRuntimeContext(),
                        getMetricGroup(),
                        memorySize,
                        callCenterHashJoinOperator);
        LOG.info(
                "Open all join operator in multiple node successfully, operator memory size is {}.",
                memorySize);
        LOG.info("catalog_sales table without is not null calc.");
    }

    @Override
    public List<Input> getInputs() {
        return Arrays.asList(
                new AbstractInput(this, 1) {
                    @Override
                    public void processElement(StreamRecord element) throws Exception {
                        processCatalogSalesCalc(element);
                    }
                },
                new AbstractInput(this, 2) {
                    @Override
                    public void processElement(StreamRecord element) throws Exception {
                        dateDimHashJoinOperator.processBuild(element);
                    }
                },
                new AbstractInput(this, 3) {
                    @Override
                    public void processElement(StreamRecord element) throws Exception {
                        callCenterHashJoinOperator.processBuild(element);
                    }
                },
                new AbstractInput(this, 4) {
                    @Override
                    public void processElement(StreamRecord element) throws Exception {
                        shipModeHashJoinOperator.processBuild(element);
                    }
                },
                new AbstractInput(this, 5) {
                    @Override
                    public void processElement(StreamRecord element) throws Exception {
                        warehouseHashJoinOperator.processBuild(element);
                    }
                });
    }

    @Override
    public void endInput(int inputId) throws Exception {
        inputSelectionHandler.endInput(inputId);
        switch (inputId) {
            case 1:
                LOG.info("Ending catalog_sales probe.");
                break;
            case 2:
                dateDimHashJoinOperator.endInput1();
                LOG.info("Ending date_dim build.");
                break;
            case 3:
                callCenterHashJoinOperator.endInput(1);
                LOG.info("Ending call_center build.");
                break;
            case 4:
                shipModeHashJoinOperator.endInput(1);
                LOG.info("Ending ship_mode build.");
                break;
            case 5:
                warehouseHashJoinOperator.endInput(1);
                LOG.info("Ending warehouse build.");
                break;
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        dateDimHashJoinOperator.close();
        callCenterHashJoinOperator.close();
        shipModeHashJoinOperator.close();
        warehouseHashJoinOperator.close();
        LOG.info("Close all BHJ operator.");
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

    public void processCatalogSalesCalc(
            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element) throws Exception {
        /*org.apache.flink.table.data.RowData in1;
        long field$11;
        boolean isNull$11;
        long field$13;
        boolean isNull$13;
        long field$16;
        boolean isNull$16;
        long field$19;
        boolean isNull$19;
        boolean result$12;
        boolean result$15;
        boolean isNull$15;
        boolean result$14;
        boolean result$18;
        boolean isNull$18;
        boolean result$17;
        boolean result$21;
        boolean isNull$21;
        boolean result$20;

        in1 = (org.apache.flink.table.data.RowData) element.getValue();

        isNull$16 = in1.isNullAt(2);
        field$16 = -1L;
        if (!isNull$16) {
            field$16 = in1.getLong(2);
        }
        isNull$11 = in1.isNullAt(0);
        field$11 = -1L;
        if (!isNull$11) {
            field$11 = in1.getLong(0);
        }
        isNull$13 = in1.isNullAt(1);
        field$13 = -1L;
        if (!isNull$13) {
            field$13 = in1.getLong(1);
        }
        isNull$19 = in1.isNullAt(3);
        field$19 = -1L;
        if (!isNull$19) {
            field$19 = in1.getLong(3);
        }
        result$12 = !isNull$11;

        result$15 = false;
        isNull$15 = false;
        if (!false && !result$12) {
            // left expr is false, skip right expr
        } else {
            result$14 = !isNull$13;

            if (!false && !false) {
                result$15 = result$12 && result$14;
                isNull$15 = false;
            } else if (!false && result$12 && false) {
                result$15 = false;
                isNull$15 = true;
            } else if (!false && !result$12 && false) {
                result$15 = false;
                isNull$15 = false;
            } else if (false && !false && result$14) {
                result$15 = false;
                isNull$15 = true;
            } else if (false && !false && !result$14) {
                result$15 = false;
                isNull$15 = false;
            } else {
                result$15 = false;
                isNull$15 = true;
            }
        }

        result$18 = false;
        isNull$18 = false;
        if (!isNull$15 && !result$15) {
            // left expr is false, skip right expr
        } else {
            result$17 = !isNull$16;

            if (!isNull$15 && !false) {
                result$18 = result$15 && result$17;
                isNull$18 = false;
            } else if (!isNull$15 && result$15 && false) {
                result$18 = false;
                isNull$18 = true;
            } else if (!isNull$15 && !result$15 && false) {
                result$18 = false;
                isNull$18 = false;
            } else if (isNull$15 && !false && result$17) {
                result$18 = false;
                isNull$18 = true;
            } else if (isNull$15 && !false && !result$17) {
                result$18 = false;
                isNull$18 = false;
            } else {
                result$18 = false;
                isNull$18 = true;
            }
        }

        result$21 = false;
        isNull$21 = false;
        if (!isNull$18 && !result$18) {
            // left expr is false, skip right expr
        } else {
            result$20 = !isNull$19;

            if (!isNull$18 && !false) {
                result$21 = result$18 && result$20;
                isNull$21 = false;
            } else if (!isNull$18 && result$18 && false) {
                result$21 = false;
                isNull$21 = true;
            } else if (!isNull$18 && !result$18 && false) {
                result$21 = false;
                isNull$21 = false;
            } else if (isNull$18 && !false && result$20) {
                result$21 = false;
                isNull$21 = true;
            } else if (isNull$18 && !false && !result$20) {
                result$21 = false;
                isNull$21 = false;
            } else {
                result$21 = false;
                isNull$21 = true;
            }
        }
        if (result$21) {

        }*/
        // date_dim join
        dateDimHashJoinOperator.processProbe(element);
    }
}
