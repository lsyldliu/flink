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

package org.apache.flink.q99;

public final class BatchMultipleInputStreamOperator$182
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

    public void processInput1(org.apache.flink.table.data.RowData in1) throws Exception {

        // evaluate the required expr in advance

        boolean anyNull$25 = false;
        boolean isNull$23 = in1.isNullAt(0);
        long field$23 = isNull$23 ? -1L : (in1.getLong(0));

        anyNull$25 |= isNull$23;

        if (!anyNull$25) {

            hashTable$26.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in1);
        }
    }

    public void processInput2(org.apache.flink.table.data.RowData in2) throws Exception {

        // evaluate the required expr in advance

        boolean anyNull$29 = false;
        boolean isNull$27 = in2.isNullAt(0);
        long field$27 = isNull$27 ? -1L : (in2.getLong(0));

        anyNull$29 |= isNull$27;

        if (!anyNull$29) {

            hashTable$30.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in2);
        }
    }

    public void processInput3(org.apache.flink.table.data.RowData in3) throws Exception {

        // evaluate the required expr in advance

        boolean anyNull$33 = false;
        boolean isNull$31 = in3.isNullAt(0);
        long field$31 = isNull$31 ? -1L : (in3.getLong(0));

        anyNull$33 |= isNull$31;

        if (!anyNull$33) {

            hashTable$34.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in3);
        }
    }

    public void processInput4(org.apache.flink.table.data.RowData in4) throws Exception {

        // evaluate the required expr in advance

        boolean anyNull$36 = false;
        boolean isNull$35 = in4.isNullAt(0);
        long field$35 = isNull$35 ? -1L : (in4.getLong(0));

        anyNull$36 |= isNull$35;

        if (!anyNull$36) {

            hashTable$37.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in4);
        }
    }

    public void processInput5(org.apache.flink.table.data.RowData in5) throws Exception {
        // for loop to iterate the ColumnarRowData
        org.apache.flink.table.data.columnar.ColumnarRowData columnarRowData =
                (org.apache.flink.table.data.columnar.ColumnarRowData) in5;
        int numRows = columnarRowData.getNumRows();
        for (int i = 0; i < numRows; i++) {
            columnarRowData.setRowId(i);

            // evaluate the required expr in advance

            boolean isNull$38 = columnarRowData.isNullAt(0);
            long field$38 = isNull$38 ? -1L : (columnarRowData.getLong(0));
            boolean result$44 = !isNull$38;
            if (result$44) {

                // evaluate the required expr in advance

                // generate join key for probe side

                // find matches from hash table
                org.apache.flink.table.runtime.util.RowIterator buildIter$48 =
                        isNull$38 ? null : hashTable$37.get(field$38);
                if (buildIter$48 != null) {
                    while (buildIter$48.advanceNext()) {
                        org.apache.flink.table.data.RowData buildRow$46 = buildIter$48.getRow();
                        {

                            // evaluate the required expr in advance

                            // evaluate the required expr in advance

                            // generate join key for probe side
                            boolean isNull$39 = columnarRowData.isNullAt(1);
                            long field$39 = isNull$39 ? -1L : (columnarRowData.getLong(1));
                            // find matches from hash table
                            org.apache.flink.table.runtime.util.RowIterator buildIter$54 =
                                    isNull$39 ? null : hashTable$34.get(field$39);
                            if (buildIter$54 != null) {
                                while (buildIter$54.advanceNext()) {
                                    org.apache.flink.table.data.RowData buildRow$51 =
                                            buildIter$54.getRow();
                                    {

                                        // evaluate the required expr in advance

                                        // evaluate the required expr in advance

                                        // generate join key for probe side
                                        boolean isNull$40 = columnarRowData.isNullAt(2);
                                        long field$40 =
                                                isNull$40 ? -1L : (columnarRowData.getLong(2));
                                        // find matches from hash table
                                        org.apache.flink.table.runtime.util.RowIterator
                                                buildIter$60 =
                                                        isNull$40
                                                                ? null
                                                                : hashTable$30.get(field$40);
                                        if (buildIter$60 != null) {
                                            while (buildIter$60.advanceNext()) {
                                                org.apache.flink.table.data.RowData buildRow$57 =
                                                        buildIter$60.getRow();
                                                {

                                                    // evaluate the required expr in advance

                                                    // evaluate the required expr in advance

                                                    // generate join key for probe side
                                                    boolean isNull$41 = columnarRowData.isNullAt(3);
                                                    long field$41 =
                                                            isNull$41
                                                                    ? -1L
                                                                    : (columnarRowData.getLong(3));
                                                    // find matches from hash table
                                                    org.apache.flink.table.runtime.util.RowIterator
                                                            buildIter$66 =
                                                                    isNull$41
                                                                            ? null
                                                                            : hashTable$26.get(
                                                                                    field$41);
                                                    if (buildIter$66 != null) {
                                                        while (buildIter$66.advanceNext()) {
                                                            org.apache.flink.table.data.RowData
                                                                    buildRow$63 =
                                                                            buildIter$66.getRow();
                                                            {

                                                                // evaluate the required expr in
                                                                // advance
                                                                boolean isNull$42 =
                                                                        columnarRowData.isNullAt(4);
                                                                long field$42 =
                                                                        isNull$42
                                                                                ? -1L
                                                                                : (columnarRowData
                                                                                        .getLong(
                                                                                                4));

                                                                // evaluate the required expr in
                                                                // advance

                                                                // wrap variable to row

                                                                boolean isNull$65 =
                                                                        buildRow$63.isNullAt(1);
                                                                org.apache.flink.table.data.binary
                                                                                .BinaryStringData
                                                                        field$65 =
                                                                                isNull$65
                                                                                        ? org.apache
                                                                                                .flink
                                                                                                .table
                                                                                                .data
                                                                                                .binary
                                                                                                .BinaryStringData
                                                                                                .EMPTY_UTF8
                                                                                        : (((org
                                                                                                        .apache
                                                                                                        .flink
                                                                                                        .table
                                                                                                        .data
                                                                                                        .binary
                                                                                                        .BinaryStringData)
                                                                                                buildRow$63
                                                                                                        .getString(
                                                                                                                1)));

                                                                boolean isNull$68 =
                                                                        isNull$65 || false || false;
                                                                org.apache.flink.table.data.binary
                                                                                .BinaryStringData
                                                                        result$68 =
                                                                                org.apache.flink
                                                                                        .table.data
                                                                                        .binary
                                                                                        .BinaryStringData
                                                                                        .EMPTY_UTF8;
                                                                if (!isNull$68) {

                                                                    result$68 =
                                                                            org.apache.flink.table
                                                                                    .data.binary
                                                                                    .BinaryStringDataUtil
                                                                                    .substringSQL(
                                                                                            field$65,
                                                                                            ((int)
                                                                                                    1),
                                                                                            ((int)
                                                                                                    20));

                                                                    isNull$68 = (result$68 == null);
                                                                }

                                                                if (isNull$68) {
                                                                    out_inputRow$13.setNullAt(0);
                                                                } else {
                                                                    out_inputRow$13
                                                                            .setNonPrimitiveValue(
                                                                                    0, result$68);
                                                                }

                                                                boolean isNull$59 =
                                                                        buildRow$57.isNullAt(1);
                                                                org.apache.flink.table.data.binary
                                                                                .BinaryStringData
                                                                        field$59 =
                                                                                isNull$59
                                                                                        ? org.apache
                                                                                                .flink
                                                                                                .table
                                                                                                .data
                                                                                                .binary
                                                                                                .BinaryStringData
                                                                                                .EMPTY_UTF8
                                                                                        : (((org
                                                                                                        .apache
                                                                                                        .flink
                                                                                                        .table
                                                                                                        .data
                                                                                                        .binary
                                                                                                        .BinaryStringData)
                                                                                                buildRow$57
                                                                                                        .getString(
                                                                                                                1)));
                                                                if (isNull$59) {
                                                                    out_inputRow$13.setNullAt(1);
                                                                } else {
                                                                    out_inputRow$13
                                                                            .setNonPrimitiveValue(
                                                                                    1, field$59);
                                                                }

                                                                boolean isNull$53 =
                                                                        buildRow$51.isNullAt(1);
                                                                org.apache.flink.table.data.binary
                                                                                .BinaryStringData
                                                                        field$53 =
                                                                                isNull$53
                                                                                        ? org.apache
                                                                                                .flink
                                                                                                .table
                                                                                                .data
                                                                                                .binary
                                                                                                .BinaryStringData
                                                                                                .EMPTY_UTF8
                                                                                        : (((org
                                                                                                        .apache
                                                                                                        .flink
                                                                                                        .table
                                                                                                        .data
                                                                                                        .binary
                                                                                                        .BinaryStringData)
                                                                                                buildRow$51
                                                                                                        .getString(
                                                                                                                1)));
                                                                if (isNull$53) {
                                                                    out_inputRow$13.setNullAt(2);
                                                                } else {
                                                                    out_inputRow$13
                                                                            .setNonPrimitiveValue(
                                                                                    2, field$53);
                                                                }

                                                                boolean isNull$69 =
                                                                        isNull$38 || isNull$42;
                                                                long result$69 = -1L;
                                                                if (!isNull$69) {

                                                                    result$69 =
                                                                            (long)
                                                                                    (field$38
                                                                                            - field$42);
                                                                }

                                                                boolean isNull$70 =
                                                                        isNull$69 || false;
                                                                boolean result$70 = false;
                                                                if (!isNull$70) {

                                                                    result$70 =
                                                                            result$69 <= ((int) 30);
                                                                }

                                                                int result$71 = -1;
                                                                boolean isNull$71;
                                                                if (result$70) {

                                                                    // --- Cast section generated by
                                                                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                    // --- End cast section

                                                                    isNull$71 = false;
                                                                    if (!isNull$71) {
                                                                        result$71 = ((int) 1);
                                                                    }
                                                                } else {

                                                                    // --- Cast section generated by
                                                                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                    // --- End cast section

                                                                    isNull$71 = false;
                                                                    if (!isNull$71) {
                                                                        result$71 = ((int) 0);
                                                                    }
                                                                }
                                                                out_inputRow$13.setInt(
                                                                        3, result$71);

                                                                // evaluate left expr

                                                                boolean isNull$72 =
                                                                        isNull$38 || isNull$42;
                                                                long result$72 = -1L;
                                                                if (!isNull$72) {

                                                                    result$72 =
                                                                            (long)
                                                                                    (field$38
                                                                                            - field$42);
                                                                }

                                                                boolean isNull$73 =
                                                                        isNull$72 || false;
                                                                boolean result$73 = false;
                                                                if (!isNull$73) {

                                                                    result$73 =
                                                                            result$72 > ((int) 30);
                                                                }

                                                                // evaluate right expr

                                                                boolean isNull$74 =
                                                                        isNull$38 || isNull$42;
                                                                long result$74 = -1L;
                                                                if (!isNull$74) {

                                                                    result$74 =
                                                                            (long)
                                                                                    (field$38
                                                                                            - field$42);
                                                                }

                                                                boolean isNull$75 =
                                                                        isNull$74 || false;
                                                                boolean result$75 = false;
                                                                if (!isNull$75) {

                                                                    result$75 =
                                                                            result$74 <= ((int) 60);
                                                                }

                                                                boolean result$76 = false;
                                                                boolean isNull$76 = false;
                                                                if (!isNull$73 && !result$73) {
                                                                    // left expr is false, skip
                                                                    // right expr
                                                                } else {
                                                                    if (!isNull$73 && !isNull$75) {
                                                                        result$76 =
                                                                                result$73
                                                                                        && result$75;
                                                                        isNull$76 = false;
                                                                    } else if (!isNull$73
                                                                            && result$73
                                                                            && isNull$75) {
                                                                        result$76 = false;
                                                                        isNull$76 = true;
                                                                    } else if (!isNull$73
                                                                            && !result$73
                                                                            && isNull$75) {
                                                                        result$76 = false;
                                                                        isNull$76 = false;
                                                                    } else if (isNull$73
                                                                            && !isNull$75
                                                                            && result$75) {
                                                                        result$76 = false;
                                                                        isNull$76 = true;
                                                                    } else if (isNull$73
                                                                            && !isNull$75
                                                                            && !result$75) {
                                                                        result$76 = false;
                                                                        isNull$76 = false;
                                                                    } else {
                                                                        result$76 = false;
                                                                        isNull$76 = true;
                                                                    }
                                                                }
                                                                int result$77 = -1;
                                                                boolean isNull$77;
                                                                if (result$76) {

                                                                    // --- Cast section generated by
                                                                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                    // --- End cast section

                                                                    isNull$77 = false;
                                                                    if (!isNull$77) {
                                                                        result$77 = ((int) 1);
                                                                    }
                                                                } else {

                                                                    // --- Cast section generated by
                                                                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                    // --- End cast section

                                                                    isNull$77 = false;
                                                                    if (!isNull$77) {
                                                                        result$77 = ((int) 0);
                                                                    }
                                                                }
                                                                out_inputRow$13.setInt(
                                                                        4, result$77);

                                                                // evaluate left expr

                                                                boolean isNull$78 =
                                                                        isNull$38 || isNull$42;
                                                                long result$78 = -1L;
                                                                if (!isNull$78) {

                                                                    result$78 =
                                                                            (long)
                                                                                    (field$38
                                                                                            - field$42);
                                                                }

                                                                boolean isNull$79 =
                                                                        isNull$78 || false;
                                                                boolean result$79 = false;
                                                                if (!isNull$79) {

                                                                    result$79 =
                                                                            result$78 > ((int) 60);
                                                                }

                                                                // evaluate right expr

                                                                boolean isNull$80 =
                                                                        isNull$38 || isNull$42;
                                                                long result$80 = -1L;
                                                                if (!isNull$80) {

                                                                    result$80 =
                                                                            (long)
                                                                                    (field$38
                                                                                            - field$42);
                                                                }

                                                                boolean isNull$81 =
                                                                        isNull$80 || false;
                                                                boolean result$81 = false;
                                                                if (!isNull$81) {

                                                                    result$81 =
                                                                            result$80 <= ((int) 90);
                                                                }

                                                                boolean result$82 = false;
                                                                boolean isNull$82 = false;
                                                                if (!isNull$79 && !result$79) {
                                                                    // left expr is false, skip
                                                                    // right expr
                                                                } else {
                                                                    if (!isNull$79 && !isNull$81) {
                                                                        result$82 =
                                                                                result$79
                                                                                        && result$81;
                                                                        isNull$82 = false;
                                                                    } else if (!isNull$79
                                                                            && result$79
                                                                            && isNull$81) {
                                                                        result$82 = false;
                                                                        isNull$82 = true;
                                                                    } else if (!isNull$79
                                                                            && !result$79
                                                                            && isNull$81) {
                                                                        result$82 = false;
                                                                        isNull$82 = false;
                                                                    } else if (isNull$79
                                                                            && !isNull$81
                                                                            && result$81) {
                                                                        result$82 = false;
                                                                        isNull$82 = true;
                                                                    } else if (isNull$79
                                                                            && !isNull$81
                                                                            && !result$81) {
                                                                        result$82 = false;
                                                                        isNull$82 = false;
                                                                    } else {
                                                                        result$82 = false;
                                                                        isNull$82 = true;
                                                                    }
                                                                }
                                                                int result$83 = -1;
                                                                boolean isNull$83;
                                                                if (result$82) {

                                                                    // --- Cast section generated by
                                                                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                    // --- End cast section

                                                                    isNull$83 = false;
                                                                    if (!isNull$83) {
                                                                        result$83 = ((int) 1);
                                                                    }
                                                                } else {

                                                                    // --- Cast section generated by
                                                                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                    // --- End cast section

                                                                    isNull$83 = false;
                                                                    if (!isNull$83) {
                                                                        result$83 = ((int) 0);
                                                                    }
                                                                }
                                                                out_inputRow$13.setInt(
                                                                        5, result$83);

                                                                // evaluate left expr

                                                                boolean isNull$84 =
                                                                        isNull$38 || isNull$42;
                                                                long result$84 = -1L;
                                                                if (!isNull$84) {

                                                                    result$84 =
                                                                            (long)
                                                                                    (field$38
                                                                                            - field$42);
                                                                }

                                                                boolean isNull$85 =
                                                                        isNull$84 || false;
                                                                boolean result$85 = false;
                                                                if (!isNull$85) {

                                                                    result$85 =
                                                                            result$84 > ((int) 90);
                                                                }

                                                                // evaluate right expr

                                                                boolean isNull$86 =
                                                                        isNull$38 || isNull$42;
                                                                long result$86 = -1L;
                                                                if (!isNull$86) {

                                                                    result$86 =
                                                                            (long)
                                                                                    (field$38
                                                                                            - field$42);
                                                                }

                                                                boolean isNull$87 =
                                                                        isNull$86 || false;
                                                                boolean result$87 = false;
                                                                if (!isNull$87) {

                                                                    result$87 =
                                                                            result$86
                                                                                    <= ((int) 120);
                                                                }

                                                                boolean result$88 = false;
                                                                boolean isNull$88 = false;
                                                                if (!isNull$85 && !result$85) {
                                                                    // left expr is false, skip
                                                                    // right expr
                                                                } else {
                                                                    if (!isNull$85 && !isNull$87) {
                                                                        result$88 =
                                                                                result$85
                                                                                        && result$87;
                                                                        isNull$88 = false;
                                                                    } else if (!isNull$85
                                                                            && result$85
                                                                            && isNull$87) {
                                                                        result$88 = false;
                                                                        isNull$88 = true;
                                                                    } else if (!isNull$85
                                                                            && !result$85
                                                                            && isNull$87) {
                                                                        result$88 = false;
                                                                        isNull$88 = false;
                                                                    } else if (isNull$85
                                                                            && !isNull$87
                                                                            && result$87) {
                                                                        result$88 = false;
                                                                        isNull$88 = true;
                                                                    } else if (isNull$85
                                                                            && !isNull$87
                                                                            && !result$87) {
                                                                        result$88 = false;
                                                                        isNull$88 = false;
                                                                    } else {
                                                                        result$88 = false;
                                                                        isNull$88 = true;
                                                                    }
                                                                }
                                                                int result$89 = -1;
                                                                boolean isNull$89;
                                                                if (result$88) {

                                                                    // --- Cast section generated by
                                                                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                    // --- End cast section

                                                                    isNull$89 = false;
                                                                    if (!isNull$89) {
                                                                        result$89 = ((int) 1);
                                                                    }
                                                                } else {

                                                                    // --- Cast section generated by
                                                                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                    // --- End cast section

                                                                    isNull$89 = false;
                                                                    if (!isNull$89) {
                                                                        result$89 = ((int) 0);
                                                                    }
                                                                }
                                                                out_inputRow$13.setInt(
                                                                        6, result$89);

                                                                boolean isNull$90 =
                                                                        isNull$38 || isNull$42;
                                                                long result$90 = -1L;
                                                                if (!isNull$90) {

                                                                    result$90 =
                                                                            (long)
                                                                                    (field$38
                                                                                            - field$42);
                                                                }

                                                                boolean isNull$91 =
                                                                        isNull$90 || false;
                                                                boolean result$91 = false;
                                                                if (!isNull$91) {

                                                                    result$91 =
                                                                            result$90 > ((int) 120);
                                                                }

                                                                int result$92 = -1;
                                                                boolean isNull$92;
                                                                if (result$91) {

                                                                    // --- Cast section generated by
                                                                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                    // --- End cast section

                                                                    isNull$92 = false;
                                                                    if (!isNull$92) {
                                                                        result$92 = ((int) 1);
                                                                    }
                                                                } else {

                                                                    // --- Cast section generated by
                                                                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                    // --- End cast section

                                                                    isNull$92 = false;
                                                                    if (!isNull$92) {
                                                                        result$92 = ((int) 0);
                                                                    }
                                                                }
                                                                out_inputRow$13.setInt(
                                                                        7, result$92);

                                                                output.collect(
                                                                        outElement.replace(
                                                                                out_inputRow$13));
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public void endInput1() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$26.endBuild();
    }

    public void endInput2() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$30.endBuild();
    }

    public void endInput3() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$34.endBuild();
    }

    public void endInput4() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$37.endBuild();
    }

    public void endInput5() throws Exception {}

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.BoxedWrapperRowData out_inputRow$13 =
            new org.apache.flink.table.data.BoxedWrapperRowData(8);
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$97;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$98;
    Projection$99 buildToBinaryRow$108;
    private transient java.lang.Object[] buildProjRefs$109;
    Projection$102 probeToBinaryRow$108;
    private transient java.lang.Object[] probeProjRefs$110;
    LongHashTable$111 hashTable$26;

    private void hj_init$114(Object[] references) throws Exception {
        buildSer$97 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$98 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$109 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$108 = new Projection$99(buildProjRefs$109);
        probeProjRefs$110 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$108 = new Projection$102(probeProjRefs$110);
    }

    private transient java.lang.Object[] hj_Refs$115;

    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$117;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$118;
    Projection$119 buildToBinaryRow$128;
    private transient java.lang.Object[] buildProjRefs$129;
    Projection$122 probeToBinaryRow$128;
    private transient java.lang.Object[] probeProjRefs$130;
    LongHashTable$131 hashTable$30;

    private void hj_init$134(Object[] references) throws Exception {
        buildSer$117 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$118 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$129 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$128 = new Projection$119(buildProjRefs$129);
        probeProjRefs$130 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$128 = new Projection$122(probeProjRefs$130);
    }

    private transient java.lang.Object[] hj_Refs$135;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$137;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$138;
    Projection$139 buildToBinaryRow$148;
    private transient java.lang.Object[] buildProjRefs$149;
    Projection$142 probeToBinaryRow$148;
    private transient java.lang.Object[] probeProjRefs$150;
    LongHashTable$151 hashTable$34;

    private void hj_init$154(Object[] references) throws Exception {
        buildSer$137 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$138 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$149 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$148 = new Projection$139(buildProjRefs$149);
        probeProjRefs$150 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$148 = new Projection$142(probeProjRefs$150);
    }

    private transient java.lang.Object[] hj_Refs$155;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$157;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$158;
    Projection$159 buildToBinaryRow$167;
    private transient java.lang.Object[] buildProjRefs$168;
    Projection$161 probeToBinaryRow$167;
    private transient java.lang.Object[] probeProjRefs$169;
    LongHashTable$170 hashTable$37;

    private void hj_init$173(Object[] references) throws Exception {
        buildSer$157 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$158 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$168 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$167 = new Projection$159(buildProjRefs$168);
        probeProjRefs$169 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$167 = new Projection$161(probeProjRefs$169);
    }

    private transient java.lang.Object[] hj_Refs$174;
    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$181;

    public BatchMultipleInputStreamOperator$182(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 5);
        this.parameters = parameters;
        hj_Refs$115 = (((java.lang.Object[]) references[0]));
        hj_init$114(hj_Refs$115);
        hj_Refs$135 = (((java.lang.Object[]) references[1]));
        hj_init$134(hj_Refs$135);
        hj_Refs$155 = (((java.lang.Object[]) references[2]));
        hj_init$154(hj_Refs$155);
        hj_Refs$174 = (((java.lang.Object[]) references[3]));
        hj_init$173(hj_Refs$174);
        inputSpecRefs$181 = (((java.util.ArrayList) references[4]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$181);
    }

    @Override
    public void open() throws Exception {
        super.open();

        long memorySize$112 = computeMemorySize(0.25);
        hashTable$26 = new LongHashTable$111(memorySize$112);
        long memorySize$132 = computeMemorySize(0.25);
        hashTable$30 = new LongHashTable$131(memorySize$132);
        long memorySize$152 = computeMemorySize(0.25);
        hashTable$34 = new LongHashTable$151(memorySize$152);
        long memorySize$171 = computeMemorySize(0.25);
        hashTable$37 = new LongHashTable$170(memorySize$171);
    }

    @Override
    public java.util.List getInputs() {
        return java.util.Arrays.asList(
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 1) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`w_warehouse_sk` BIGINT NOT NULL, `w_warehouse_name`
                        // VARCHAR(2147483647)>
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
                        // InputType: ROW<`sm_ship_mode_sk` BIGINT NOT NULL, `sm_type`
                        // VARCHAR(2147483647)>
                        org.apache.flink.table.data.RowData in2 =
                                (org.apache.flink.table.data.RowData) element.getValue();
                        processInput2(in2);
                    }
                },
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 3) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`cc_call_center_sk` BIGINT NOT NULL, `cc_name`
                        // VARCHAR(2147483647)>
                        org.apache.flink.table.data.RowData in3 =
                                (org.apache.flink.table.data.RowData) element.getValue();
                        processInput3(in3);
                    }
                },
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 4) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`d_date_sk` BIGINT NOT NULL>
                        org.apache.flink.table.data.RowData in4 =
                                (org.apache.flink.table.data.RowData) element.getValue();
                        processInput4(in4);
                    }
                },
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 5) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`cs_ship_date_sk` BIGINT, `cs_call_center_sk` BIGINT,
                        // `cs_ship_mode_sk` BIGINT, `cs_warehouse_sk` BIGINT, `cs_sold_date_sk`
                        // BIGINT>
                        org.apache.flink.table.data.RowData in5 =
                                (org.apache.flink.table.data.RowData) element.getValue();
                        processInput5(in5);
                    }
                });
    }

    @Override
    public void endInput(int inputId) throws Exception {
        inputSelectionHandler.endInput(inputId);
        switch (inputId) {
            case 1:
                endInput1();
                break;

            case 2:
                endInput2();
                break;

            case 3:
                endInput3();
                break;

            case 4:
                endInput4();
                break;

            case 5:
                endInput5();
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

        if (this.hashTable$37 != null) {
            this.hashTable$37.close();
            this.hashTable$37.free();
            this.hashTable$37 = null;
        }

        if (this.hashTable$34 != null) {
            this.hashTable$34.close();
            this.hashTable$34.free();
            this.hashTable$34 = null;
        }

        if (this.hashTable$30 != null) {
            this.hashTable$30.close();
            this.hashTable$30.free();
            this.hashTable$30 = null;
        }

        if (this.hashTable$26 != null) {
            this.hashTable$26.close();
            this.hashTable$26.free();
            this.hashTable$26 = null;
        }
    }

    public class Projection$99
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(2);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$99(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$100 = in1.isNullAt(0);
            long field$100 = isNull$100 ? -1L : (in1.getLong(0));
            if (isNull$100) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$100);
            }

            boolean isNull$101 = in1.isNullAt(1);
            org.apache.flink.table.data.binary.BinaryStringData field$101 =
                    isNull$101
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(1)));
            if (isNull$101) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeString(1, field$101);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$111
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$111(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$97,
                    probeSer$98,
                    getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    getContainingTask().getEnvironment().getIOManager(),
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
                return probeToBinaryRow$108.apply(row);
            }
        }
    }

    public class Projection$102
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(5);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$102(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$103 = in1.isNullAt(0);
            long field$103 = isNull$103 ? -1L : (in1.getLong(0));
            if (isNull$103) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$103);
            }

            boolean isNull$104 = in1.isNullAt(1);
            long field$104 = isNull$104 ? -1L : (in1.getLong(1));
            if (isNull$104) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$104);
            }

            boolean isNull$105 = in1.isNullAt(2);
            long field$105 = isNull$105 ? -1L : (in1.getLong(2));
            if (isNull$105) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeLong(2, field$105);
            }

            boolean isNull$106 = in1.isNullAt(3);
            org.apache.flink.table.data.binary.BinaryStringData field$106 =
                    isNull$106
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(3)));
            if (isNull$106) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeString(3, field$106);
            }

            boolean isNull$107 = in1.isNullAt(4);
            org.apache.flink.table.data.binary.BinaryStringData field$107 =
                    isNull$107
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(4)));
            if (isNull$107) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeString(4, field$107);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$161
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(5);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$161(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$162 = in1.isNullAt(0);
            long field$162 = isNull$162 ? -1L : (in1.getLong(0));
            if (isNull$162) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$162);
            }

            boolean isNull$163 = in1.isNullAt(1);
            long field$163 = isNull$163 ? -1L : (in1.getLong(1));
            if (isNull$163) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$163);
            }

            boolean isNull$164 = in1.isNullAt(2);
            long field$164 = isNull$164 ? -1L : (in1.getLong(2));
            if (isNull$164) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeLong(2, field$164);
            }

            boolean isNull$165 = in1.isNullAt(3);
            long field$165 = isNull$165 ? -1L : (in1.getLong(3));
            if (isNull$165) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeLong(3, field$165);
            }

            boolean isNull$166 = in1.isNullAt(4);
            long field$166 = isNull$166 ? -1L : (in1.getLong(4));
            if (isNull$166) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeLong(4, field$166);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$170
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$170(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$157,
                    probeSer$158,
                    getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    getContainingTask().getEnvironment().getIOManager(),
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
                return probeToBinaryRow$167.apply(row);
            }
        }
    }

    public class Projection$159
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$159(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$160 = in1.isNullAt(0);
            long field$160 = isNull$160 ? -1L : (in1.getLong(0));
            if (isNull$160) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$160);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$122
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(5);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$122(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$123 = in1.isNullAt(0);
            long field$123 = isNull$123 ? -1L : (in1.getLong(0));
            if (isNull$123) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$123);
            }

            boolean isNull$124 = in1.isNullAt(1);
            long field$124 = isNull$124 ? -1L : (in1.getLong(1));
            if (isNull$124) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$124);
            }

            boolean isNull$125 = in1.isNullAt(2);
            long field$125 = isNull$125 ? -1L : (in1.getLong(2));
            if (isNull$125) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeLong(2, field$125);
            }

            boolean isNull$126 = in1.isNullAt(3);
            long field$126 = isNull$126 ? -1L : (in1.getLong(3));
            if (isNull$126) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeLong(3, field$126);
            }

            boolean isNull$127 = in1.isNullAt(4);
            org.apache.flink.table.data.binary.BinaryStringData field$127 =
                    isNull$127
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(4)));
            if (isNull$127) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeString(4, field$127);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$119
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(2);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$119(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$120 = in1.isNullAt(0);
            long field$120 = isNull$120 ? -1L : (in1.getLong(0));
            if (isNull$120) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$120);
            }

            boolean isNull$121 = in1.isNullAt(1);
            org.apache.flink.table.data.binary.BinaryStringData field$121 =
                    isNull$121
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(1)));
            if (isNull$121) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeString(1, field$121);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$131
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$131(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$117,
                    probeSer$118,
                    getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    getContainingTask().getEnvironment().getIOManager(),
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
                return probeToBinaryRow$128.apply(row);
            }
        }
    }

    public class LongHashTable$151
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$151(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$137,
                    probeSer$138,
                    getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    getContainingTask().getEnvironment().getIOManager(),
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
                return probeToBinaryRow$148.apply(row);
            }
        }
    }

    public class Projection$142
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(5);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$142(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$143 = in1.isNullAt(0);
            long field$143 = isNull$143 ? -1L : (in1.getLong(0));
            if (isNull$143) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$143);
            }

            boolean isNull$144 = in1.isNullAt(1);
            long field$144 = isNull$144 ? -1L : (in1.getLong(1));
            if (isNull$144) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$144);
            }

            boolean isNull$145 = in1.isNullAt(2);
            long field$145 = isNull$145 ? -1L : (in1.getLong(2));
            if (isNull$145) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeLong(2, field$145);
            }

            boolean isNull$146 = in1.isNullAt(3);
            long field$146 = isNull$146 ? -1L : (in1.getLong(3));
            if (isNull$146) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeLong(3, field$146);
            }

            boolean isNull$147 = in1.isNullAt(4);
            long field$147 = isNull$147 ? -1L : (in1.getLong(4));
            if (isNull$147) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeLong(4, field$147);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$139
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(2);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$139(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$140 = in1.isNullAt(0);
            long field$140 = isNull$140 ? -1L : (in1.getLong(0));
            if (isNull$140) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$140);
            }

            boolean isNull$141 = in1.isNullAt(1);
            org.apache.flink.table.data.binary.BinaryStringData field$141 =
                    isNull$141
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(1)));
            if (isNull$141) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeString(1, field$141);
            }

            outWriter.complete();

            return out;
        }
    }
}
