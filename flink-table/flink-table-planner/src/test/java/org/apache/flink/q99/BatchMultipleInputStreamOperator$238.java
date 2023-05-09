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

public final class BatchMultipleInputStreamOperator$238
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

        boolean anyNull$26 = false;
        boolean isNull$24 = in1.isNullAt(0);
        long field$24 = isNull$24 ? -1L : (in1.getLong(0));

        anyNull$26 |= isNull$24;

        if (!anyNull$26) {

            hashTable$27.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in1);
        }
    }

    public void processInput2(org.apache.flink.table.data.RowData in2) throws Exception {

        // evaluate the required expr in advance

        boolean anyNull$30 = false;
        boolean isNull$28 = in2.isNullAt(0);
        long field$28 = isNull$28 ? -1L : (in2.getLong(0));

        anyNull$30 |= isNull$28;

        if (!anyNull$30) {

            hashTable$31.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in2);
        }
    }

    public void processInput3(org.apache.flink.table.data.RowData in3) throws Exception {

        // evaluate the required expr in advance

        boolean anyNull$34 = false;
        boolean isNull$32 = in3.isNullAt(0);
        long field$32 = isNull$32 ? -1L : (in3.getLong(0));

        anyNull$34 |= isNull$32;

        if (!anyNull$34) {

            hashTable$35.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in3);
        }
    }

    public void processInput4(org.apache.flink.table.data.RowData in4) throws Exception {

        // evaluate the required expr in advance

        boolean anyNull$37 = false;
        boolean isNull$36 = in4.isNullAt(0);
        long field$36 = isNull$36 ? -1L : (in4.getLong(0));

        anyNull$37 |= isNull$36;

        if (!anyNull$37) {

            hashTable$38.putBuildRow((org.apache.flink.table.data.binary.BinaryRowData) in4);
        }
    }

    public void processInput5(org.apache.flink.table.data.RowData in5) throws Exception {
        // for loop to iterate the ColumnarRowData
        org.apache.flink.table.data.columnar.ColumnarRowData columnarRowData$39 =
                (org.apache.flink.table.data.columnar.ColumnarRowData) in5;
        int numRows$45 = columnarRowData$39.getNumRows();
        for (int i = 0; i < numRows$45; i++) {
            columnarRowData$39.setRowId(i);

            // evaluate the required expr in advance

            boolean isNull$40 = columnarRowData$39.isNullAt(0);
            long field$40 = isNull$40 ? -1L : (columnarRowData$39.getLong(0));
            boolean result$47 = !isNull$40;
            if (result$47) {

                // evaluate the required expr in advance

                // generate join key for probe side

                // find matches from hash table
                org.apache.flink.table.runtime.util.RowIterator buildIter$51 =
                        isNull$40 ? null : hashTable$38.get(field$40);
                if (buildIter$51 != null) {
                    while (buildIter$51.advanceNext()) {
                        org.apache.flink.table.data.RowData buildRow$49 = buildIter$51.getRow();
                        {

                            // evaluate the required expr in advance

                            // evaluate the required expr in advance

                            // generate join key for probe side
                            boolean isNull$41 = columnarRowData$39.isNullAt(1);
                            long field$41 = isNull$41 ? -1L : (columnarRowData$39.getLong(1));
                            // find matches from hash table
                            org.apache.flink.table.runtime.util.RowIterator buildIter$57 =
                                    isNull$41 ? null : hashTable$35.get(field$41);
                            if (buildIter$57 != null) {
                                while (buildIter$57.advanceNext()) {
                                    org.apache.flink.table.data.RowData buildRow$54 =
                                            buildIter$57.getRow();
                                    {

                                        // evaluate the required expr in advance

                                        // evaluate the required expr in advance

                                        // generate join key for probe side
                                        boolean isNull$42 = columnarRowData$39.isNullAt(2);
                                        long field$42 =
                                                isNull$42 ? -1L : (columnarRowData$39.getLong(2));
                                        // find matches from hash table
                                        org.apache.flink.table.runtime.util.RowIterator
                                                buildIter$63 =
                                                        isNull$42
                                                                ? null
                                                                : hashTable$31.get(field$42);
                                        if (buildIter$63 != null) {
                                            while (buildIter$63.advanceNext()) {
                                                org.apache.flink.table.data.RowData buildRow$60 =
                                                        buildIter$63.getRow();
                                                {

                                                    // evaluate the required expr in advance

                                                    // evaluate the required expr in advance

                                                    // generate join key for probe side
                                                    boolean isNull$43 =
                                                            columnarRowData$39.isNullAt(3);
                                                    long field$43 =
                                                            isNull$43
                                                                    ? -1L
                                                                    : (columnarRowData$39.getLong(
                                                                            3));
                                                    // find matches from hash table
                                                    org.apache.flink.table.runtime.util.RowIterator
                                                            buildIter$69 =
                                                                    isNull$43
                                                                            ? null
                                                                            : hashTable$27.get(
                                                                                    field$43);
                                                    if (buildIter$69 != null) {
                                                        while (buildIter$69.advanceNext()) {
                                                            org.apache.flink.table.data.RowData
                                                                    buildRow$66 =
                                                                            buildIter$69.getRow();
                                                            {

                                                                // evaluate the required expr in
                                                                // advance
                                                                boolean isNull$44 =
                                                                        columnarRowData$39.isNullAt(
                                                                                4);
                                                                long field$44 =
                                                                        isNull$44
                                                                                ? -1L
                                                                                : (columnarRowData$39
                                                                                        .getLong(
                                                                                                4));

                                                                // evaluate the required expr in
                                                                // advance

                                                                do {
                                                                    // input field access
                                                                    boolean isNull$68 =
                                                                            buildRow$66.isNullAt(1);
                                                                    org.apache.flink.table.data
                                                                                    .binary
                                                                                    .BinaryStringData
                                                                            field$68 =
                                                                                    isNull$68
                                                                                            ? org
                                                                                                    .apache
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
                                                                                                    buildRow$66
                                                                                                            .getString(
                                                                                                                    1)));

                                                                    boolean isNull$71 =
                                                                            isNull$68 || false
                                                                                    || false;
                                                                    org.apache.flink.table.data
                                                                                    .binary
                                                                                    .BinaryStringData
                                                                            result$71 =
                                                                                    org.apache.flink
                                                                                            .table
                                                                                            .data
                                                                                            .binary
                                                                                            .BinaryStringData
                                                                                            .EMPTY_UTF8;
                                                                    if (!isNull$71) {

                                                                        result$71 =
                                                                                org.apache.flink
                                                                                        .table.data
                                                                                        .binary
                                                                                        .BinaryStringDataUtil
                                                                                        .substringSQL(
                                                                                                field$68,
                                                                                                ((int)
                                                                                                        1),
                                                                                                ((int)
                                                                                                        20));

                                                                        isNull$71 =
                                                                                (result$71 == null);
                                                                    }

                                                                    boolean isNull$62 =
                                                                            buildRow$60.isNullAt(1);
                                                                    org.apache.flink.table.data
                                                                                    .binary
                                                                                    .BinaryStringData
                                                                            field$62 =
                                                                                    isNull$62
                                                                                            ? org
                                                                                                    .apache
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
                                                                                                    buildRow$60
                                                                                                            .getString(
                                                                                                                    1)));
                                                                    boolean isNull$56 =
                                                                            buildRow$54.isNullAt(1);
                                                                    org.apache.flink.table.data
                                                                                    .binary
                                                                                    .BinaryStringData
                                                                            field$56 =
                                                                                    isNull$56
                                                                                            ? org
                                                                                                    .apache
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
                                                                                                    buildRow$54
                                                                                                            .getString(
                                                                                                                    1)));
                                                                    boolean isNull$72 =
                                                                            isNull$40 || isNull$44;
                                                                    long result$72 = -1L;
                                                                    if (!isNull$72) {

                                                                        result$72 =
                                                                                (long)
                                                                                        (field$40
                                                                                                - field$44);
                                                                    }

                                                                    boolean isNull$73 =
                                                                            isNull$72 || false;
                                                                    boolean result$73 = false;
                                                                    if (!isNull$73) {

                                                                        result$73 =
                                                                                result$72
                                                                                        <= ((int)
                                                                                                30);
                                                                    }

                                                                    int result$74 = -1;
                                                                    boolean isNull$74;
                                                                    if (result$73) {

                                                                        // --- Cast section
                                                                        // generated by
                                                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                        // --- End cast section

                                                                        isNull$74 = false;
                                                                        if (!isNull$74) {
                                                                            result$74 = ((int) 1);
                                                                        }
                                                                    } else {

                                                                        // --- Cast section
                                                                        // generated by
                                                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                        // --- End cast section

                                                                        isNull$74 = false;
                                                                        if (!isNull$74) {
                                                                            result$74 = ((int) 0);
                                                                        }
                                                                    }
                                                                    // evaluate left expr

                                                                    boolean isNull$75 =
                                                                            isNull$40 || isNull$44;
                                                                    long result$75 = -1L;
                                                                    if (!isNull$75) {

                                                                        result$75 =
                                                                                (long)
                                                                                        (field$40
                                                                                                - field$44);
                                                                    }

                                                                    boolean isNull$76 =
                                                                            isNull$75 || false;
                                                                    boolean result$76 = false;
                                                                    if (!isNull$76) {

                                                                        result$76 =
                                                                                result$75
                                                                                        > ((int)
                                                                                                30);
                                                                    }

                                                                    // evaluate right expr

                                                                    boolean isNull$77 =
                                                                            isNull$40 || isNull$44;
                                                                    long result$77 = -1L;
                                                                    if (!isNull$77) {

                                                                        result$77 =
                                                                                (long)
                                                                                        (field$40
                                                                                                - field$44);
                                                                    }

                                                                    boolean isNull$78 =
                                                                            isNull$77 || false;
                                                                    boolean result$78 = false;
                                                                    if (!isNull$78) {

                                                                        result$78 =
                                                                                result$77
                                                                                        <= ((int)
                                                                                                60);
                                                                    }

                                                                    boolean result$79 = false;
                                                                    boolean isNull$79 = false;
                                                                    if (!isNull$76 && !result$76) {
                                                                        // left expr is false, skip
                                                                        // right expr
                                                                    } else {
                                                                        if (!isNull$76
                                                                                && !isNull$78) {
                                                                            result$79 =
                                                                                    result$76
                                                                                            && result$78;
                                                                            isNull$79 = false;
                                                                        } else if (!isNull$76
                                                                                && result$76
                                                                                && isNull$78) {
                                                                            result$79 = false;
                                                                            isNull$79 = true;
                                                                        } else if (!isNull$76
                                                                                && !result$76
                                                                                && isNull$78) {
                                                                            result$79 = false;
                                                                            isNull$79 = false;
                                                                        } else if (isNull$76
                                                                                && !isNull$78
                                                                                && result$78) {
                                                                            result$79 = false;
                                                                            isNull$79 = true;
                                                                        } else if (isNull$76
                                                                                && !isNull$78
                                                                                && !result$78) {
                                                                            result$79 = false;
                                                                            isNull$79 = false;
                                                                        } else {
                                                                            result$79 = false;
                                                                            isNull$79 = true;
                                                                        }
                                                                    }
                                                                    int result$80 = -1;
                                                                    boolean isNull$80;
                                                                    if (result$79) {

                                                                        // --- Cast section
                                                                        // generated by
                                                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                        // --- End cast section

                                                                        isNull$80 = false;
                                                                        if (!isNull$80) {
                                                                            result$80 = ((int) 1);
                                                                        }
                                                                    } else {

                                                                        // --- Cast section
                                                                        // generated by
                                                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                        // --- End cast section

                                                                        isNull$80 = false;
                                                                        if (!isNull$80) {
                                                                            result$80 = ((int) 0);
                                                                        }
                                                                    }
                                                                    // evaluate left expr

                                                                    boolean isNull$81 =
                                                                            isNull$40 || isNull$44;
                                                                    long result$81 = -1L;
                                                                    if (!isNull$81) {

                                                                        result$81 =
                                                                                (long)
                                                                                        (field$40
                                                                                                - field$44);
                                                                    }

                                                                    boolean isNull$82 =
                                                                            isNull$81 || false;
                                                                    boolean result$82 = false;
                                                                    if (!isNull$82) {

                                                                        result$82 =
                                                                                result$81
                                                                                        > ((int)
                                                                                                60);
                                                                    }

                                                                    // evaluate right expr

                                                                    boolean isNull$83 =
                                                                            isNull$40 || isNull$44;
                                                                    long result$83 = -1L;
                                                                    if (!isNull$83) {

                                                                        result$83 =
                                                                                (long)
                                                                                        (field$40
                                                                                                - field$44);
                                                                    }

                                                                    boolean isNull$84 =
                                                                            isNull$83 || false;
                                                                    boolean result$84 = false;
                                                                    if (!isNull$84) {

                                                                        result$84 =
                                                                                result$83
                                                                                        <= ((int)
                                                                                                90);
                                                                    }

                                                                    boolean result$85 = false;
                                                                    boolean isNull$85 = false;
                                                                    if (!isNull$82 && !result$82) {
                                                                        // left expr is false, skip
                                                                        // right expr
                                                                    } else {
                                                                        if (!isNull$82
                                                                                && !isNull$84) {
                                                                            result$85 =
                                                                                    result$82
                                                                                            && result$84;
                                                                            isNull$85 = false;
                                                                        } else if (!isNull$82
                                                                                && result$82
                                                                                && isNull$84) {
                                                                            result$85 = false;
                                                                            isNull$85 = true;
                                                                        } else if (!isNull$82
                                                                                && !result$82
                                                                                && isNull$84) {
                                                                            result$85 = false;
                                                                            isNull$85 = false;
                                                                        } else if (isNull$82
                                                                                && !isNull$84
                                                                                && result$84) {
                                                                            result$85 = false;
                                                                            isNull$85 = true;
                                                                        } else if (isNull$82
                                                                                && !isNull$84
                                                                                && !result$84) {
                                                                            result$85 = false;
                                                                            isNull$85 = false;
                                                                        } else {
                                                                            result$85 = false;
                                                                            isNull$85 = true;
                                                                        }
                                                                    }
                                                                    int result$86 = -1;
                                                                    boolean isNull$86;
                                                                    if (result$85) {

                                                                        // --- Cast section
                                                                        // generated by
                                                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                        // --- End cast section

                                                                        isNull$86 = false;
                                                                        if (!isNull$86) {
                                                                            result$86 = ((int) 1);
                                                                        }
                                                                    } else {

                                                                        // --- Cast section
                                                                        // generated by
                                                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                        // --- End cast section

                                                                        isNull$86 = false;
                                                                        if (!isNull$86) {
                                                                            result$86 = ((int) 0);
                                                                        }
                                                                    }
                                                                    // evaluate left expr

                                                                    boolean isNull$87 =
                                                                            isNull$40 || isNull$44;
                                                                    long result$87 = -1L;
                                                                    if (!isNull$87) {

                                                                        result$87 =
                                                                                (long)
                                                                                        (field$40
                                                                                                - field$44);
                                                                    }

                                                                    boolean isNull$88 =
                                                                            isNull$87 || false;
                                                                    boolean result$88 = false;
                                                                    if (!isNull$88) {

                                                                        result$88 =
                                                                                result$87
                                                                                        > ((int)
                                                                                                90);
                                                                    }

                                                                    // evaluate right expr

                                                                    boolean isNull$89 =
                                                                            isNull$40 || isNull$44;
                                                                    long result$89 = -1L;
                                                                    if (!isNull$89) {

                                                                        result$89 =
                                                                                (long)
                                                                                        (field$40
                                                                                                - field$44);
                                                                    }

                                                                    boolean isNull$90 =
                                                                            isNull$89 || false;
                                                                    boolean result$90 = false;
                                                                    if (!isNull$90) {

                                                                        result$90 =
                                                                                result$89
                                                                                        <= ((int)
                                                                                                120);
                                                                    }

                                                                    boolean result$91 = false;
                                                                    boolean isNull$91 = false;
                                                                    if (!isNull$88 && !result$88) {
                                                                        // left expr is false, skip
                                                                        // right expr
                                                                    } else {
                                                                        if (!isNull$88
                                                                                && !isNull$90) {
                                                                            result$91 =
                                                                                    result$88
                                                                                            && result$90;
                                                                            isNull$91 = false;
                                                                        } else if (!isNull$88
                                                                                && result$88
                                                                                && isNull$90) {
                                                                            result$91 = false;
                                                                            isNull$91 = true;
                                                                        } else if (!isNull$88
                                                                                && !result$88
                                                                                && isNull$90) {
                                                                            result$91 = false;
                                                                            isNull$91 = false;
                                                                        } else if (isNull$88
                                                                                && !isNull$90
                                                                                && result$90) {
                                                                            result$91 = false;
                                                                            isNull$91 = true;
                                                                        } else if (isNull$88
                                                                                && !isNull$90
                                                                                && !result$90) {
                                                                            result$91 = false;
                                                                            isNull$91 = false;
                                                                        } else {
                                                                            result$91 = false;
                                                                            isNull$91 = true;
                                                                        }
                                                                    }
                                                                    int result$92 = -1;
                                                                    boolean isNull$92;
                                                                    if (result$91) {

                                                                        // --- Cast section
                                                                        // generated by
                                                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                        // --- End cast section

                                                                        isNull$92 = false;
                                                                        if (!isNull$92) {
                                                                            result$92 = ((int) 1);
                                                                        }
                                                                    } else {

                                                                        // --- Cast section
                                                                        // generated by
                                                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                        // --- End cast section

                                                                        isNull$92 = false;
                                                                        if (!isNull$92) {
                                                                            result$92 = ((int) 0);
                                                                        }
                                                                    }
                                                                    boolean isNull$93 =
                                                                            isNull$40 || isNull$44;
                                                                    long result$93 = -1L;
                                                                    if (!isNull$93) {

                                                                        result$93 =
                                                                                (long)
                                                                                        (field$40
                                                                                                - field$44);
                                                                    }

                                                                    boolean isNull$94 =
                                                                            isNull$93 || false;
                                                                    boolean result$94 = false;
                                                                    if (!isNull$94) {

                                                                        result$94 =
                                                                                result$93
                                                                                        > ((int)
                                                                                                120);
                                                                    }

                                                                    int result$95 = -1;
                                                                    boolean isNull$95;
                                                                    if (result$94) {

                                                                        // --- Cast section
                                                                        // generated by
                                                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                        // --- End cast section

                                                                        isNull$95 = false;
                                                                        if (!isNull$95) {
                                                                            result$95 = ((int) 1);
                                                                        }
                                                                    } else {

                                                                        // --- Cast section
                                                                        // generated by
                                                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                        // --- End cast section

                                                                        isNull$95 = false;
                                                                        if (!isNull$95) {
                                                                            result$95 = ((int) 0);
                                                                        }
                                                                    }

                                                                    if (localAggSuppressed$136) {

                                                                        adaptiveLocalAggWriter$137
                                                                                .reset();

                                                                        if (isNull$71) {
                                                                            adaptiveLocalAggWriter$137
                                                                                    .setNullAt(0);
                                                                        } else {
                                                                            adaptiveLocalAggWriter$137
                                                                                    .writeString(
                                                                                            0,
                                                                                            result$71);
                                                                        }

                                                                        if (isNull$62) {
                                                                            adaptiveLocalAggWriter$137
                                                                                    .setNullAt(1);
                                                                        } else {
                                                                            adaptiveLocalAggWriter$137
                                                                                    .writeString(
                                                                                            1,
                                                                                            field$62);
                                                                        }

                                                                        if (isNull$56) {
                                                                            adaptiveLocalAggWriter$137
                                                                                    .setNullAt(2);
                                                                        } else {
                                                                            adaptiveLocalAggWriter$137
                                                                                    .writeString(
                                                                                            2,
                                                                                            field$56);
                                                                        }

                                                                        boolean isNull$138 =
                                                                                isNull$74;
                                                                        int field$138 = -1;
                                                                        if (!isNull$138) {
                                                                            field$138 = result$74;
                                                                        }
                                                                        // --- Cast section
                                                                        // generated by
                                                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                        // --- End cast section

                                                                        if (isNull$138) {
                                                                            adaptiveLocalAggWriter$137
                                                                                    .setNullAt(3);
                                                                        } else {
                                                                            adaptiveLocalAggWriter$137
                                                                                    .writeInt(
                                                                                            3,
                                                                                            field$138);
                                                                        }

                                                                        boolean isNull$139 =
                                                                                isNull$80;
                                                                        int field$139 = -1;
                                                                        if (!isNull$139) {
                                                                            field$139 = result$80;
                                                                        }
                                                                        // --- Cast section
                                                                        // generated by
                                                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                        // --- End cast section

                                                                        if (isNull$139) {
                                                                            adaptiveLocalAggWriter$137
                                                                                    .setNullAt(4);
                                                                        } else {
                                                                            adaptiveLocalAggWriter$137
                                                                                    .writeInt(
                                                                                            4,
                                                                                            field$139);
                                                                        }

                                                                        boolean isNull$140 =
                                                                                isNull$86;
                                                                        int field$140 = -1;
                                                                        if (!isNull$140) {
                                                                            field$140 = result$86;
                                                                        }
                                                                        // --- Cast section
                                                                        // generated by
                                                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                        // --- End cast section

                                                                        if (isNull$140) {
                                                                            adaptiveLocalAggWriter$137
                                                                                    .setNullAt(5);
                                                                        } else {
                                                                            adaptiveLocalAggWriter$137
                                                                                    .writeInt(
                                                                                            5,
                                                                                            field$140);
                                                                        }

                                                                        boolean isNull$141 =
                                                                                isNull$92;
                                                                        int field$141 = -1;
                                                                        if (!isNull$141) {
                                                                            field$141 = result$92;
                                                                        }
                                                                        // --- Cast section
                                                                        // generated by
                                                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                        // --- End cast section

                                                                        if (isNull$141) {
                                                                            adaptiveLocalAggWriter$137
                                                                                    .setNullAt(6);
                                                                        } else {
                                                                            adaptiveLocalAggWriter$137
                                                                                    .writeInt(
                                                                                            6,
                                                                                            field$141);
                                                                        }

                                                                        boolean isNull$142 =
                                                                                isNull$95;
                                                                        int field$142 = -1;
                                                                        if (!isNull$142) {
                                                                            field$142 = result$95;
                                                                        }
                                                                        // --- Cast section
                                                                        // generated by
                                                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                        // --- End cast section

                                                                        if (isNull$142) {
                                                                            adaptiveLocalAggWriter$137
                                                                                    .setNullAt(7);
                                                                        } else {
                                                                            adaptiveLocalAggWriter$137
                                                                                    .writeInt(
                                                                                            7,
                                                                                            field$142);
                                                                        }

                                                                        adaptiveLocalAggWriter$137
                                                                                .complete();

                                                                        localagg_consume$135(
                                                                                adaptiveLocalAggValue$137);
                                                                        continue;
                                                                    }

                                                                    // project key from input

                                                                    // wrap variable to row

                                                                    currentKeyWriter$97.reset();

                                                                    if (isNull$71) {
                                                                        currentKeyWriter$97
                                                                                .setNullAt(0);
                                                                    } else {
                                                                        currentKeyWriter$97
                                                                                .writeString(
                                                                                        0,
                                                                                        result$71);
                                                                    }

                                                                    if (isNull$62) {
                                                                        currentKeyWriter$97
                                                                                .setNullAt(1);
                                                                    } else {
                                                                        currentKeyWriter$97
                                                                                .writeString(
                                                                                        1,
                                                                                        field$62);
                                                                    }

                                                                    if (isNull$56) {
                                                                        currentKeyWriter$97
                                                                                .setNullAt(2);
                                                                    } else {
                                                                        currentKeyWriter$97
                                                                                .writeString(
                                                                                        2,
                                                                                        field$56);
                                                                    }

                                                                    currentKeyWriter$97.complete();

                                                                    // look up output buffer using
                                                                    // current group key
                                                                    org.apache.flink.table.runtime
                                                                                    .util
                                                                                    .collections
                                                                                    .binary.BytesMap
                                                                                    .LookupInfo
                                                                            lookupInfo$98 =
                                                                                    (org.apache
                                                                                                    .flink
                                                                                                    .table
                                                                                                    .runtime
                                                                                                    .util
                                                                                                    .collections
                                                                                                    .binary
                                                                                                    .BytesMap
                                                                                                    .LookupInfo)
                                                                                            aggregateMap$122
                                                                                                    .lookup(
                                                                                                            currentKey$97);
                                                                    org.apache.flink.table.data
                                                                                    .binary
                                                                                    .BinaryRowData
                                                                            currentAggBuffer$98 =
                                                                                    (org.apache
                                                                                                    .flink
                                                                                                    .table
                                                                                                    .data
                                                                                                    .binary
                                                                                                    .BinaryRowData)
                                                                                            lookupInfo$98
                                                                                                    .getValue();

                                                                    if (!lookupInfo$98.isFound()) {
                                                                        distinctCount$143++;

                                                                        // append empty agg buffer
                                                                        // into aggregate map for
                                                                        // current group key
                                                                        try {
                                                                            currentAggBuffer$98 =
                                                                                    aggregateMap$122
                                                                                            .append(
                                                                                                    lookupInfo$98,
                                                                                                    emptyAggBuffer$100);
                                                                        } catch (
                                                                                java.io.EOFException
                                                                                        exp) {
                                                                            LOG.info(
                                                                                    "BytesHashMap out of memory with {} entries, output directly.",
                                                                                    aggregateMap$122
                                                                                            .getNumElements());
                                                                            // hash map out of
                                                                            // memory, output
                                                                            // directly

                                                                            org.apache.flink.table
                                                                                                    .runtime
                                                                                                    .util
                                                                                                    .KeyValueIterator<
                                                                                            org
                                                                                                    .apache
                                                                                                    .flink
                                                                                                    .table
                                                                                                    .data
                                                                                                    .RowData,
                                                                                            org
                                                                                                    .apache
                                                                                                    .flink
                                                                                                    .table
                                                                                                    .data
                                                                                                    .RowData>
                                                                                    iterator$126 =
                                                                                            null;
                                                                            aggregateMap$122
                                                                                    .getEntryIterator(
                                                                                            false); // reuse key/value during iterating
                                                                            while (iterator$126
                                                                                    .advanceNext()) {
                                                                                // set result and
                                                                                // output
                                                                                reuseAggMapKey$124 =
                                                                                        (org.apache
                                                                                                        .flink
                                                                                                        .table
                                                                                                        .data
                                                                                                        .RowData)
                                                                                                iterator$126
                                                                                                        .getKey();
                                                                                reuseAggBuffer$125 =
                                                                                        (org.apache
                                                                                                        .flink
                                                                                                        .table
                                                                                                        .data
                                                                                                        .RowData)
                                                                                                iterator$126
                                                                                                        .getValue();

                                                                                // consume the row
                                                                                // of agg produce
                                                                                localagg_consume$135(
                                                                                        hashAggOutput$123
                                                                                                .replace(
                                                                                                        reuseAggMapKey$124,
                                                                                                        reuseAggBuffer$125));
                                                                            }

                                                                            // retry append

                                                                            // reset aggregate map
                                                                            // retry append
                                                                            aggregateMap$122
                                                                                    .reset();
                                                                            lookupInfo$98 =
                                                                                    (org.apache
                                                                                                    .flink
                                                                                                    .table
                                                                                                    .runtime
                                                                                                    .util
                                                                                                    .collections
                                                                                                    .binary
                                                                                                    .BytesMap
                                                                                                    .LookupInfo)
                                                                                            aggregateMap$122
                                                                                                    .lookup(
                                                                                                            currentKey$97);
                                                                            try {
                                                                                currentAggBuffer$98 =
                                                                                        aggregateMap$122
                                                                                                .append(
                                                                                                        lookupInfo$98,
                                                                                                        emptyAggBuffer$100);
                                                                            } catch (
                                                                                    java.io
                                                                                                    .EOFException
                                                                                            e) {
                                                                                throw new OutOfMemoryError(
                                                                                        "BytesHashMap Out of Memory.");
                                                                            }
                                                                        }
                                                                    }

                                                                    totalCount$144++;

                                                                    if (totalCount$144 == 500000) {
                                                                        LOG.info(
                                                                                "Local hash aggregation checkpoint reached, sampling threshold = "
                                                                                        + 500000
                                                                                        + ", distinct value count = "
                                                                                        + distinctCount$143
                                                                                        + ", total = "
                                                                                        + totalCount$144
                                                                                        + ", distinct value rate threshold = "
                                                                                        + 0.5);
                                                                        if (distinctCount$143
                                                                                        / (1.0
                                                                                                * totalCount$144)
                                                                                > 0.5) {
                                                                            LOG.info(
                                                                                    "Local hash aggregation is suppressed");
                                                                            localAggSuppressed$136 =
                                                                                    true;
                                                                        }
                                                                    }

                                                                    // aggregate buffer fields
                                                                    // access
                                                                    boolean isNull$102 =
                                                                            currentAggBuffer$98
                                                                                    .isNullAt(0);
                                                                    int field$102 =
                                                                            isNull$102
                                                                                    ? -1
                                                                                    : (currentAggBuffer$98
                                                                                            .getInt(
                                                                                                    0));
                                                                    boolean isNull$103 =
                                                                            currentAggBuffer$98
                                                                                    .isNullAt(1);
                                                                    int field$103 =
                                                                            isNull$103
                                                                                    ? -1
                                                                                    : (currentAggBuffer$98
                                                                                            .getInt(
                                                                                                    1));
                                                                    boolean isNull$104 =
                                                                            currentAggBuffer$98
                                                                                    .isNullAt(2);
                                                                    int field$104 =
                                                                            isNull$104
                                                                                    ? -1
                                                                                    : (currentAggBuffer$98
                                                                                            .getInt(
                                                                                                    2));
                                                                    boolean isNull$105 =
                                                                            currentAggBuffer$98
                                                                                    .isNullAt(3);
                                                                    int field$105 =
                                                                            isNull$105
                                                                                    ? -1
                                                                                    : (currentAggBuffer$98
                                                                                            .getInt(
                                                                                                    3));
                                                                    boolean isNull$106 =
                                                                            currentAggBuffer$98
                                                                                    .isNullAt(4);
                                                                    int field$106 =
                                                                            isNull$106
                                                                                    ? -1
                                                                                    : (currentAggBuffer$98
                                                                                            .getInt(
                                                                                                    4));
                                                                    // do aggregate and update agg
                                                                    // buffer
                                                                    int result$109 = -1;
                                                                    boolean isNull$109;
                                                                    if (false) {

                                                                        // --- Cast section
                                                                        // generated by
                                                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                        // --- End cast section

                                                                        isNull$109 = isNull$102;
                                                                        if (!isNull$109) {
                                                                            result$109 = field$102;
                                                                        }
                                                                    } else {
                                                                        int result$108 = -1;
                                                                        boolean isNull$108;
                                                                        if (isNull$102) {

                                                                            // --- Cast section
                                                                            // generated by
                                                                            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                            // --- End cast section

                                                                            isNull$108 = isNull$74;
                                                                            if (!isNull$108) {
                                                                                result$108 =
                                                                                        result$74;
                                                                            }
                                                                        } else {

                                                                            boolean isNull$107 =
                                                                                    isNull$102
                                                                                            || isNull$74;
                                                                            int result$107 = -1;
                                                                            if (!isNull$107) {

                                                                                result$107 =
                                                                                        (int)
                                                                                                (field$102
                                                                                                        + result$74);
                                                                            }

                                                                            // --- Cast section
                                                                            // generated by
                                                                            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                            // --- End cast section

                                                                            isNull$108 = isNull$107;
                                                                            if (!isNull$108) {
                                                                                result$108 =
                                                                                        result$107;
                                                                            }
                                                                        }
                                                                        // --- Cast section
                                                                        // generated by
                                                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                        // --- End cast section

                                                                        isNull$109 = isNull$108;
                                                                        if (!isNull$109) {
                                                                            result$109 = result$108;
                                                                        }
                                                                    }
                                                                    if (isNull$109) {
                                                                        currentAggBuffer$98
                                                                                .setNullAt(0);
                                                                    } else {
                                                                        currentAggBuffer$98.setInt(
                                                                                0, result$109);
                                                                    }
                                                                    int result$112 = -1;
                                                                    boolean isNull$112;
                                                                    if (false) {

                                                                        // --- Cast section
                                                                        // generated by
                                                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                        // --- End cast section

                                                                        isNull$112 = isNull$103;
                                                                        if (!isNull$112) {
                                                                            result$112 = field$103;
                                                                        }
                                                                    } else {
                                                                        int result$111 = -1;
                                                                        boolean isNull$111;
                                                                        if (isNull$103) {

                                                                            // --- Cast section
                                                                            // generated by
                                                                            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                            // --- End cast section

                                                                            isNull$111 = isNull$80;
                                                                            if (!isNull$111) {
                                                                                result$111 =
                                                                                        result$80;
                                                                            }
                                                                        } else {

                                                                            boolean isNull$110 =
                                                                                    isNull$103
                                                                                            || isNull$80;
                                                                            int result$110 = -1;
                                                                            if (!isNull$110) {

                                                                                result$110 =
                                                                                        (int)
                                                                                                (field$103
                                                                                                        + result$80);
                                                                            }

                                                                            // --- Cast section
                                                                            // generated by
                                                                            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                            // --- End cast section

                                                                            isNull$111 = isNull$110;
                                                                            if (!isNull$111) {
                                                                                result$111 =
                                                                                        result$110;
                                                                            }
                                                                        }
                                                                        // --- Cast section
                                                                        // generated by
                                                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                        // --- End cast section

                                                                        isNull$112 = isNull$111;
                                                                        if (!isNull$112) {
                                                                            result$112 = result$111;
                                                                        }
                                                                    }
                                                                    if (isNull$112) {
                                                                        currentAggBuffer$98
                                                                                .setNullAt(1);
                                                                    } else {
                                                                        currentAggBuffer$98.setInt(
                                                                                1, result$112);
                                                                    }
                                                                    int result$115 = -1;
                                                                    boolean isNull$115;
                                                                    if (false) {

                                                                        // --- Cast section
                                                                        // generated by
                                                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                        // --- End cast section

                                                                        isNull$115 = isNull$104;
                                                                        if (!isNull$115) {
                                                                            result$115 = field$104;
                                                                        }
                                                                    } else {
                                                                        int result$114 = -1;
                                                                        boolean isNull$114;
                                                                        if (isNull$104) {

                                                                            // --- Cast section
                                                                            // generated by
                                                                            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                            // --- End cast section

                                                                            isNull$114 = isNull$86;
                                                                            if (!isNull$114) {
                                                                                result$114 =
                                                                                        result$86;
                                                                            }
                                                                        } else {

                                                                            boolean isNull$113 =
                                                                                    isNull$104
                                                                                            || isNull$86;
                                                                            int result$113 = -1;
                                                                            if (!isNull$113) {

                                                                                result$113 =
                                                                                        (int)
                                                                                                (field$104
                                                                                                        + result$86);
                                                                            }

                                                                            // --- Cast section
                                                                            // generated by
                                                                            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                            // --- End cast section

                                                                            isNull$114 = isNull$113;
                                                                            if (!isNull$114) {
                                                                                result$114 =
                                                                                        result$113;
                                                                            }
                                                                        }
                                                                        // --- Cast section
                                                                        // generated by
                                                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                        // --- End cast section

                                                                        isNull$115 = isNull$114;
                                                                        if (!isNull$115) {
                                                                            result$115 = result$114;
                                                                        }
                                                                    }
                                                                    if (isNull$115) {
                                                                        currentAggBuffer$98
                                                                                .setNullAt(2);
                                                                    } else {
                                                                        currentAggBuffer$98.setInt(
                                                                                2, result$115);
                                                                    }
                                                                    int result$118 = -1;
                                                                    boolean isNull$118;
                                                                    if (false) {

                                                                        // --- Cast section
                                                                        // generated by
                                                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                        // --- End cast section

                                                                        isNull$118 = isNull$105;
                                                                        if (!isNull$118) {
                                                                            result$118 = field$105;
                                                                        }
                                                                    } else {
                                                                        int result$117 = -1;
                                                                        boolean isNull$117;
                                                                        if (isNull$105) {

                                                                            // --- Cast section
                                                                            // generated by
                                                                            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                            // --- End cast section

                                                                            isNull$117 = isNull$92;
                                                                            if (!isNull$117) {
                                                                                result$117 =
                                                                                        result$92;
                                                                            }
                                                                        } else {

                                                                            boolean isNull$116 =
                                                                                    isNull$105
                                                                                            || isNull$92;
                                                                            int result$116 = -1;
                                                                            if (!isNull$116) {

                                                                                result$116 =
                                                                                        (int)
                                                                                                (field$105
                                                                                                        + result$92);
                                                                            }

                                                                            // --- Cast section
                                                                            // generated by
                                                                            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                            // --- End cast section

                                                                            isNull$117 = isNull$116;
                                                                            if (!isNull$117) {
                                                                                result$117 =
                                                                                        result$116;
                                                                            }
                                                                        }
                                                                        // --- Cast section
                                                                        // generated by
                                                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                        // --- End cast section

                                                                        isNull$118 = isNull$117;
                                                                        if (!isNull$118) {
                                                                            result$118 = result$117;
                                                                        }
                                                                    }
                                                                    if (isNull$118) {
                                                                        currentAggBuffer$98
                                                                                .setNullAt(3);
                                                                    } else {
                                                                        currentAggBuffer$98.setInt(
                                                                                3, result$118);
                                                                    }
                                                                    int result$121 = -1;
                                                                    boolean isNull$121;
                                                                    if (false) {

                                                                        // --- Cast section
                                                                        // generated by
                                                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                        // --- End cast section

                                                                        isNull$121 = isNull$106;
                                                                        if (!isNull$121) {
                                                                            result$121 = field$106;
                                                                        }
                                                                    } else {
                                                                        int result$120 = -1;
                                                                        boolean isNull$120;
                                                                        if (isNull$106) {

                                                                            // --- Cast section
                                                                            // generated by
                                                                            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                            // --- End cast section

                                                                            isNull$120 = isNull$95;
                                                                            if (!isNull$120) {
                                                                                result$120 =
                                                                                        result$95;
                                                                            }
                                                                        } else {

                                                                            boolean isNull$119 =
                                                                                    isNull$106
                                                                                            || isNull$95;
                                                                            int result$119 = -1;
                                                                            if (!isNull$119) {

                                                                                result$119 =
                                                                                        (int)
                                                                                                (field$106
                                                                                                        + result$95);
                                                                            }

                                                                            // --- Cast section
                                                                            // generated by
                                                                            // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                            // --- End cast section

                                                                            isNull$120 = isNull$119;
                                                                            if (!isNull$120) {
                                                                                result$120 =
                                                                                        result$119;
                                                                            }
                                                                        }
                                                                        // --- Cast section
                                                                        // generated by
                                                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                        // --- End cast section

                                                                        isNull$121 = isNull$120;
                                                                        if (!isNull$121) {
                                                                            result$121 = result$120;
                                                                        }
                                                                    }
                                                                    if (isNull$121) {
                                                                        currentAggBuffer$98
                                                                                .setNullAt(4);
                                                                    } else {
                                                                        currentAggBuffer$98.setInt(
                                                                                4, result$121);
                                                                    }
                                                                    // flush result form map if
                                                                    // suppress is enable.

                                                                    if (localAggSuppressed$136) {

                                                                        org.apache.flink.table
                                                                                                .runtime
                                                                                                .util
                                                                                                .KeyValueIterator<
                                                                                        org.apache
                                                                                                .flink
                                                                                                .table
                                                                                                .data
                                                                                                .RowData,
                                                                                        org.apache
                                                                                                .flink
                                                                                                .table
                                                                                                .data
                                                                                                .RowData>
                                                                                iterator$126 = null;
                                                                        aggregateMap$122
                                                                                .getEntryIterator(
                                                                                        false); // reuse key/value during iterating
                                                                        while (iterator$126
                                                                                .advanceNext()) {
                                                                            // set result and output
                                                                            reuseAggMapKey$124 =
                                                                                    (org.apache
                                                                                                    .flink
                                                                                                    .table
                                                                                                    .data
                                                                                                    .RowData)
                                                                                            iterator$126
                                                                                                    .getKey();
                                                                            reuseAggBuffer$125 =
                                                                                    (org.apache
                                                                                                    .flink
                                                                                                    .table
                                                                                                    .data
                                                                                                    .RowData)
                                                                                            iterator$126
                                                                                                    .getValue();

                                                                            // consume the row of
                                                                            // agg produce
                                                                            localagg_consume$135(
                                                                                    hashAggOutput$123
                                                                                            .replace(
                                                                                                    reuseAggMapKey$124,
                                                                                                    reuseAggBuffer$125));
                                                                        }
                                                                    }

                                                                } while (false);
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
        hashTable$27.endBuild();
    }

    public void endInput2() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$31.endBuild();
    }

    public void endInput3() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$35.endBuild();
    }

    public void endInput4() throws Exception {

        LOG.info("Finish build phase.");
        hashTable$38.endBuild();
    }

    public void endInput5() throws Exception {

        localagg_withKeyEndInput$145();
        // call downstream endInput

    }

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.binary.BinaryRowData currentKey$97 =
            new org.apache.flink.table.data.binary.BinaryRowData(3);
    org.apache.flink.table.data.writer.BinaryRowWriter currentKeyWriter$97 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(currentKey$97);
    org.apache.flink.table.data.binary.BinaryRowData emptyAggBuffer$100 =
            new org.apache.flink.table.data.binary.BinaryRowData(5);
    org.apache.flink.table.data.writer.BinaryRowWriter emptyAggBufferWriterTerm$101 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(emptyAggBuffer$100);
    org.apache.flink.table.data.utils.JoinedRowData hashAggOutput$123 =
            new org.apache.flink.table.data.utils.JoinedRowData();
    private transient org.apache.flink.table.data.RowData reuseAggMapKey$124;
    private transient org.apache.flink.table.data.RowData reuseAggBuffer$125;

    private void localagg_consume$135(org.apache.flink.table.data.RowData hashAggOutput$123)
            throws Exception {

        // evaluate the required expr in advance

        output.collect(outElement.replace(hashAggOutput$123));
    }

    private transient boolean localAggSuppressed$136 = false;
    private org.apache.flink.table.data.binary.BinaryRowData adaptiveLocalAggValue$137 =
            new org.apache.flink.table.data.binary.BinaryRowData(8);
    private org.apache.flink.table.data.writer.BinaryRowWriter adaptiveLocalAggWriter$137 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(adaptiveLocalAggValue$137);
    private transient long distinctCount$143 = 0;
    private transient long totalCount$144 = 0;

    private void localagg_withKeyEndInput$145() throws Exception {
        if (!localAggSuppressed$136) {

            org.apache.flink.table.runtime.util.KeyValueIterator<
                            org.apache.flink.table.data.RowData,
                            org.apache.flink.table.data.RowData>
                    iterator$126 = null;
            aggregateMap$122.getEntryIterator(false); // reuse key/value during iterating
            while (iterator$126.advanceNext()) {
                // set result and output
                reuseAggMapKey$124 = (org.apache.flink.table.data.RowData) iterator$126.getKey();
                reuseAggBuffer$125 = (org.apache.flink.table.data.RowData) iterator$126.getValue();

                // consume the row of agg produce
                localagg_consume$135(
                        hashAggOutput$123.replace(reuseAggMapKey$124, reuseAggBuffer$125));
            }
        }
    }

    private transient org.apache.flink.table.types.logical.LogicalType[] groupKeyTypes$147;
    private transient org.apache.flink.table.types.logical.LogicalType[] aggBufferTypes$147;
    private transient org.apache.flink.table.runtime.util.collections.binary.BytesHashMap
            aggregateMap$122;

    private void localagg_init$150(Object[] references) throws Exception {
        groupKeyTypes$147 = (((org.apache.flink.table.types.logical.LogicalType[]) references[0]));
        aggBufferTypes$147 = (((org.apache.flink.table.types.logical.LogicalType[]) references[1]));
    }

    private transient java.lang.Object[] localagg_Refs$151;

    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$153;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$154;
    Projection$155 buildToBinaryRow$164;
    private transient java.lang.Object[] buildProjRefs$165;
    Projection$158 probeToBinaryRow$164;
    private transient java.lang.Object[] probeProjRefs$166;
    LongHashTable$167 hashTable$27;

    private void hj_init$170(Object[] references) throws Exception {
        buildSer$153 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$154 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$165 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$164 = new Projection$155(buildProjRefs$165);
        probeProjRefs$166 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$164 = new Projection$158(probeProjRefs$166);
    }

    private transient java.lang.Object[] hj_Refs$171;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$173;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$174;
    Projection$175 buildToBinaryRow$184;
    private transient java.lang.Object[] buildProjRefs$185;
    Projection$178 probeToBinaryRow$184;
    private transient java.lang.Object[] probeProjRefs$186;
    LongHashTable$187 hashTable$31;

    private void hj_init$190(Object[] references) throws Exception {
        buildSer$173 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$174 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$185 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$184 = new Projection$175(buildProjRefs$185);
        probeProjRefs$186 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$184 = new Projection$178(probeProjRefs$186);
    }

    private transient java.lang.Object[] hj_Refs$191;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$193;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$194;
    Projection$195 buildToBinaryRow$204;
    private transient java.lang.Object[] buildProjRefs$205;
    Projection$198 probeToBinaryRow$204;
    private transient java.lang.Object[] probeProjRefs$206;
    LongHashTable$207 hashTable$35;

    private void hj_init$210(Object[] references) throws Exception {
        buildSer$193 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$194 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$205 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$204 = new Projection$195(buildProjRefs$205);
        probeProjRefs$206 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$204 = new Projection$198(probeProjRefs$206);
    }

    private transient java.lang.Object[] hj_Refs$211;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$213;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$214;
    Projection$215 buildToBinaryRow$223;
    private transient java.lang.Object[] buildProjRefs$224;
    Projection$217 probeToBinaryRow$223;
    private transient java.lang.Object[] probeProjRefs$225;
    LongHashTable$226 hashTable$38;

    private void hj_init$229(Object[] references) throws Exception {
        buildSer$213 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$214 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$224 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$223 = new Projection$215(buildProjRefs$224);
        probeProjRefs$225 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$223 = new Projection$217(probeProjRefs$225);
    }

    private transient java.lang.Object[] hj_Refs$230;
    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$237;

    public BatchMultipleInputStreamOperator$238(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 5);
        this.parameters = parameters;
        localagg_Refs$151 = (((java.lang.Object[]) references[0]));
        localagg_init$150(localagg_Refs$151);
        hj_Refs$171 = (((java.lang.Object[]) references[1]));
        hj_init$170(hj_Refs$171);
        hj_Refs$191 = (((java.lang.Object[]) references[2]));
        hj_init$190(hj_Refs$191);
        hj_Refs$211 = (((java.lang.Object[]) references[3]));
        hj_init$210(hj_Refs$211);
        hj_Refs$230 = (((java.lang.Object[]) references[4]));
        hj_init$229(hj_Refs$230);
        inputSpecRefs$237 = (((java.util.ArrayList) references[5]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$237);
    }

    @Override
    public void open() throws Exception {
        super.open();

        // wrap variable to row

        emptyAggBufferWriterTerm$101.reset();

        if (true) {
            emptyAggBufferWriterTerm$101.setNullAt(0);
        } else {
            emptyAggBufferWriterTerm$101.writeInt(0, ((int) -1));
        }

        if (true) {
            emptyAggBufferWriterTerm$101.setNullAt(1);
        } else {
            emptyAggBufferWriterTerm$101.writeInt(1, ((int) -1));
        }

        if (true) {
            emptyAggBufferWriterTerm$101.setNullAt(2);
        } else {
            emptyAggBufferWriterTerm$101.writeInt(2, ((int) -1));
        }

        if (true) {
            emptyAggBufferWriterTerm$101.setNullAt(3);
        } else {
            emptyAggBufferWriterTerm$101.writeInt(3, ((int) -1));
        }

        if (true) {
            emptyAggBufferWriterTerm$101.setNullAt(4);
        } else {
            emptyAggBufferWriterTerm$101.writeInt(4, ((int) -1));
        }

        emptyAggBufferWriterTerm$101.complete();

        long memorySize$148 = computeMemorySize(0.10738255033557047);
        aggregateMap$122 =
                new org.apache.flink.table.runtime.util.collections.binary.BytesHashMap(
                        getContainingTask(),
                        getContainingTask().getEnvironment().getMemoryManager(),
                        memorySize$148,
                        groupKeyTypes$147,
                        aggBufferTypes$147);

        long memorySize$168 = computeMemorySize(0.2231543624161074);
        hashTable$27 = new LongHashTable$167(memorySize$168);
        long memorySize$188 = computeMemorySize(0.2231543624161074);
        hashTable$31 = new LongHashTable$187(memorySize$188);
        long memorySize$208 = computeMemorySize(0.2231543624161074);
        hashTable$35 = new LongHashTable$207(memorySize$208);
        long memorySize$227 = computeMemorySize(0.2231543624161074);
        hashTable$38 = new LongHashTable$226(memorySize$227);
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

        if (this.hashTable$38 != null) {
            this.hashTable$38.close();
            this.hashTable$38.free();
            this.hashTable$38 = null;
        }

        if (this.hashTable$35 != null) {
            this.hashTable$35.close();
            this.hashTable$35.free();
            this.hashTable$35 = null;
        }

        if (this.hashTable$31 != null) {
            this.hashTable$31.close();
            this.hashTable$31.free();
            this.hashTable$31 = null;
        }

        if (this.hashTable$27 != null) {
            this.hashTable$27.close();
            this.hashTable$27.free();
            this.hashTable$27 = null;
        }

        aggregateMap$122.free();
    }

    public class LongHashTable$226
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$226(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$213,
                    probeSer$214,
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
                return probeToBinaryRow$223.apply(row);
            }
        }
    }

    public class Projection$215
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$215(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$216 = in1.isNullAt(0);
            long field$216 = isNull$216 ? -1L : (in1.getLong(0));
            if (isNull$216) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$216);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$217
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(5);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$217(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$218 = in1.isNullAt(0);
            long field$218 = isNull$218 ? -1L : (in1.getLong(0));
            if (isNull$218) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$218);
            }

            boolean isNull$219 = in1.isNullAt(1);
            long field$219 = isNull$219 ? -1L : (in1.getLong(1));
            if (isNull$219) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$219);
            }

            boolean isNull$220 = in1.isNullAt(2);
            long field$220 = isNull$220 ? -1L : (in1.getLong(2));
            if (isNull$220) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeLong(2, field$220);
            }

            boolean isNull$221 = in1.isNullAt(3);
            long field$221 = isNull$221 ? -1L : (in1.getLong(3));
            if (isNull$221) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeLong(3, field$221);
            }

            boolean isNull$222 = in1.isNullAt(4);
            long field$222 = isNull$222 ? -1L : (in1.getLong(4));
            if (isNull$222) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeLong(4, field$222);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$207
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$207(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$193,
                    probeSer$194,
                    getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    getContainingTask().getEnvironment().getIOManager(),
                    20,
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
                return probeToBinaryRow$204.apply(row);
            }
        }
    }

    public class Projection$195
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(2);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$195(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$196 = in1.isNullAt(0);
            long field$196 = isNull$196 ? -1L : (in1.getLong(0));
            if (isNull$196) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$196);
            }

            boolean isNull$197 = in1.isNullAt(1);
            org.apache.flink.table.data.binary.BinaryStringData field$197 =
                    isNull$197
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(1)));
            if (isNull$197) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeString(1, field$197);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$198
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(5);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$198(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$199 = in1.isNullAt(0);
            long field$199 = isNull$199 ? -1L : (in1.getLong(0));
            if (isNull$199) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$199);
            }

            boolean isNull$200 = in1.isNullAt(1);
            long field$200 = isNull$200 ? -1L : (in1.getLong(1));
            if (isNull$200) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$200);
            }

            boolean isNull$201 = in1.isNullAt(2);
            long field$201 = isNull$201 ? -1L : (in1.getLong(2));
            if (isNull$201) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeLong(2, field$201);
            }

            boolean isNull$202 = in1.isNullAt(3);
            long field$202 = isNull$202 ? -1L : (in1.getLong(3));
            if (isNull$202) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeLong(3, field$202);
            }

            boolean isNull$203 = in1.isNullAt(4);
            long field$203 = isNull$203 ? -1L : (in1.getLong(4));
            if (isNull$203) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeLong(4, field$203);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$155
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(2);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$155(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$156 = in1.isNullAt(0);
            long field$156 = isNull$156 ? -1L : (in1.getLong(0));
            if (isNull$156) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$156);
            }

            boolean isNull$157 = in1.isNullAt(1);
            org.apache.flink.table.data.binary.BinaryStringData field$157 =
                    isNull$157
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(1)));
            if (isNull$157) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeString(1, field$157);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$158
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(5);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$158(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$159 = in1.isNullAt(0);
            long field$159 = isNull$159 ? -1L : (in1.getLong(0));
            if (isNull$159) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$159);
            }

            boolean isNull$160 = in1.isNullAt(1);
            long field$160 = isNull$160 ? -1L : (in1.getLong(1));
            if (isNull$160) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$160);
            }

            boolean isNull$161 = in1.isNullAt(2);
            long field$161 = isNull$161 ? -1L : (in1.getLong(2));
            if (isNull$161) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeLong(2, field$161);
            }

            boolean isNull$162 = in1.isNullAt(3);
            org.apache.flink.table.data.binary.BinaryStringData field$162 =
                    isNull$162
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(3)));
            if (isNull$162) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeString(3, field$162);
            }

            boolean isNull$163 = in1.isNullAt(4);
            org.apache.flink.table.data.binary.BinaryStringData field$163 =
                    isNull$163
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(4)));
            if (isNull$163) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeString(4, field$163);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$167
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$167(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$153,
                    probeSer$154,
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
                return probeToBinaryRow$164.apply(row);
            }
        }
    }

    public class Projection$178
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(5);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$178(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$179 = in1.isNullAt(0);
            long field$179 = isNull$179 ? -1L : (in1.getLong(0));
            if (isNull$179) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$179);
            }

            boolean isNull$180 = in1.isNullAt(1);
            long field$180 = isNull$180 ? -1L : (in1.getLong(1));
            if (isNull$180) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$180);
            }

            boolean isNull$181 = in1.isNullAt(2);
            long field$181 = isNull$181 ? -1L : (in1.getLong(2));
            if (isNull$181) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeLong(2, field$181);
            }

            boolean isNull$182 = in1.isNullAt(3);
            long field$182 = isNull$182 ? -1L : (in1.getLong(3));
            if (isNull$182) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeLong(3, field$182);
            }

            boolean isNull$183 = in1.isNullAt(4);
            org.apache.flink.table.data.binary.BinaryStringData field$183 =
                    isNull$183
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(4)));
            if (isNull$183) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeString(4, field$183);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$175
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(2);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$175(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$176 = in1.isNullAt(0);
            long field$176 = isNull$176 ? -1L : (in1.getLong(0));
            if (isNull$176) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$176);
            }

            boolean isNull$177 = in1.isNullAt(1);
            org.apache.flink.table.data.binary.BinaryStringData field$177 =
                    isNull$177
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(1)));
            if (isNull$177) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeString(1, field$177);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$187
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$187(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$173,
                    probeSer$174,
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
                return probeToBinaryRow$184.apply(row);
            }
        }
    }
}
