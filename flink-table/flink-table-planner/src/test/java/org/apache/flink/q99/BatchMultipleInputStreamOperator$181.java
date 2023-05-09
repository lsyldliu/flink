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

public final class BatchMultipleInputStreamOperator$181
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

        // evaluate the required expr in advance

        boolean isNull$38 = in5.isNullAt(0);
        long field$38 = isNull$38 ? -1L : (in5.getLong(0));
        boolean result$43 = !isNull$38;
        if (result$43) {

            // evaluate the required expr in advance

            // generate join key for probe side

            // find matches from hash table
            org.apache.flink.table.runtime.util.RowIterator buildIter$47 =
                    isNull$38 ? null : hashTable$37.get(field$38);
            if (buildIter$47 != null) {
                while (buildIter$47.advanceNext()) {
                    org.apache.flink.table.data.RowData buildRow$45 = buildIter$47.getRow();
                    {

                        // evaluate the required expr in advance

                        // evaluate the required expr in advance

                        // generate join key for probe side
                        boolean isNull$39 = in5.isNullAt(1);
                        long field$39 = isNull$39 ? -1L : (in5.getLong(1));
                        // find matches from hash table
                        org.apache.flink.table.runtime.util.RowIterator buildIter$53 =
                                isNull$39 ? null : hashTable$34.get(field$39);
                        if (buildIter$53 != null) {
                            while (buildIter$53.advanceNext()) {
                                org.apache.flink.table.data.RowData buildRow$50 =
                                        buildIter$53.getRow();
                                {

                                    // evaluate the required expr in advance

                                    // evaluate the required expr in advance

                                    // generate join key for probe side
                                    boolean isNull$40 = in5.isNullAt(2);
                                    long field$40 = isNull$40 ? -1L : (in5.getLong(2));
                                    // find matches from hash table
                                    org.apache.flink.table.runtime.util.RowIterator buildIter$59 =
                                            isNull$40 ? null : hashTable$30.get(field$40);
                                    if (buildIter$59 != null) {
                                        while (buildIter$59.advanceNext()) {
                                            org.apache.flink.table.data.RowData buildRow$56 =
                                                    buildIter$59.getRow();
                                            {

                                                // evaluate the required expr in advance

                                                // evaluate the required expr in advance

                                                // generate join key for probe side
                                                boolean isNull$41 = in5.isNullAt(3);
                                                long field$41 = isNull$41 ? -1L : (in5.getLong(3));
                                                // find matches from hash table
                                                org.apache.flink.table.runtime.util.RowIterator
                                                        buildIter$65 =
                                                                isNull$41
                                                                        ? null
                                                                        : hashTable$26.get(
                                                                                field$41);
                                                if (buildIter$65 != null) {
                                                    while (buildIter$65.advanceNext()) {
                                                        org.apache.flink.table.data.RowData
                                                                buildRow$62 = buildIter$65.getRow();
                                                        {

                                                            // evaluate the required expr in advance
                                                            boolean isNull$42 = in5.isNullAt(4);
                                                            long field$42 =
                                                                    isNull$42
                                                                            ? -1L
                                                                            : (in5.getLong(4));

                                                            // evaluate the required expr in advance

                                                            // wrap variable to row

                                                            boolean isNull$64 =
                                                                    buildRow$62.isNullAt(1);
                                                            org.apache.flink.table.data.binary
                                                                            .BinaryStringData
                                                                    field$64 =
                                                                            isNull$64
                                                                                    ? org.apache
                                                                                            .flink
                                                                                            .table
                                                                                            .data
                                                                                            .binary
                                                                                            .BinaryStringData
                                                                                            .EMPTY_UTF8
                                                                                    : (((org.apache
                                                                                                    .flink
                                                                                                    .table
                                                                                                    .data
                                                                                                    .binary
                                                                                                    .BinaryStringData)
                                                                                            buildRow$62
                                                                                                    .getString(
                                                                                                            1)));

                                                            boolean isNull$67 =
                                                                    isNull$64 || false || false;
                                                            org.apache.flink.table.data.binary
                                                                            .BinaryStringData
                                                                    result$67 =
                                                                            org.apache.flink.table
                                                                                    .data.binary
                                                                                    .BinaryStringData
                                                                                    .EMPTY_UTF8;
                                                            if (!isNull$67) {

                                                                result$67 =
                                                                        org.apache.flink.table.data
                                                                                .binary
                                                                                .BinaryStringDataUtil
                                                                                .substringSQL(
                                                                                        field$64,
                                                                                        ((int) 1),
                                                                                        ((int) 20));

                                                                isNull$67 = (result$67 == null);
                                                            }

                                                            if (isNull$67) {
                                                                out_inputRow$13.setNullAt(0);
                                                            } else {
                                                                out_inputRow$13
                                                                        .setNonPrimitiveValue(
                                                                                0, result$67);
                                                            }

                                                            boolean isNull$58 =
                                                                    buildRow$56.isNullAt(1);
                                                            org.apache.flink.table.data.binary
                                                                            .BinaryStringData
                                                                    field$58 =
                                                                            isNull$58
                                                                                    ? org.apache
                                                                                            .flink
                                                                                            .table
                                                                                            .data
                                                                                            .binary
                                                                                            .BinaryStringData
                                                                                            .EMPTY_UTF8
                                                                                    : (((org.apache
                                                                                                    .flink
                                                                                                    .table
                                                                                                    .data
                                                                                                    .binary
                                                                                                    .BinaryStringData)
                                                                                            buildRow$56
                                                                                                    .getString(
                                                                                                            1)));
                                                            if (isNull$58) {
                                                                out_inputRow$13.setNullAt(1);
                                                            } else {
                                                                out_inputRow$13
                                                                        .setNonPrimitiveValue(
                                                                                1, field$58);
                                                            }

                                                            boolean isNull$52 =
                                                                    buildRow$50.isNullAt(1);
                                                            org.apache.flink.table.data.binary
                                                                            .BinaryStringData
                                                                    field$52 =
                                                                            isNull$52
                                                                                    ? org.apache
                                                                                            .flink
                                                                                            .table
                                                                                            .data
                                                                                            .binary
                                                                                            .BinaryStringData
                                                                                            .EMPTY_UTF8
                                                                                    : (((org.apache
                                                                                                    .flink
                                                                                                    .table
                                                                                                    .data
                                                                                                    .binary
                                                                                                    .BinaryStringData)
                                                                                            buildRow$50
                                                                                                    .getString(
                                                                                                            1)));
                                                            if (isNull$52) {
                                                                out_inputRow$13.setNullAt(2);
                                                            } else {
                                                                out_inputRow$13
                                                                        .setNonPrimitiveValue(
                                                                                2, field$52);
                                                            }

                                                            boolean isNull$68 =
                                                                    isNull$38 || isNull$42;
                                                            long result$68 = -1L;
                                                            if (!isNull$68) {

                                                                result$68 =
                                                                        (long)
                                                                                (field$38
                                                                                        - field$42);
                                                            }

                                                            boolean isNull$69 = isNull$68 || false;
                                                            boolean result$69 = false;
                                                            if (!isNull$69) {

                                                                result$69 = result$68 <= ((int) 30);
                                                            }

                                                            int result$70 = -1;
                                                            boolean isNull$70;
                                                            if (result$69) {

                                                                // --- Cast section generated by
                                                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                // --- End cast section

                                                                isNull$70 = false;
                                                                if (!isNull$70) {
                                                                    result$70 = ((int) 1);
                                                                }
                                                            } else {

                                                                // --- Cast section generated by
                                                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                // --- End cast section

                                                                isNull$70 = false;
                                                                if (!isNull$70) {
                                                                    result$70 = ((int) 0);
                                                                }
                                                            }
                                                            out_inputRow$13.setInt(3, result$70);

                                                            // evaluate left expr

                                                            boolean isNull$71 =
                                                                    isNull$38 || isNull$42;
                                                            long result$71 = -1L;
                                                            if (!isNull$71) {

                                                                result$71 =
                                                                        (long)
                                                                                (field$38
                                                                                        - field$42);
                                                            }

                                                            boolean isNull$72 = isNull$71 || false;
                                                            boolean result$72 = false;
                                                            if (!isNull$72) {

                                                                result$72 = result$71 > ((int) 30);
                                                            }

                                                            // evaluate right expr

                                                            boolean isNull$73 =
                                                                    isNull$38 || isNull$42;
                                                            long result$73 = -1L;
                                                            if (!isNull$73) {

                                                                result$73 =
                                                                        (long)
                                                                                (field$38
                                                                                        - field$42);
                                                            }

                                                            boolean isNull$74 = isNull$73 || false;
                                                            boolean result$74 = false;
                                                            if (!isNull$74) {

                                                                result$74 = result$73 <= ((int) 60);
                                                            }

                                                            boolean result$75 = false;
                                                            boolean isNull$75 = false;
                                                            if (!isNull$72 && !result$72) {
                                                                // left expr is false, skip right
                                                                // expr
                                                            } else {
                                                                if (!isNull$72 && !isNull$74) {
                                                                    result$75 =
                                                                            result$72 && result$74;
                                                                    isNull$75 = false;
                                                                } else if (!isNull$72
                                                                        && result$72
                                                                        && isNull$74) {
                                                                    result$75 = false;
                                                                    isNull$75 = true;
                                                                } else if (!isNull$72
                                                                        && !result$72
                                                                        && isNull$74) {
                                                                    result$75 = false;
                                                                    isNull$75 = false;
                                                                } else if (isNull$72
                                                                        && !isNull$74
                                                                        && result$74) {
                                                                    result$75 = false;
                                                                    isNull$75 = true;
                                                                } else if (isNull$72
                                                                        && !isNull$74
                                                                        && !result$74) {
                                                                    result$75 = false;
                                                                    isNull$75 = false;
                                                                } else {
                                                                    result$75 = false;
                                                                    isNull$75 = true;
                                                                }
                                                            }
                                                            int result$76 = -1;
                                                            boolean isNull$76;
                                                            if (result$75) {

                                                                // --- Cast section generated by
                                                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                // --- End cast section

                                                                isNull$76 = false;
                                                                if (!isNull$76) {
                                                                    result$76 = ((int) 1);
                                                                }
                                                            } else {

                                                                // --- Cast section generated by
                                                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                // --- End cast section

                                                                isNull$76 = false;
                                                                if (!isNull$76) {
                                                                    result$76 = ((int) 0);
                                                                }
                                                            }
                                                            out_inputRow$13.setInt(4, result$76);

                                                            // evaluate left expr

                                                            boolean isNull$77 =
                                                                    isNull$38 || isNull$42;
                                                            long result$77 = -1L;
                                                            if (!isNull$77) {

                                                                result$77 =
                                                                        (long)
                                                                                (field$38
                                                                                        - field$42);
                                                            }

                                                            boolean isNull$78 = isNull$77 || false;
                                                            boolean result$78 = false;
                                                            if (!isNull$78) {

                                                                result$78 = result$77 > ((int) 60);
                                                            }

                                                            // evaluate right expr

                                                            boolean isNull$79 =
                                                                    isNull$38 || isNull$42;
                                                            long result$79 = -1L;
                                                            if (!isNull$79) {

                                                                result$79 =
                                                                        (long)
                                                                                (field$38
                                                                                        - field$42);
                                                            }

                                                            boolean isNull$80 = isNull$79 || false;
                                                            boolean result$80 = false;
                                                            if (!isNull$80) {

                                                                result$80 = result$79 <= ((int) 90);
                                                            }

                                                            boolean result$81 = false;
                                                            boolean isNull$81 = false;
                                                            if (!isNull$78 && !result$78) {
                                                                // left expr is false, skip right
                                                                // expr
                                                            } else {
                                                                if (!isNull$78 && !isNull$80) {
                                                                    result$81 =
                                                                            result$78 && result$80;
                                                                    isNull$81 = false;
                                                                } else if (!isNull$78
                                                                        && result$78
                                                                        && isNull$80) {
                                                                    result$81 = false;
                                                                    isNull$81 = true;
                                                                } else if (!isNull$78
                                                                        && !result$78
                                                                        && isNull$80) {
                                                                    result$81 = false;
                                                                    isNull$81 = false;
                                                                } else if (isNull$78
                                                                        && !isNull$80
                                                                        && result$80) {
                                                                    result$81 = false;
                                                                    isNull$81 = true;
                                                                } else if (isNull$78
                                                                        && !isNull$80
                                                                        && !result$80) {
                                                                    result$81 = false;
                                                                    isNull$81 = false;
                                                                } else {
                                                                    result$81 = false;
                                                                    isNull$81 = true;
                                                                }
                                                            }
                                                            int result$82 = -1;
                                                            boolean isNull$82;
                                                            if (result$81) {

                                                                // --- Cast section generated by
                                                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                // --- End cast section

                                                                isNull$82 = false;
                                                                if (!isNull$82) {
                                                                    result$82 = ((int) 1);
                                                                }
                                                            } else {

                                                                // --- Cast section generated by
                                                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                // --- End cast section

                                                                isNull$82 = false;
                                                                if (!isNull$82) {
                                                                    result$82 = ((int) 0);
                                                                }
                                                            }
                                                            out_inputRow$13.setInt(5, result$82);

                                                            // evaluate left expr

                                                            boolean isNull$83 =
                                                                    isNull$38 || isNull$42;
                                                            long result$83 = -1L;
                                                            if (!isNull$83) {

                                                                result$83 =
                                                                        (long)
                                                                                (field$38
                                                                                        - field$42);
                                                            }

                                                            boolean isNull$84 = isNull$83 || false;
                                                            boolean result$84 = false;
                                                            if (!isNull$84) {

                                                                result$84 = result$83 > ((int) 90);
                                                            }

                                                            // evaluate right expr

                                                            boolean isNull$85 =
                                                                    isNull$38 || isNull$42;
                                                            long result$85 = -1L;
                                                            if (!isNull$85) {

                                                                result$85 =
                                                                        (long)
                                                                                (field$38
                                                                                        - field$42);
                                                            }

                                                            boolean isNull$86 = isNull$85 || false;
                                                            boolean result$86 = false;
                                                            if (!isNull$86) {

                                                                result$86 =
                                                                        result$85 <= ((int) 120);
                                                            }

                                                            boolean result$87 = false;
                                                            boolean isNull$87 = false;
                                                            if (!isNull$84 && !result$84) {
                                                                // left expr is false, skip right
                                                                // expr
                                                            } else {
                                                                if (!isNull$84 && !isNull$86) {
                                                                    result$87 =
                                                                            result$84 && result$86;
                                                                    isNull$87 = false;
                                                                } else if (!isNull$84
                                                                        && result$84
                                                                        && isNull$86) {
                                                                    result$87 = false;
                                                                    isNull$87 = true;
                                                                } else if (!isNull$84
                                                                        && !result$84
                                                                        && isNull$86) {
                                                                    result$87 = false;
                                                                    isNull$87 = false;
                                                                } else if (isNull$84
                                                                        && !isNull$86
                                                                        && result$86) {
                                                                    result$87 = false;
                                                                    isNull$87 = true;
                                                                } else if (isNull$84
                                                                        && !isNull$86
                                                                        && !result$86) {
                                                                    result$87 = false;
                                                                    isNull$87 = false;
                                                                } else {
                                                                    result$87 = false;
                                                                    isNull$87 = true;
                                                                }
                                                            }
                                                            int result$88 = -1;
                                                            boolean isNull$88;
                                                            if (result$87) {

                                                                // --- Cast section generated by
                                                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                // --- End cast section

                                                                isNull$88 = false;
                                                                if (!isNull$88) {
                                                                    result$88 = ((int) 1);
                                                                }
                                                            } else {

                                                                // --- Cast section generated by
                                                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                // --- End cast section

                                                                isNull$88 = false;
                                                                if (!isNull$88) {
                                                                    result$88 = ((int) 0);
                                                                }
                                                            }
                                                            out_inputRow$13.setInt(6, result$88);

                                                            boolean isNull$89 =
                                                                    isNull$38 || isNull$42;
                                                            long result$89 = -1L;
                                                            if (!isNull$89) {

                                                                result$89 =
                                                                        (long)
                                                                                (field$38
                                                                                        - field$42);
                                                            }

                                                            boolean isNull$90 = isNull$89 || false;
                                                            boolean result$90 = false;
                                                            if (!isNull$90) {

                                                                result$90 = result$89 > ((int) 120);
                                                            }

                                                            int result$91 = -1;
                                                            boolean isNull$91;
                                                            if (result$90) {

                                                                // --- Cast section generated by
                                                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                // --- End cast section

                                                                isNull$91 = false;
                                                                if (!isNull$91) {
                                                                    result$91 = ((int) 1);
                                                                }
                                                            } else {

                                                                // --- Cast section generated by
                                                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                                                // --- End cast section

                                                                isNull$91 = false;
                                                                if (!isNull$91) {
                                                                    result$91 = ((int) 0);
                                                                }
                                                            }
                                                            out_inputRow$13.setInt(7, result$91);

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
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$96;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$97;
    Projection$98 buildToBinaryRow$107;
    private transient java.lang.Object[] buildProjRefs$108;
    Projection$101 probeToBinaryRow$107;
    private transient java.lang.Object[] probeProjRefs$109;
    LongHashTable$110 hashTable$26;

    private void hj_init$113(Object[] references) throws Exception {
        buildSer$96 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$97 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$108 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$107 = new Projection$98(buildProjRefs$108);
        probeProjRefs$109 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$107 = new Projection$101(probeProjRefs$109);
    }

    private transient java.lang.Object[] hj_Refs$114;

    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$116;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$117;
    Projection$118 buildToBinaryRow$127;
    private transient java.lang.Object[] buildProjRefs$128;
    Projection$121 probeToBinaryRow$127;
    private transient java.lang.Object[] probeProjRefs$129;
    LongHashTable$130 hashTable$30;

    private void hj_init$133(Object[] references) throws Exception {
        buildSer$116 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$117 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$128 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$127 = new Projection$118(buildProjRefs$128);
        probeProjRefs$129 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$127 = new Projection$121(probeProjRefs$129);
    }

    private transient java.lang.Object[] hj_Refs$134;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$136;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$137;
    Projection$138 buildToBinaryRow$147;
    private transient java.lang.Object[] buildProjRefs$148;
    Projection$141 probeToBinaryRow$147;
    private transient java.lang.Object[] probeProjRefs$149;
    LongHashTable$150 hashTable$34;

    private void hj_init$153(Object[] references) throws Exception {
        buildSer$136 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$137 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$148 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$147 = new Projection$138(buildProjRefs$148);
        probeProjRefs$149 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$147 = new Projection$141(probeProjRefs$149);
    }

    private transient java.lang.Object[] hj_Refs$154;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$156;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$157;
    Projection$158 buildToBinaryRow$166;
    private transient java.lang.Object[] buildProjRefs$167;
    Projection$160 probeToBinaryRow$166;
    private transient java.lang.Object[] probeProjRefs$168;
    LongHashTable$169 hashTable$37;

    private void hj_init$172(Object[] references) throws Exception {
        buildSer$156 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$157 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$167 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$166 = new Projection$158(buildProjRefs$167);
        probeProjRefs$168 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$166 = new Projection$160(probeProjRefs$168);
    }

    private transient java.lang.Object[] hj_Refs$173;
    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$180;

    public BatchMultipleInputStreamOperator$181(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 5);
        this.parameters = parameters;
        hj_Refs$114 = (((java.lang.Object[]) references[0]));
        hj_init$113(hj_Refs$114);
        hj_Refs$134 = (((java.lang.Object[]) references[1]));
        hj_init$133(hj_Refs$134);
        hj_Refs$154 = (((java.lang.Object[]) references[2]));
        hj_init$153(hj_Refs$154);
        hj_Refs$173 = (((java.lang.Object[]) references[3]));
        hj_init$172(hj_Refs$173);
        inputSpecRefs$180 = (((java.util.ArrayList) references[4]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$180);
    }

    @Override
    public void open() throws Exception {
        super.open();

        long memorySize$111 = computeMemorySize(0.25);
        hashTable$26 = new LongHashTable$110(memorySize$111);
        long memorySize$131 = computeMemorySize(0.25);
        hashTable$30 = new LongHashTable$130(memorySize$131);
        long memorySize$151 = computeMemorySize(0.25);
        hashTable$34 = new LongHashTable$150(memorySize$151);
        long memorySize$170 = computeMemorySize(0.25);
        hashTable$37 = new LongHashTable$169(memorySize$170);
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

    public class LongHashTable$150
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$150(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$136,
                    probeSer$137,
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
                return probeToBinaryRow$147.apply(row);
            }
        }
    }

    public class Projection$141
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(5);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$141(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$142 = in1.isNullAt(0);
            long field$142 = isNull$142 ? -1L : (in1.getLong(0));
            if (isNull$142) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$142);
            }

            boolean isNull$143 = in1.isNullAt(1);
            long field$143 = isNull$143 ? -1L : (in1.getLong(1));
            if (isNull$143) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$143);
            }

            boolean isNull$144 = in1.isNullAt(2);
            long field$144 = isNull$144 ? -1L : (in1.getLong(2));
            if (isNull$144) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeLong(2, field$144);
            }

            boolean isNull$145 = in1.isNullAt(3);
            long field$145 = isNull$145 ? -1L : (in1.getLong(3));
            if (isNull$145) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeLong(3, field$145);
            }

            boolean isNull$146 = in1.isNullAt(4);
            long field$146 = isNull$146 ? -1L : (in1.getLong(4));
            if (isNull$146) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeLong(4, field$146);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$138
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(2);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$138(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$139 = in1.isNullAt(0);
            long field$139 = isNull$139 ? -1L : (in1.getLong(0));
            if (isNull$139) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$139);
            }

            boolean isNull$140 = in1.isNullAt(1);
            org.apache.flink.table.data.binary.BinaryStringData field$140 =
                    isNull$140
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(1)));
            if (isNull$140) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeString(1, field$140);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$101
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(5);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$101(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$102 = in1.isNullAt(0);
            long field$102 = isNull$102 ? -1L : (in1.getLong(0));
            if (isNull$102) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$102);
            }

            boolean isNull$103 = in1.isNullAt(1);
            long field$103 = isNull$103 ? -1L : (in1.getLong(1));
            if (isNull$103) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$103);
            }

            boolean isNull$104 = in1.isNullAt(2);
            long field$104 = isNull$104 ? -1L : (in1.getLong(2));
            if (isNull$104) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeLong(2, field$104);
            }

            boolean isNull$105 = in1.isNullAt(3);
            org.apache.flink.table.data.binary.BinaryStringData field$105 =
                    isNull$105
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(3)));
            if (isNull$105) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeString(3, field$105);
            }

            boolean isNull$106 = in1.isNullAt(4);
            org.apache.flink.table.data.binary.BinaryStringData field$106 =
                    isNull$106
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(4)));
            if (isNull$106) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeString(4, field$106);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$98
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(2);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$98(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$99 = in1.isNullAt(0);
            long field$99 = isNull$99 ? -1L : (in1.getLong(0));
            if (isNull$99) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$99);
            }

            boolean isNull$100 = in1.isNullAt(1);
            org.apache.flink.table.data.binary.BinaryStringData field$100 =
                    isNull$100
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(1)));
            if (isNull$100) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeString(1, field$100);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$110
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$110(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$96,
                    probeSer$97,
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
                return probeToBinaryRow$107.apply(row);
            }
        }
    }

    public class Projection$158
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(1);
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

            outWriter.complete();

            return out;
        }
    }

    public class Projection$160
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(5);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$160(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$161 = in1.isNullAt(0);
            long field$161 = isNull$161 ? -1L : (in1.getLong(0));
            if (isNull$161) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$161);
            }

            boolean isNull$162 = in1.isNullAt(1);
            long field$162 = isNull$162 ? -1L : (in1.getLong(1));
            if (isNull$162) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$162);
            }

            boolean isNull$163 = in1.isNullAt(2);
            long field$163 = isNull$163 ? -1L : (in1.getLong(2));
            if (isNull$163) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeLong(2, field$163);
            }

            boolean isNull$164 = in1.isNullAt(3);
            long field$164 = isNull$164 ? -1L : (in1.getLong(3));
            if (isNull$164) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeLong(3, field$164);
            }

            boolean isNull$165 = in1.isNullAt(4);
            long field$165 = isNull$165 ? -1L : (in1.getLong(4));
            if (isNull$165) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeLong(4, field$165);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$169
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$169(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$156,
                    probeSer$157,
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
                return probeToBinaryRow$166.apply(row);
            }
        }
    }

    public class LongHashTable$130
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$130(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$116,
                    probeSer$117,
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
                return probeToBinaryRow$127.apply(row);
            }
        }
    }

    public class Projection$121
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(5);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$121(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$122 = in1.isNullAt(0);
            long field$122 = isNull$122 ? -1L : (in1.getLong(0));
            if (isNull$122) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$122);
            }

            boolean isNull$123 = in1.isNullAt(1);
            long field$123 = isNull$123 ? -1L : (in1.getLong(1));
            if (isNull$123) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeLong(1, field$123);
            }

            boolean isNull$124 = in1.isNullAt(2);
            long field$124 = isNull$124 ? -1L : (in1.getLong(2));
            if (isNull$124) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeLong(2, field$124);
            }

            boolean isNull$125 = in1.isNullAt(3);
            long field$125 = isNull$125 ? -1L : (in1.getLong(3));
            if (isNull$125) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeLong(3, field$125);
            }

            boolean isNull$126 = in1.isNullAt(4);
            org.apache.flink.table.data.binary.BinaryStringData field$126 =
                    isNull$126
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(4)));
            if (isNull$126) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeString(4, field$126);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$118
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(2);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$118(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$119 = in1.isNullAt(0);
            long field$119 = isNull$119 ? -1L : (in1.getLong(0));
            if (isNull$119) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$119);
            }

            boolean isNull$120 = in1.isNullAt(1);
            org.apache.flink.table.data.binary.BinaryStringData field$120 =
                    isNull$120
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(1)));
            if (isNull$120) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeString(1, field$120);
            }

            outWriter.complete();

            return out;
        }
    }
}
