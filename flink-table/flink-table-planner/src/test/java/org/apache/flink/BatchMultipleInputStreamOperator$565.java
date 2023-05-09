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

package org.apache.flink;

/** q35 left outer join */
public final class BatchMultipleInputStreamOperator$565
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

        // project key from input

        // wrap variable to row

        currentKeyWriter$361.reset();

        boolean isNull$360 = in2.isNullAt(0);
        long field$360 = isNull$360 ? -1L : (in2.getLong(0));
        if (isNull$360) {
            currentKeyWriter$361.setNullAt(0);
        } else {
            currentKeyWriter$361.writeLong(0, field$360);
        }

        currentKeyWriter$361.complete();

        // look up output buffer using current group key
        org.apache.flink.table.runtime.util.collections.binary.BytesMap.LookupInfo lookupInfo$362 =
                (org.apache.flink.table.runtime.util.collections.binary.BytesMap.LookupInfo)
                        aggregateMap$366.lookup(currentKey$361);
        org.apache.flink.table.data.binary.BinaryRowData currentAggBuffer$362 =
                (org.apache.flink.table.data.binary.BinaryRowData) lookupInfo$362.getValue();

        if (!lookupInfo$362.isFound()) {

            // append empty agg buffer into aggregate map for current group key
            try {
                currentAggBuffer$362 = aggregateMap$366.append(lookupInfo$362, emptyAggBuffer$364);
            } catch (java.io.EOFException exp) {
                throw new OutOfMemoryError(
                        "Global HashAgg doesn't support fallback to sort-based agg currently.");
            }
        }
        // aggregate buffer fields access

        // do aggregate and update agg buffer

    }

    public void processInput3(org.apache.flink.table.data.RowData in3) throws Exception {

        // evaluate the required expr in advance

        // project key from input

        // wrap variable to row

        currentKeyWriter$368.reset();

        boolean isNull$367 = in3.isNullAt(0);
        long field$367 = isNull$367 ? -1L : (in3.getLong(0));
        if (isNull$367) {
            currentKeyWriter$368.setNullAt(0);
        } else {
            currentKeyWriter$368.writeLong(0, field$367);
        }

        currentKeyWriter$368.complete();

        // look up output buffer using current group key
        org.apache.flink.table.runtime.util.collections.binary.BytesMap.LookupInfo lookupInfo$369 =
                (org.apache.flink.table.runtime.util.collections.binary.BytesMap.LookupInfo)
                        aggregateMap$373.lookup(currentKey$368);
        org.apache.flink.table.data.binary.BinaryRowData currentAggBuffer$369 =
                (org.apache.flink.table.data.binary.BinaryRowData) lookupInfo$369.getValue();

        if (!lookupInfo$369.isFound()) {

            // append empty agg buffer into aggregate map for current group key
            try {
                currentAggBuffer$369 = aggregateMap$373.append(lookupInfo$369, emptyAggBuffer$371);
            } catch (java.io.EOFException exp) {
                throw new OutOfMemoryError(
                        "Global HashAgg doesn't support fallback to sort-based agg currently.");
            }
        }
        // aggregate buffer fields access

        // do aggregate and update agg buffer

        // wrap variable to row

    }

    public void processInput1(org.apache.flink.table.data.RowData in1) throws Exception {

        // evaluate the required expr in advance

        // generate join key for probe side
        boolean isNull$374 = in1.isNullAt(0);
        long field$374 = isNull$374 ? -1L : (in1.getLong(0));

        boolean found$388 = false;
        // find matches from hash table
        org.apache.flink.table.runtime.util.RowIterator buildIter$387 =
                isNull$374 ? null : hashTable$389.get(field$374);
        while ((buildIter$387 != null && buildIter$387.advanceNext()) || !found$388) {
            org.apache.flink.table.data.RowData buildRow$381 =
                    buildIter$387 != null ? buildIter$387.getRow() : null;
            final boolean conditionPassed$386 = true;
            if (conditionPassed$386) {
                found$388 = true;

                // evaluate the required expr in advance

                // evaluate the required expr in advance

                // generate join key for probe side

                boolean found$399 = false;
                // find matches from hash table
                org.apache.flink.table.runtime.util.RowIterator buildIter$398 =
                        isNull$374 ? null : hashTable$400.get(field$374);
                while ((buildIter$398 != null && buildIter$398.advanceNext()) || !found$399) {
                    org.apache.flink.table.data.RowData buildRow$392 =
                            buildIter$398 != null ? buildIter$398.getRow() : null;
                    final boolean conditionPassed$397 = true;
                    if (conditionPassed$397) {
                        found$399 = true;

                        // evaluate the required expr in advance

                        boolean isNull$385 = true;
                        boolean field$385 = false;
                        if (buildRow$381 != null) {
                            boolean isNull$383 = buildRow$381.isNullAt(1);
                            boolean field$383 = isNull$383 ? false : (buildRow$381.getBoolean(1));
                            isNull$385 = isNull$383;
                            field$385 = field$383;
                        }

                        boolean isNull$396 = true;
                        boolean field$396 = false;
                        if (buildRow$392 != null) {
                            boolean isNull$394 = buildRow$392.isNullAt(1);
                            boolean field$394 = isNull$394 ? false : (buildRow$392.getBoolean(1));
                            isNull$396 = isNull$394;
                            field$396 = field$394;
                        }

                        boolean result$402 = true;
                        boolean isNull$402 = false;
                        if (!false && true) {
                            // left expr is true, skip right expr
                        } else {
                            if (!false && !false) {
                                result$402 = true || true;
                                isNull$402 = false;
                            } else if (!false && true && false) {
                                result$402 = true;
                                isNull$402 = false;
                            } else if (!false && !true && false) {
                                result$402 = false;
                                isNull$402 = true;
                            } else if (false && !false && true) {
                                result$402 = true;
                                isNull$402 = false;
                            } else if (false && !false && !true) {
                                result$402 = false;
                                isNull$402 = true;
                            } else {
                                result$402 = false;
                                isNull$402 = true;
                            }
                        }
                        if (result$402) {

                            // evaluate the required expr in advance

                            // project key from input

                            // wrap variable to row

                            currentKeyWriter$404.reset();

                            boolean isNull$380 = in1.isNullAt(6);
                            org.apache.flink.table.data.binary.BinaryStringData field$380 =
                                    isNull$380
                                            ? org.apache.flink.table.data.binary.BinaryStringData
                                                    .EMPTY_UTF8
                                            : (((org.apache.flink.table.data.binary
                                                            .BinaryStringData)
                                                    in1.getString(6)));
                            if (isNull$380) {
                                currentKeyWriter$404.setNullAt(0);
                            } else {
                                currentKeyWriter$404.writeString(0, field$380);
                            }

                            boolean isNull$375 = in1.isNullAt(1);
                            org.apache.flink.table.data.binary.BinaryStringData field$375 =
                                    isNull$375
                                            ? org.apache.flink.table.data.binary.BinaryStringData
                                                    .EMPTY_UTF8
                                            : (((org.apache.flink.table.data.binary
                                                            .BinaryStringData)
                                                    in1.getString(1)));
                            if (isNull$375) {
                                currentKeyWriter$404.setNullAt(1);
                            } else {
                                currentKeyWriter$404.writeString(1, field$375);
                            }

                            boolean isNull$376 = in1.isNullAt(2);
                            org.apache.flink.table.data.binary.BinaryStringData field$376 =
                                    isNull$376
                                            ? org.apache.flink.table.data.binary.BinaryStringData
                                                    .EMPTY_UTF8
                                            : (((org.apache.flink.table.data.binary
                                                            .BinaryStringData)
                                                    in1.getString(2)));
                            if (isNull$376) {
                                currentKeyWriter$404.setNullAt(2);
                            } else {
                                currentKeyWriter$404.writeString(2, field$376);
                            }

                            boolean isNull$377 = in1.isNullAt(3);
                            int field$377 = isNull$377 ? -1 : (in1.getInt(3));
                            if (isNull$377) {
                                currentKeyWriter$404.setNullAt(3);
                            } else {
                                currentKeyWriter$404.writeInt(3, field$377);
                            }

                            boolean isNull$378 = in1.isNullAt(4);
                            int field$378 = isNull$378 ? -1 : (in1.getInt(4));
                            if (isNull$378) {
                                currentKeyWriter$404.setNullAt(4);
                            } else {
                                currentKeyWriter$404.writeInt(4, field$378);
                            }

                            boolean isNull$379 = in1.isNullAt(5);
                            int field$379 = isNull$379 ? -1 : (in1.getInt(5));
                            if (isNull$379) {
                                currentKeyWriter$404.setNullAt(5);
                            } else {
                                currentKeyWriter$404.writeInt(5, field$379);
                            }

                            currentKeyWriter$404.complete();

                            // look up output buffer using current group key
                            org.apache.flink.table.runtime.util.collections.binary.BytesMap
                                            .LookupInfo
                                    lookupInfo$405 =
                                            (org.apache.flink.table.runtime.util.collections.binary
                                                            .BytesMap.LookupInfo)
                                                    aggregateMap$456.lookup(currentKey$404);
                            org.apache.flink.table.data.binary.BinaryRowData currentAggBuffer$405 =
                                    (org.apache.flink.table.data.binary.BinaryRowData)
                                            lookupInfo$405.getValue();

                            if (!lookupInfo$405.isFound()) {

                                // append empty agg buffer into aggregate map for current group key
                                try {
                                    currentAggBuffer$405 =
                                            aggregateMap$456.append(
                                                    lookupInfo$405, emptyAggBuffer$407);
                                } catch (java.io.EOFException exp) {
                                    LOG.info(
                                            "BytesHashMap out of memory with {} entries, output directly.",
                                            aggregateMap$456.getNumElements());
                                    // hash map out of memory, output directly

                                    org.apache.flink.table.runtime.util.KeyValueIterator<
                                                    org.apache.flink.table.data.RowData,
                                                    org.apache.flink.table.data.RowData>
                                            iterator$460 = null;
                                    aggregateMap$456.getEntryIterator(
                                            false); // reuse key/value during
                                    // iterating
                                    while (iterator$460.advanceNext()) {
                                        // set result and output
                                        reuseAggMapKey$458 =
                                                (org.apache.flink.table.data.RowData)
                                                        iterator$460.getKey();
                                        reuseAggBuffer$459 =
                                                (org.apache.flink.table.data.RowData)
                                                        iterator$460.getValue();

                                        // consume the row of agg produce
                                        localagg_consume$480(
                                                hashAggOutput$457.replace(
                                                        reuseAggMapKey$458, reuseAggBuffer$459));
                                    }

                                    // retry append

                                    // reset aggregate map retry append
                                    aggregateMap$456.reset();
                                    lookupInfo$405 =
                                            (org.apache.flink.table.runtime.util.collections.binary
                                                            .BytesMap.LookupInfo)
                                                    aggregateMap$456.lookup(currentKey$404);
                                    try {
                                        currentAggBuffer$405 =
                                                aggregateMap$456.append(
                                                        lookupInfo$405, emptyAggBuffer$407);
                                    } catch (java.io.EOFException e) {
                                        throw new OutOfMemoryError("BytesHashMap Out of Memory.");
                                    }
                                }
                            }
                            // aggregate buffer fields access
                            boolean isNull$409 = currentAggBuffer$405.isNullAt(0);
                            long field$409 = isNull$409 ? -1L : (currentAggBuffer$405.getLong(0));
                            boolean isNull$410 = currentAggBuffer$405.isNullAt(1);
                            long field$410 = isNull$410 ? -1L : (currentAggBuffer$405.getLong(1));
                            boolean isNull$411 = currentAggBuffer$405.isNullAt(2);
                            long field$411 = isNull$411 ? -1L : (currentAggBuffer$405.getLong(2));
                            boolean isNull$412 = currentAggBuffer$405.isNullAt(3);
                            int field$412 = isNull$412 ? -1 : (currentAggBuffer$405.getInt(3));
                            boolean isNull$413 = currentAggBuffer$405.isNullAt(4);
                            int field$413 = isNull$413 ? -1 : (currentAggBuffer$405.getInt(4));
                            boolean isNull$414 = currentAggBuffer$405.isNullAt(5);
                            long field$414 = isNull$414 ? -1L : (currentAggBuffer$405.getLong(5));
                            boolean isNull$415 = currentAggBuffer$405.isNullAt(6);
                            long field$415 = isNull$415 ? -1L : (currentAggBuffer$405.getLong(6));
                            boolean isNull$416 = currentAggBuffer$405.isNullAt(7);
                            int field$416 = isNull$416 ? -1 : (currentAggBuffer$405.getInt(7));
                            boolean isNull$417 = currentAggBuffer$405.isNullAt(8);
                            int field$417 = isNull$417 ? -1 : (currentAggBuffer$405.getInt(8));
                            boolean isNull$418 = currentAggBuffer$405.isNullAt(9);
                            long field$418 = isNull$418 ? -1L : (currentAggBuffer$405.getLong(9));
                            boolean isNull$419 = currentAggBuffer$405.isNullAt(10);
                            long field$419 = isNull$419 ? -1L : (currentAggBuffer$405.getLong(10));
                            boolean isNull$420 = currentAggBuffer$405.isNullAt(11);
                            int field$420 = isNull$420 ? -1 : (currentAggBuffer$405.getInt(11));
                            boolean isNull$421 = currentAggBuffer$405.isNullAt(12);
                            int field$421 = isNull$421 ? -1 : (currentAggBuffer$405.getInt(12));
                            // do aggregate and update agg buffer
                            boolean isNull$422 = isNull$409 || false;
                            long result$422 = -1L;
                            if (!isNull$422) {

                                result$422 = (long) (field$409 + ((long) 1L));
                            }

                            if (isNull$422) {
                                currentAggBuffer$405.setNullAt(0);
                            } else {
                                currentAggBuffer$405.setLong(0, result$422);
                            }
                            long result$424 = -1L;
                            boolean isNull$424;
                            if (isNull$377) {

                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$424 = isNull$410;
                                if (!isNull$424) {
                                    result$424 = field$410;
                                }
                            } else {

                                boolean isNull$423 = isNull$410 || isNull$377;
                                long result$423 = -1L;
                                if (!isNull$423) {

                                    result$423 = (long) (field$410 + ((long) (field$377)));
                                }

                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$424 = isNull$423;
                                if (!isNull$424) {
                                    result$424 = result$423;
                                }
                            }
                            if (isNull$424) {
                                currentAggBuffer$405.setNullAt(1);
                            } else {
                                currentAggBuffer$405.setLong(1, result$424);
                            }
                            long result$426 = -1L;
                            boolean isNull$426;
                            if (isNull$377) {

                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$426 = isNull$411;
                                if (!isNull$426) {
                                    result$426 = field$411;
                                }
                            } else {

                                boolean isNull$425 = isNull$411 || false;
                                long result$425 = -1L;
                                if (!isNull$425) {

                                    result$425 = (long) (field$411 + ((long) 1L));
                                }

                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$426 = isNull$425;
                                if (!isNull$426) {
                                    result$426 = result$425;
                                }
                            }
                            if (isNull$426) {
                                currentAggBuffer$405.setNullAt(2);
                            } else {
                                currentAggBuffer$405.setLong(2, result$426);
                            }
                            int result$430 = -1;
                            boolean isNull$430;
                            if (isNull$377) {

                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$430 = isNull$412;
                                if (!isNull$430) {
                                    result$430 = field$412;
                                }
                            } else {
                                int result$429 = -1;
                                boolean isNull$429;
                                if (isNull$412) {

                                    // --- Cast section generated by
                                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                    // --- End cast section

                                    isNull$429 = isNull$377;
                                    if (!isNull$429) {
                                        result$429 = field$377;
                                    }
                                } else {
                                    boolean isNull$427 = isNull$377 || isNull$412;
                                    boolean result$427 = false;
                                    if (!isNull$427) {

                                        result$427 = field$377 > field$412;
                                    }

                                    int result$428 = -1;
                                    boolean isNull$428;
                                    if (result$427) {

                                        // --- Cast section generated by
                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                        // --- End cast section

                                        isNull$428 = isNull$377;
                                        if (!isNull$428) {
                                            result$428 = field$377;
                                        }
                                    } else {

                                        // --- Cast section generated by
                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                        // --- End cast section

                                        isNull$428 = isNull$412;
                                        if (!isNull$428) {
                                            result$428 = field$412;
                                        }
                                    }
                                    // --- Cast section generated by
                                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                    // --- End cast section

                                    isNull$429 = isNull$428;
                                    if (!isNull$429) {
                                        result$429 = result$428;
                                    }
                                }
                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$430 = isNull$429;
                                if (!isNull$430) {
                                    result$430 = result$429;
                                }
                            }
                            if (isNull$430) {
                                currentAggBuffer$405.setNullAt(3);
                            } else {
                                currentAggBuffer$405.setInt(3, result$430);
                            }
                            int result$433 = -1;
                            boolean isNull$433;
                            if (isNull$377) {

                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$433 = isNull$413;
                                if (!isNull$433) {
                                    result$433 = field$413;
                                }
                            } else {
                                int result$432 = -1;
                                boolean isNull$432;
                                if (isNull$413) {

                                    // --- Cast section generated by
                                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                    // --- End cast section

                                    isNull$432 = isNull$377;
                                    if (!isNull$432) {
                                        result$432 = field$377;
                                    }
                                } else {

                                    boolean isNull$431 = isNull$413 || isNull$377;
                                    int result$431 = -1;
                                    if (!isNull$431) {

                                        result$431 = (int) (field$413 + field$377);
                                    }

                                    // --- Cast section generated by
                                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                    // --- End cast section

                                    isNull$432 = isNull$431;
                                    if (!isNull$432) {
                                        result$432 = result$431;
                                    }
                                }
                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$433 = isNull$432;
                                if (!isNull$433) {
                                    result$433 = result$432;
                                }
                            }
                            if (isNull$433) {
                                currentAggBuffer$405.setNullAt(4);
                            } else {
                                currentAggBuffer$405.setInt(4, result$433);
                            }
                            long result$435 = -1L;
                            boolean isNull$435;
                            if (isNull$378) {

                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$435 = isNull$414;
                                if (!isNull$435) {
                                    result$435 = field$414;
                                }
                            } else {

                                boolean isNull$434 = isNull$414 || isNull$378;
                                long result$434 = -1L;
                                if (!isNull$434) {

                                    result$434 = (long) (field$414 + ((long) (field$378)));
                                }

                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$435 = isNull$434;
                                if (!isNull$435) {
                                    result$435 = result$434;
                                }
                            }
                            if (isNull$435) {
                                currentAggBuffer$405.setNullAt(5);
                            } else {
                                currentAggBuffer$405.setLong(5, result$435);
                            }
                            long result$437 = -1L;
                            boolean isNull$437;
                            if (isNull$378) {

                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$437 = isNull$415;
                                if (!isNull$437) {
                                    result$437 = field$415;
                                }
                            } else {

                                boolean isNull$436 = isNull$415 || false;
                                long result$436 = -1L;
                                if (!isNull$436) {

                                    result$436 = (long) (field$415 + ((long) 1L));
                                }

                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$437 = isNull$436;
                                if (!isNull$437) {
                                    result$437 = result$436;
                                }
                            }
                            if (isNull$437) {
                                currentAggBuffer$405.setNullAt(6);
                            } else {
                                currentAggBuffer$405.setLong(6, result$437);
                            }
                            int result$441 = -1;
                            boolean isNull$441;
                            if (isNull$378) {

                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$441 = isNull$416;
                                if (!isNull$441) {
                                    result$441 = field$416;
                                }
                            } else {
                                int result$440 = -1;
                                boolean isNull$440;
                                if (isNull$416) {

                                    // --- Cast section generated by
                                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                    // --- End cast section

                                    isNull$440 = isNull$378;
                                    if (!isNull$440) {
                                        result$440 = field$378;
                                    }
                                } else {
                                    boolean isNull$438 = isNull$378 || isNull$416;
                                    boolean result$438 = false;
                                    if (!isNull$438) {

                                        result$438 = field$378 > field$416;
                                    }

                                    int result$439 = -1;
                                    boolean isNull$439;
                                    if (result$438) {

                                        // --- Cast section generated by
                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                        // --- End cast section

                                        isNull$439 = isNull$378;
                                        if (!isNull$439) {
                                            result$439 = field$378;
                                        }
                                    } else {

                                        // --- Cast section generated by
                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                        // --- End cast section

                                        isNull$439 = isNull$416;
                                        if (!isNull$439) {
                                            result$439 = field$416;
                                        }
                                    }
                                    // --- Cast section generated by
                                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                    // --- End cast section

                                    isNull$440 = isNull$439;
                                    if (!isNull$440) {
                                        result$440 = result$439;
                                    }
                                }
                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$441 = isNull$440;
                                if (!isNull$441) {
                                    result$441 = result$440;
                                }
                            }
                            if (isNull$441) {
                                currentAggBuffer$405.setNullAt(7);
                            } else {
                                currentAggBuffer$405.setInt(7, result$441);
                            }
                            int result$444 = -1;
                            boolean isNull$444;
                            if (isNull$378) {

                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$444 = isNull$417;
                                if (!isNull$444) {
                                    result$444 = field$417;
                                }
                            } else {
                                int result$443 = -1;
                                boolean isNull$443;
                                if (isNull$417) {

                                    // --- Cast section generated by
                                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                    // --- End cast section

                                    isNull$443 = isNull$378;
                                    if (!isNull$443) {
                                        result$443 = field$378;
                                    }
                                } else {

                                    boolean isNull$442 = isNull$417 || isNull$378;
                                    int result$442 = -1;
                                    if (!isNull$442) {

                                        result$442 = (int) (field$417 + field$378);
                                    }

                                    // --- Cast section generated by
                                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                    // --- End cast section

                                    isNull$443 = isNull$442;
                                    if (!isNull$443) {
                                        result$443 = result$442;
                                    }
                                }
                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$444 = isNull$443;
                                if (!isNull$444) {
                                    result$444 = result$443;
                                }
                            }
                            if (isNull$444) {
                                currentAggBuffer$405.setNullAt(8);
                            } else {
                                currentAggBuffer$405.setInt(8, result$444);
                            }
                            long result$446 = -1L;
                            boolean isNull$446;
                            if (isNull$379) {

                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$446 = isNull$418;
                                if (!isNull$446) {
                                    result$446 = field$418;
                                }
                            } else {

                                boolean isNull$445 = isNull$418 || isNull$379;
                                long result$445 = -1L;
                                if (!isNull$445) {

                                    result$445 = (long) (field$418 + ((long) (field$379)));
                                }

                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$446 = isNull$445;
                                if (!isNull$446) {
                                    result$446 = result$445;
                                }
                            }
                            if (isNull$446) {
                                currentAggBuffer$405.setNullAt(9);
                            } else {
                                currentAggBuffer$405.setLong(9, result$446);
                            }
                            long result$448 = -1L;
                            boolean isNull$448;
                            if (isNull$379) {

                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$448 = isNull$419;
                                if (!isNull$448) {
                                    result$448 = field$419;
                                }
                            } else {

                                boolean isNull$447 = isNull$419 || false;
                                long result$447 = -1L;
                                if (!isNull$447) {

                                    result$447 = (long) (field$419 + ((long) 1L));
                                }

                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$448 = isNull$447;
                                if (!isNull$448) {
                                    result$448 = result$447;
                                }
                            }
                            if (isNull$448) {
                                currentAggBuffer$405.setNullAt(10);
                            } else {
                                currentAggBuffer$405.setLong(10, result$448);
                            }
                            int result$452 = -1;
                            boolean isNull$452;
                            if (isNull$379) {

                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$452 = isNull$420;
                                if (!isNull$452) {
                                    result$452 = field$420;
                                }
                            } else {
                                int result$451 = -1;
                                boolean isNull$451;
                                if (isNull$420) {

                                    // --- Cast section generated by
                                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                    // --- End cast section

                                    isNull$451 = isNull$379;
                                    if (!isNull$451) {
                                        result$451 = field$379;
                                    }
                                } else {
                                    boolean isNull$449 = isNull$379 || isNull$420;
                                    boolean result$449 = false;
                                    if (!isNull$449) {

                                        result$449 = field$379 > field$420;
                                    }

                                    int result$450 = -1;
                                    boolean isNull$450;
                                    if (result$449) {

                                        // --- Cast section generated by
                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                        // --- End cast section

                                        isNull$450 = isNull$379;
                                        if (!isNull$450) {
                                            result$450 = field$379;
                                        }
                                    } else {

                                        // --- Cast section generated by
                                        // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                        // --- End cast section

                                        isNull$450 = isNull$420;
                                        if (!isNull$450) {
                                            result$450 = field$420;
                                        }
                                    }
                                    // --- Cast section generated by
                                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                    // --- End cast section

                                    isNull$451 = isNull$450;
                                    if (!isNull$451) {
                                        result$451 = result$450;
                                    }
                                }
                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$452 = isNull$451;
                                if (!isNull$452) {
                                    result$452 = result$451;
                                }
                            }
                            if (isNull$452) {
                                currentAggBuffer$405.setNullAt(11);
                            } else {
                                currentAggBuffer$405.setInt(11, result$452);
                            }
                            int result$455 = -1;
                            boolean isNull$455;
                            if (isNull$379) {

                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$455 = isNull$421;
                                if (!isNull$455) {
                                    result$455 = field$421;
                                }
                            } else {
                                int result$454 = -1;
                                boolean isNull$454;
                                if (isNull$421) {

                                    // --- Cast section generated by
                                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                    // --- End cast section

                                    isNull$454 = isNull$379;
                                    if (!isNull$454) {
                                        result$454 = field$379;
                                    }
                                } else {

                                    boolean isNull$453 = isNull$421 || isNull$379;
                                    int result$453 = -1;
                                    if (!isNull$453) {

                                        result$453 = (int) (field$421 + field$379);
                                    }

                                    // --- Cast section generated by
                                    // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                    // --- End cast section

                                    isNull$454 = isNull$453;
                                    if (!isNull$454) {
                                        result$454 = result$453;
                                    }
                                }
                                // --- Cast section generated by
                                // org.apache.flink.table.planner.functions.casting.IdentityCastRule

                                // --- End cast section

                                isNull$455 = isNull$454;
                                if (!isNull$455) {
                                    result$455 = result$454;
                                }
                            }
                            if (isNull$455) {
                                currentAggBuffer$405.setNullAt(12);
                            } else {
                                currentAggBuffer$405.setInt(12, result$455);
                            }
                        }
                    }
                }
            }
        }
    }

    public void endInput2() throws Exception {

        hashagg_withKeyEndInput$488();
        // call downstream endInput

        LOG.info("Finish build phase.");
        hashTable$400.endBuild();
    }

    public void endInput3() throws Exception {

        hashagg_withKeyEndInput$496();
        // call downstream endInput

        LOG.info("Finish build phase.");
        hashTable$389.endBuild();
    }

    public void endInput1() throws Exception {

        localagg_withKeyEndInput$497();
        // call downstream endInput

    }

    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement =
            new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);
    org.apache.flink.table.data.binary.BinaryRowData currentKey$404 =
            new org.apache.flink.table.data.binary.BinaryRowData(6);
    org.apache.flink.table.data.writer.BinaryRowWriter currentKeyWriter$404 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(currentKey$404);
    org.apache.flink.table.data.binary.BinaryRowData emptyAggBuffer$407 =
            new org.apache.flink.table.data.binary.BinaryRowData(13);
    org.apache.flink.table.data.writer.BinaryRowWriter emptyAggBufferWriterTerm$408 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(emptyAggBuffer$407);
    org.apache.flink.table.data.utils.JoinedRowData hashAggOutput$457 =
            new org.apache.flink.table.data.utils.JoinedRowData();
    private transient org.apache.flink.table.data.RowData reuseAggMapKey$458;
    private transient org.apache.flink.table.data.RowData reuseAggBuffer$459;

    private void localagg_consume$480(org.apache.flink.table.data.RowData hashAggOutput$457)
            throws Exception {

        // evaluate the required expr in advance

        output.collect(outElement.replace(hashAggOutput$457));
    }

    private void localagg_withKeyEndInput$497() throws Exception {

        org.apache.flink.table.runtime.util.KeyValueIterator<
                        org.apache.flink.table.data.RowData, org.apache.flink.table.data.RowData>
                iterator$460 = null;
        aggregateMap$456.getEntryIterator(false); // reuse key/value during iterating
        while (iterator$460.advanceNext()) {
            // set result and output
            reuseAggMapKey$458 = (org.apache.flink.table.data.RowData) iterator$460.getKey();
            reuseAggBuffer$459 = (org.apache.flink.table.data.RowData) iterator$460.getValue();

            // consume the row of agg produce
            localagg_consume$480(hashAggOutput$457.replace(reuseAggMapKey$458, reuseAggBuffer$459));
        }
    }

    private transient org.apache.flink.table.types.logical.LogicalType[] groupKeyTypes$499;
    private transient org.apache.flink.table.types.logical.LogicalType[] aggBufferTypes$499;
    private transient org.apache.flink.table.runtime.util.collections.binary.BytesHashMap
            aggregateMap$456;

    private void localagg_init$502(Object[] references) throws Exception {
        groupKeyTypes$499 = (((org.apache.flink.table.types.logical.LogicalType[]) references[0]));
        aggBufferTypes$499 = (((org.apache.flink.table.types.logical.LogicalType[]) references[1]));
    }

    private transient java.lang.Object[] localagg_Refs$503;

    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$505;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$506;
    Projection$507 buildToBinaryRow$519;
    private transient java.lang.Object[] buildProjRefs$520;
    Projection$510 probeToBinaryRow$519;
    private transient java.lang.Object[] probeProjRefs$521;
    LongHashTable$522 hashTable$400;

    private void hj_init$525(Object[] references) throws Exception {
        buildSer$505 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$506 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$520 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$519 = new Projection$507(buildProjRefs$520);
        probeProjRefs$521 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$519 = new Projection$510(probeProjRefs$521);
    }

    private transient java.lang.Object[] hj_Refs$526;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer buildSer$528;
    private transient org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer probeSer$529;
    Projection$530 buildToBinaryRow$541;
    private transient java.lang.Object[] buildProjRefs$542;
    Projection$533 probeToBinaryRow$541;
    private transient java.lang.Object[] probeProjRefs$543;
    LongHashTable$544 hashTable$389;

    private void hj_init$547(Object[] references) throws Exception {
        buildSer$528 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[0]));
        probeSer$529 =
                (((org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer)
                        references[1]));
        buildProjRefs$542 = (((java.lang.Object[]) references[2]));
        buildToBinaryRow$541 = new Projection$530(buildProjRefs$542);
        probeProjRefs$543 = (((java.lang.Object[]) references[3]));
        probeToBinaryRow$541 = new Projection$533(probeProjRefs$543);
    }

    private transient java.lang.Object[] hj_Refs$548;
    org.apache.flink.table.data.binary.BinaryRowData hj_inputRow$353 =
            new org.apache.flink.table.data.binary.BinaryRowData(2);
    org.apache.flink.table.data.writer.BinaryRowWriter hj_inputWriter$495 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(hj_inputRow$353);
    org.apache.flink.table.data.binary.BinaryRowData currentKey$368 =
            new org.apache.flink.table.data.binary.BinaryRowData(1);
    org.apache.flink.table.data.writer.BinaryRowWriter currentKeyWriter$368 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(currentKey$368);
    org.apache.flink.table.data.binary.BinaryRowData emptyAggBuffer$371 =
            new org.apache.flink.table.data.binary.BinaryRowData(0);
    org.apache.flink.table.data.writer.BinaryRowWriter emptyAggBufferWriterTerm$372 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(emptyAggBuffer$371);

    private void hashagg_withKeyEndInput$496() throws Exception {

        org.apache.flink.table.data.RowData reuseAggMapKey$489;
        org.apache.flink.table.data.RowData reuseAggBuffer$489;
        org.apache.flink.table.runtime.util.KeyValueIterator<
                        org.apache.flink.table.data.RowData, org.apache.flink.table.data.RowData>
                iterator$490 = null;
        aggregateMap$373.getEntryIterator(false); // reuse key/value during iterating
        while (iterator$490.advanceNext()) {
            // set result and output
            reuseAggMapKey$489 = (org.apache.flink.table.data.RowData) iterator$490.getKey();
            reuseAggBuffer$489 = (org.apache.flink.table.data.RowData) iterator$490.getValue();

            // consume the row of agg produce

            // evaluate the required expr in advance

            // evaluate the required expr in advance

            boolean anyNull$494 = false;
            boolean isNull$491 = reuseAggMapKey$489.isNullAt(0);
            long field$491 = isNull$491 ? -1L : (reuseAggMapKey$489.getLong(0));

            anyNull$494 |= isNull$491;

            if (!anyNull$494) {

                // wrap variable to row

                hj_inputWriter$495.reset();

                if (isNull$491) {
                    hj_inputWriter$495.setNullAt(0);
                } else {
                    hj_inputWriter$495.writeLong(0, field$491);
                }

                if (false) {
                    hj_inputWriter$495.setNullAt(1);
                } else {
                    hj_inputWriter$495.writeBoolean(1, ((boolean) true));
                }

                hj_inputWriter$495.complete();

                hashTable$389.putBuildRow(
                        (org.apache.flink.table.data.binary.BinaryRowData) hj_inputRow$353);
            }
        }
    }

    private transient org.apache.flink.table.types.logical.LogicalType[] groupKeyTypes$551;
    private transient org.apache.flink.table.types.logical.LogicalType[] aggBufferTypes$551;
    private transient org.apache.flink.table.runtime.util.collections.binary.BytesHashMap
            aggregateMap$373;

    private void hashagg_init$554(Object[] references) throws Exception {
        groupKeyTypes$551 = (((org.apache.flink.table.types.logical.LogicalType[]) references[0]));
        aggBufferTypes$551 = (((org.apache.flink.table.types.logical.LogicalType[]) references[1]));
    }

    private transient java.lang.Object[] hashagg_Refs$555;
    org.apache.flink.table.data.binary.BinaryRowData hj_inputRow$357 =
            new org.apache.flink.table.data.binary.BinaryRowData(2);
    org.apache.flink.table.data.writer.BinaryRowWriter hj_inputWriter$487 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(hj_inputRow$357);
    org.apache.flink.table.data.binary.BinaryRowData currentKey$361 =
            new org.apache.flink.table.data.binary.BinaryRowData(1);
    org.apache.flink.table.data.writer.BinaryRowWriter currentKeyWriter$361 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(currentKey$361);
    org.apache.flink.table.data.binary.BinaryRowData emptyAggBuffer$364 =
            new org.apache.flink.table.data.binary.BinaryRowData(0);
    org.apache.flink.table.data.writer.BinaryRowWriter emptyAggBufferWriterTerm$365 =
            new org.apache.flink.table.data.writer.BinaryRowWriter(emptyAggBuffer$364);

    private void hashagg_withKeyEndInput$488() throws Exception {

        org.apache.flink.table.data.RowData reuseAggMapKey$481;
        org.apache.flink.table.data.RowData reuseAggBuffer$481;
        org.apache.flink.table.runtime.util.KeyValueIterator<
                        org.apache.flink.table.data.RowData, org.apache.flink.table.data.RowData>
                iterator$482 = null;
        aggregateMap$366.getEntryIterator(false); // reuse key/value during iterating
        while (iterator$482.advanceNext()) {
            // set result and output
            reuseAggMapKey$481 = (org.apache.flink.table.data.RowData) iterator$482.getKey();
            reuseAggBuffer$481 = (org.apache.flink.table.data.RowData) iterator$482.getValue();

            // consume the row of agg produce

            // evaluate the required expr in advance

            // evaluate the required expr in advance

            boolean anyNull$486 = false;
            boolean isNull$483 = reuseAggMapKey$481.isNullAt(0);
            long field$483 = isNull$483 ? -1L : (reuseAggMapKey$481.getLong(0));

            anyNull$486 |= isNull$483;

            if (!anyNull$486) {

                // wrap variable to row

                hj_inputWriter$487.reset();

                if (isNull$483) {
                    hj_inputWriter$487.setNullAt(0);
                } else {
                    hj_inputWriter$487.writeLong(0, field$483);
                }

                if (false) {
                    hj_inputWriter$487.setNullAt(1);
                } else {
                    hj_inputWriter$487.writeBoolean(1, ((boolean) true));
                }

                hj_inputWriter$487.complete();

                hashTable$400.putBuildRow(
                        (org.apache.flink.table.data.binary.BinaryRowData) hj_inputRow$357);
            }
        }
    }

    private transient org.apache.flink.table.types.logical.LogicalType[] groupKeyTypes$558;
    private transient org.apache.flink.table.types.logical.LogicalType[] aggBufferTypes$558;
    private transient org.apache.flink.table.runtime.util.collections.binary.BytesHashMap
            aggregateMap$366;

    private void hashagg_init$561(Object[] references) throws Exception {
        groupKeyTypes$558 = (((org.apache.flink.table.types.logical.LogicalType[]) references[0]));
        aggBufferTypes$558 = (((org.apache.flink.table.types.logical.LogicalType[]) references[1]));
    }

    private transient java.lang.Object[] hashagg_Refs$562;
    private final org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters;
    private final org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionHandler
            inputSelectionHandler;
    private transient java.util.ArrayList inputSpecRefs$564;

    public BatchMultipleInputStreamOperator$565(
            Object[] references,
            org.apache.flink.streaming.api.operators.StreamOperatorParameters parameters)
            throws Exception {
        super(parameters, 3);
        this.parameters = parameters;
        localagg_Refs$503 = (((java.lang.Object[]) references[0]));
        localagg_init$502(localagg_Refs$503);
        hj_Refs$526 = (((java.lang.Object[]) references[1]));
        hj_init$525(hj_Refs$526);
        hj_Refs$548 = (((java.lang.Object[]) references[2]));
        hj_init$547(hj_Refs$548);
        hashagg_Refs$555 = (((java.lang.Object[]) references[3]));
        hashagg_init$554(hashagg_Refs$555);
        hashagg_Refs$562 = (((java.lang.Object[]) references[4]));
        hashagg_init$561(hashagg_Refs$562);
        inputSpecRefs$564 = (((java.util.ArrayList) references[5]));
        this.inputSelectionHandler =
                new org.apache.flink.table.runtime.operators.multipleinput.input
                        .InputSelectionHandler(inputSpecRefs$564);
    }

    @Override
    public void open() throws Exception {
        super.open();

        // wrap variable to row

        emptyAggBufferWriterTerm$408.reset();

        if (false) {
            emptyAggBufferWriterTerm$408.setNullAt(0);
        } else {
            emptyAggBufferWriterTerm$408.writeLong(0, ((long) 0L));
        }

        if (false) {
            emptyAggBufferWriterTerm$408.setNullAt(1);
        } else {
            emptyAggBufferWriterTerm$408.writeLong(1, ((long) 0L));
        }

        if (false) {
            emptyAggBufferWriterTerm$408.setNullAt(2);
        } else {
            emptyAggBufferWriterTerm$408.writeLong(2, ((long) 0L));
        }

        if (true) {
            emptyAggBufferWriterTerm$408.setNullAt(3);
        } else {
            emptyAggBufferWriterTerm$408.writeInt(3, ((int) -1));
        }

        if (true) {
            emptyAggBufferWriterTerm$408.setNullAt(4);
        } else {
            emptyAggBufferWriterTerm$408.writeInt(4, ((int) -1));
        }

        if (false) {
            emptyAggBufferWriterTerm$408.setNullAt(5);
        } else {
            emptyAggBufferWriterTerm$408.writeLong(5, ((long) 0L));
        }

        if (false) {
            emptyAggBufferWriterTerm$408.setNullAt(6);
        } else {
            emptyAggBufferWriterTerm$408.writeLong(6, ((long) 0L));
        }

        if (true) {
            emptyAggBufferWriterTerm$408.setNullAt(7);
        } else {
            emptyAggBufferWriterTerm$408.writeInt(7, ((int) -1));
        }

        if (true) {
            emptyAggBufferWriterTerm$408.setNullAt(8);
        } else {
            emptyAggBufferWriterTerm$408.writeInt(8, ((int) -1));
        }

        if (false) {
            emptyAggBufferWriterTerm$408.setNullAt(9);
        } else {
            emptyAggBufferWriterTerm$408.writeLong(9, ((long) 0L));
        }

        if (false) {
            emptyAggBufferWriterTerm$408.setNullAt(10);
        } else {
            emptyAggBufferWriterTerm$408.writeLong(10, ((long) 0L));
        }

        if (true) {
            emptyAggBufferWriterTerm$408.setNullAt(11);
        } else {
            emptyAggBufferWriterTerm$408.writeInt(11, ((int) -1));
        }

        if (true) {
            emptyAggBufferWriterTerm$408.setNullAt(12);
        } else {
            emptyAggBufferWriterTerm$408.writeInt(12, ((int) -1));
        }

        emptyAggBufferWriterTerm$408.complete();

        long memorySize$500 = computeMemorySize(0.13973799126637554);
        aggregateMap$456 =
                new org.apache.flink.table.runtime.util.collections.binary.BytesHashMap(
                        getContainingTask(),
                        getContainingTask().getEnvironment().getMemoryManager(),
                        memorySize$500,
                        groupKeyTypes$499,
                        aggBufferTypes$499);

        long memorySize$523 = computeMemorySize(0.2903930131004367);
        hashTable$400 = new LongHashTable$522(memorySize$523);
        long memorySize$545 = computeMemorySize(0.2903930131004367);
        hashTable$389 = new LongHashTable$544(memorySize$545);

        // wrap variable to row

        emptyAggBufferWriterTerm$372.reset();

        emptyAggBufferWriterTerm$372.complete();

        long memorySize$552 = computeMemorySize(0.13973799126637554);
        aggregateMap$373 =
                new org.apache.flink.table.runtime.util.collections.binary.BytesHashMap(
                        getContainingTask(),
                        getContainingTask().getEnvironment().getMemoryManager(),
                        memorySize$552,
                        groupKeyTypes$551,
                        aggBufferTypes$551);

        // wrap variable to row

        emptyAggBufferWriterTerm$365.reset();

        emptyAggBufferWriterTerm$365.complete();

        long memorySize$559 = computeMemorySize(0.13973799126637554);
        aggregateMap$366 =
                new org.apache.flink.table.runtime.util.collections.binary.BytesHashMap(
                        getContainingTask(),
                        getContainingTask().getEnvironment().getMemoryManager(),
                        memorySize$559,
                        groupKeyTypes$558,
                        aggBufferTypes$558);
    }

    @Override
    public java.util.List getInputs() {
        return java.util.Arrays.asList(
                new org.apache.flink.streaming.api.operators.AbstractInput(this, 1) {
                    @Override
                    public void processElement(
                            org.apache.flink.streaming.runtime.streamrecord.StreamRecord element)
                            throws Exception {
                        // InputType: ROW<`c_customer_sk` BIGINT NOT NULL, `cd_gender`
                        // VARCHAR(2147483647), `cd_marital_status` VARCHAR(2147483647),
                        // `cd_dep_count` INT, `cd_dep_employed_count` INT, `cd_dep_college_count`
                        // INT, `ca_state` VARCHAR(2147483647)>
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
                        // InputType: ROW<`cs_ship_customer_sk` BIGINT>
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
                        // InputType: ROW<`ws_bill_customer_sk` BIGINT>
                        org.apache.flink.table.data.RowData in3 =
                                (org.apache.flink.table.data.RowData) element.getValue();
                        processInput3(in3);
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

            case 3:
                endInput3();
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

        aggregateMap$366.free();
        aggregateMap$373.free();

        if (this.hashTable$389 != null) {
            this.hashTable$389.close();
            this.hashTable$389.free();
            this.hashTable$389 = null;
        }

        if (this.hashTable$400 != null) {
            this.hashTable$400.close();
            this.hashTable$400.free();
            this.hashTable$400 = null;
        }

        aggregateMap$456.free();
    }

    public class Projection$530
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(2);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$530(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$531 = in1.isNullAt(0);
            long field$531 = isNull$531 ? -1L : (in1.getLong(0));
            if (isNull$531) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$531);
            }

            boolean isNull$532 = in1.isNullAt(1);
            boolean field$532 = isNull$532 ? false : (in1.getBoolean(1));
            if (isNull$532) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeBoolean(1, field$532);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$533
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(7);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$533(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$534 = in1.isNullAt(0);
            long field$534 = isNull$534 ? -1L : (in1.getLong(0));
            if (isNull$534) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$534);
            }

            boolean isNull$535 = in1.isNullAt(1);
            org.apache.flink.table.data.binary.BinaryStringData field$535 =
                    isNull$535
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(1)));
            if (isNull$535) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeString(1, field$535);
            }

            boolean isNull$536 = in1.isNullAt(2);
            org.apache.flink.table.data.binary.BinaryStringData field$536 =
                    isNull$536
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(2)));
            if (isNull$536) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeString(2, field$536);
            }

            boolean isNull$537 = in1.isNullAt(3);
            int field$537 = isNull$537 ? -1 : (in1.getInt(3));
            if (isNull$537) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeInt(3, field$537);
            }

            boolean isNull$538 = in1.isNullAt(4);
            int field$538 = isNull$538 ? -1 : (in1.getInt(4));
            if (isNull$538) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeInt(4, field$538);
            }

            boolean isNull$539 = in1.isNullAt(5);
            int field$539 = isNull$539 ? -1 : (in1.getInt(5));
            if (isNull$539) {
                outWriter.setNullAt(5);
            } else {
                outWriter.writeInt(5, field$539);
            }

            boolean isNull$540 = in1.isNullAt(6);
            org.apache.flink.table.data.binary.BinaryStringData field$540 =
                    isNull$540
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(6)));
            if (isNull$540) {
                outWriter.setNullAt(6);
            } else {
                outWriter.writeString(6, field$540);
            }

            outWriter.complete();

            return out;
        }
    }

    public class LongHashTable$544
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$544(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$528,
                    probeSer$529,
                    getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    getContainingTask().getEnvironment().getIOManager(),
                    9,
                    922759L / getRuntimeContext().getNumberOfParallelSubtasks());
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
                return probeToBinaryRow$541.apply(row);
            }
        }
    }

    public class LongHashTable$522
            extends org.apache.flink.table.runtime.hashtable.LongHybridHashTable {

        public LongHashTable$522(long memorySize) {
            super(
                    getContainingTask(),
                    true,
                    65536,
                    buildSer$505,
                    probeSer$506,
                    getContainingTask().getEnvironment().getMemoryManager(),
                    memorySize,
                    getContainingTask().getEnvironment().getIOManager(),
                    9,
                    28230509L / getRuntimeContext().getNumberOfParallelSubtasks());
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
                return probeToBinaryRow$519.apply(row);
            }
        }
    }

    public class Projection$510
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(8);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$510(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$511 = in1.isNullAt(0);
            long field$511 = isNull$511 ? -1L : (in1.getLong(0));
            if (isNull$511) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$511);
            }

            boolean isNull$512 = in1.isNullAt(1);
            org.apache.flink.table.data.binary.BinaryStringData field$512 =
                    isNull$512
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(1)));
            if (isNull$512) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeString(1, field$512);
            }

            boolean isNull$513 = in1.isNullAt(2);
            org.apache.flink.table.data.binary.BinaryStringData field$513 =
                    isNull$513
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(2)));
            if (isNull$513) {
                outWriter.setNullAt(2);
            } else {
                outWriter.writeString(2, field$513);
            }

            boolean isNull$514 = in1.isNullAt(3);
            int field$514 = isNull$514 ? -1 : (in1.getInt(3));
            if (isNull$514) {
                outWriter.setNullAt(3);
            } else {
                outWriter.writeInt(3, field$514);
            }

            boolean isNull$515 = in1.isNullAt(4);
            int field$515 = isNull$515 ? -1 : (in1.getInt(4));
            if (isNull$515) {
                outWriter.setNullAt(4);
            } else {
                outWriter.writeInt(4, field$515);
            }

            boolean isNull$516 = in1.isNullAt(5);
            int field$516 = isNull$516 ? -1 : (in1.getInt(5));
            if (isNull$516) {
                outWriter.setNullAt(5);
            } else {
                outWriter.writeInt(5, field$516);
            }

            boolean isNull$517 = in1.isNullAt(6);
            org.apache.flink.table.data.binary.BinaryStringData field$517 =
                    isNull$517
                            ? org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8
                            : (((org.apache.flink.table.data.binary.BinaryStringData)
                                    in1.getString(6)));
            if (isNull$517) {
                outWriter.setNullAt(6);
            } else {
                outWriter.writeString(6, field$517);
            }

            boolean isNull$518 = in1.isNullAt(7);
            boolean field$518 = isNull$518 ? false : (in1.getBoolean(7));
            if (isNull$518) {
                outWriter.setNullAt(7);
            } else {
                outWriter.writeBoolean(7, field$518);
            }

            outWriter.complete();

            return out;
        }
    }

    public class Projection$507
            implements org.apache.flink.table.runtime.generated.Projection<
                    org.apache.flink.table.data.RowData,
                    org.apache.flink.table.data.binary.BinaryRowData> {

        org.apache.flink.table.data.binary.BinaryRowData out =
                new org.apache.flink.table.data.binary.BinaryRowData(2);
        org.apache.flink.table.data.writer.BinaryRowWriter outWriter =
                new org.apache.flink.table.data.writer.BinaryRowWriter(out);

        public Projection$507(Object[] references) throws Exception {}

        @Override
        public org.apache.flink.table.data.binary.BinaryRowData apply(
                org.apache.flink.table.data.RowData in1) {

            // wrap variable to row

            outWriter.reset();

            boolean isNull$508 = in1.isNullAt(0);
            long field$508 = isNull$508 ? -1L : (in1.getLong(0));
            if (isNull$508) {
                outWriter.setNullAt(0);
            } else {
                outWriter.writeLong(0, field$508);
            }

            boolean isNull$509 = in1.isNullAt(1);
            boolean field$509 = isNull$509 ? false : (in1.getBoolean(1));
            if (isNull$509) {
                outWriter.setNullAt(1);
            } else {
                outWriter.writeBoolean(1, field$509);
            }

            outWriter.complete();

            return out;
        }
    }
}
