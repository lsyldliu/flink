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

package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.MiniBatchTableFunction;
import org.apache.flink.table.runtime.collector.TableFunctionCollector;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.generated.GeneratedCollector;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

/**
 * Similar to {@link LookupJoinWithCalcRetryRunner}, {@link LookupJoinBundleWithCalcFunction}
 * process join lookup logic with a calculation project function upon dimension table.
 * The difference is that it runs in a batch fashion.
 */
public class LookupJoinBundleWithCalcFunction extends LookupJoinBundleFunction {
    private static final long serialVersionUID = 1L;

    private final GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalc;

    private transient FlatMapFunction<RowData, RowData> calc;
    private transient Collector<RowData> calcCollector;

    public LookupJoinBundleWithCalcFunction(
            GeneratedFunction<MapFunction<RowData, Object[]>> generatedKeyConverter,
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalc,
            GeneratedCollector<TableFunctionCollector<RowData>> generatedCollector,
            MiniBatchTableFunction<RowData> tableFunction,
            RowType rowType,
            boolean isLeftOuterJoin,
            int tableFieldsCount) {
        super(generatedKeyConverter, generatedCollector, tableFunction, rowType, isLeftOuterJoin, tableFieldsCount);
        this.generatedCalc = generatedCalc;
    }

    @Override
    public void open(ExecutionContext ctx) throws Exception {
        super.open(ctx);
        this.calc = generatedCalc.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());
        FunctionUtils.setFunctionRuntimeContext(calc, ctx.getRuntimeContext());
        FunctionUtils.openFunction(calc, null);
        this.calcCollector = new LookupJoinBundleWithCalcFunction.CalcCollector(collector);
    }

    @Override
    public Collector<RowData> getFetcherCollector() {
        return calcCollector;
    }

    private class CalcCollector implements Collector<RowData> {

        private final Collector<RowData> delegate;

        private CalcCollector(Collector<RowData> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void collect(RowData record) {
            try {
                calc.flatMap(record, delegate);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
