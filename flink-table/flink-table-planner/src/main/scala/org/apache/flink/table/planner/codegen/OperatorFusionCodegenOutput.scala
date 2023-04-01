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

package org.apache.flink.table.planner.codegen
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator.{addReuseOutElement, generateCollect}
import org.apache.flink.table.runtime.operators.multipleinput.OperatorFusionCodegenFactory
import org.apache.flink.table.runtime.operators.multipleinput.input.MultipleInputSpec
import org.apache.flink.table.types.logical.RowType

import java.util

class OperatorFusionCodegenOutput(ctx: CodeGeneratorContext) extends OperatorFusionCodegenSupport {

  private lazy val exprCodeGenerator = new ExprCodeGenerator(ctx, false, true)

  override def getOutputType: RowType = inputs(0).getOutputType

  override def getExprCodeGenerator: ExprCodeGenerator = exprCodeGenerator

  override def doProduceProcess(multipleCtx: CodeGeneratorContext): Unit = {
    throw new UnsupportedOperationException
  }

  override def doProduceEndInput(multipleCtx: CodeGeneratorContext): Unit = {
    throw new UnsupportedOperationException
  }

  override def doConsumeProcess(
      multipleCtx: CodeGeneratorContext,
      inputId: Int,
      input: Seq[GeneratedExpression],
      row: GeneratedExpression): String = {
    addReuseOutElement(ctx)
    s"""
       |${row.getCode}
       |${generateCollect(row.resultTerm)}
       |""".stripMargin
  }

  override def doConsumeEndInput(multipleCtx: CodeGeneratorContext, inputId: Int): String = ""

  def generateMultipleOperator(
      inputSpecs: util.List[MultipleInputSpec]): OperatorFusionCodegenFactory[RowData] = {
    // process code
    inputs(0).produceProcess(ctx, 1, this)
    // endInput code
    inputs(0).produceEndInput(ctx)
    // open code

    // close code
    null
  }
}
