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

import org.apache.flink.streaming.api.operators.AbstractInput
import org.apache.flink.table.planner.codegen.CodeGenUtils.{boxedTypeTermForType, className, DEFAULT_INPUT_TERM}
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator.{ELEMENT, STREAM_RECORD}
import org.apache.flink.table.types.logical.RowType

class OperatorFusionCodegenInput(
    operatorCtx: CodeGeneratorContext,
    multipleInputId: Int,
    outputType: RowType)
  extends OperatorFusionCodegenSupport {

  override def getOutputType: RowType = outputType

  override def getExprCodeGenerator: ExprCodeGenerator = exprCodeGenerator

  private lazy val exprCodeGenerator = new ExprCodeGenerator(operatorCtx, false, true)

  override def doProduceProcess(multipleCtx: CodeGeneratorContext): Unit = {
    val inputTypeTerm = boxedTypeTermForType(outputType)
    val inputTerm = DEFAULT_INPUT_TERM + multipleInputId
    exprCodeGenerator.bindInput(outputType, inputTerm)

    multipleCtx.addReusableMultipleProcessStatement(
      s"""
         |new ${className[AbstractInput[_, _]]}(this, $multipleInputId) {
         |  @Override
         |  public void processElement($STREAM_RECORD $ELEMENT) throws Exception {
         |    ${operatorCtx.reuseLocalVariableCode()}
         |    $inputTypeTerm $inputTerm = ($inputTypeTerm) $ELEMENT.getValue();
         |    ${consumeProcess(multipleCtx, null, inputTerm)}
         |  }
         |}
         |""".stripMargin)
  }

  override def doProduceEndInput(multipleCtx: CodeGeneratorContext): Unit = {
    multipleCtx.addReusableMultipleEndInputStatement(s"""
                                                        |case $multipleInputId:
                                                        |  ${consumeEndInput(multipleCtx)}
                                                        |  break;
                                                        |""".stripMargin)
  }
}
