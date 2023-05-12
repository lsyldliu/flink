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

package org.apache.flink.table.planner.codegen.fusion

import org.apache.flink.streaming.api.operators.AbstractInput
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, ExprCodeGenerator}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{boxedTypeTermForType, className, DEFAULT_INPUT_TERM}
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator.{ELEMENT, STREAM_RECORD}
import org.apache.flink.table.types.logical.RowType

class InputFusionCodegenSpec(
    operatorCtx: CodeGeneratorContext,
    multipleInputId: Int,
    outputType: RowType)
  extends FusionCodegenSpec {

  override def getOutputType: RowType = outputType

  override def getExprCodeGenerator: ExprCodeGenerator = exprCodeGenerator

  protected lazy val exprCodeGenerator = new ExprCodeGenerator(operatorCtx, false, true)

  override def doProduceProcess(multipleCtx: CodeGeneratorContext): Unit = {
    val inputTypeTerm = boxedTypeTermForType(outputType)
    val inputTerm = DEFAULT_INPUT_TERM + multipleInputId
    exprCodeGenerator.bindInput(outputType, inputTerm)

    val processTerm = "processInput" + multipleInputId
    multipleCtx.addReusableMember(
      s"""
         |public void $processTerm($inputTypeTerm $inputTerm) throws Exception {
         |  ${consumeProcess(null, inputTerm)}
         |}
         |""".stripMargin
    )
    multipleCtx.addReusableMultipleProcessStatement(
      multipleInputId,
      s"""
         |new ${className[AbstractInput[_, _]]}(this, $multipleInputId) {
         |  @Override
         |  public void processElement($STREAM_RECORD $ELEMENT) throws Exception {
         |    // InputType: ${outputType.asSerializableString()}
         |    $inputTypeTerm $inputTerm = ($inputTypeTerm) $ELEMENT.getValue();
         |    $processTerm($inputTerm);
         |  }
         |}
         |""".stripMargin
    )
  }

  override def doProduceEndInput(multipleCtx: CodeGeneratorContext): Unit = {
    val endInputTerm = "endInput" + multipleInputId
    multipleCtx.addReusableMember(
      s"""
         |public void $endInputTerm() throws Exception {
         |  ${consumeEndInput()}
         |}
         |""".stripMargin
    )
    multipleCtx.addReusableMultipleEndInputStatement(s"""
                                                        |case $multipleInputId:
                                                        |  $endInputTerm();
                                                        |  break;
                                                        |""".stripMargin)
  }

  override def getOperatorCtx: CodeGeneratorContext = operatorCtx
}
