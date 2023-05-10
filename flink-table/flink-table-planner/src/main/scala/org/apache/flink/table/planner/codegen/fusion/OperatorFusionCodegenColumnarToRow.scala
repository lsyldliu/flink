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
import org.apache.flink.table.data.columnar.ColumnarRowData
import org.apache.flink.table.planner.codegen.CodeGeneratorContext
import org.apache.flink.table.planner.codegen.CodeGenUtils.{boxedTypeTermForType, className, newName, DEFAULT_INPUT_TERM}
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator.{ELEMENT, STREAM_RECORD}
import org.apache.flink.table.types.logical.RowType

class OperatorFusionCodegenColumnarToRow(
    operatorCtx: CodeGeneratorContext,
    multipleInputId: Int,
    outputType: RowType)
  extends OperatorFusionCodegenInput(operatorCtx, multipleInputId, outputType) {

  override def doProduceProcess(multipleCtx: CodeGeneratorContext): Unit = {
    val inputTypeTerm = boxedTypeTermForType(outputType)
    val inputTerm = DEFAULT_INPUT_TERM + multipleInputId
    val columnarRowTerm = newName("columnarRowData")
    val columnarRowTypeTerm = classOf[ColumnarRowData].getName

    exprCodeGenerator.bindInput(outputType, columnarRowTerm)

    val fieldExprs = exprCodeGenerator.generateInputAccessExprs(1)
    val processTerm = "processInput" + multipleInputId
    val rowNumTerm = newName("numRows")
    multipleCtx.addReusableMember(
      s"""
         |public void $processTerm($inputTypeTerm $inputTerm) throws Exception {
         |  // for loop to iterate the ColumnarRowData
         |  $columnarRowTypeTerm $columnarRowTerm = ($columnarRowTypeTerm) $inputTerm;
         |  int $rowNumTerm = $columnarRowTerm.getNumRows();
         |  for(int i = 0; i < $rowNumTerm; i++) {
         |    $columnarRowTerm.setRowId(i);
         |    ${consumeProcess(fieldExprs)}
         |  }
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
}
