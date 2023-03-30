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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.rex.{RexInputRef, RexNode}

/**
 * The Calc operator for whole stage codegen.
 * @param inputType
 * @param outputType
 * @param projection
 * @param condition
 * @param retainHeader
 */
class OperatorFusionCodegenCalc(
    operatorCtx: CodeGeneratorContext,
    outputType: RowType,
    projection: Seq[RexNode],
    condition: Option[RexNode],
    retainHeader: Boolean = false)
  extends OperatorFusionCodegenSupport {

  override def getOutputType: RowType = outputType

  override def getExprCodeGenerator: ExprCodeGenerator = exprCodeGenerator

  private lazy val exprCodeGenerator = new ExprCodeGenerator(operatorCtx, false, true)

  override def doProduceProcess(multipleCtx: CodeGeneratorContext): Unit = {
    assert(inputs.size == 1)
    this.inputs(0).produceProcess(multipleCtx, 1, this)
  }

  override def doConsumeProcess(
      multipleCtx: CodeGeneratorContext,
      inputId: Int,
      input: Seq[GeneratedExpression],
      row: GeneratedExpression): String = {
    val inputType = inputs(0).getOutputType
    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM
    getExprCodeGenerator.bindInput(inputType, inputTerm = inputTerm)

    val onlyFilter = projection.lengthCompare(inputType.getFieldCount) == 0 &&
      projection.zipWithIndex.forall {
        case (rexNode, index) =>
          rexNode.isInstanceOf[RexInputRef] && rexNode.asInstanceOf[RexInputRef].getIndex == index
      }

    if (condition.isEmpty && onlyFilter) {
      throw new TableException(
        "This calc has no useful projection and no filter. " +
          "It should be removed by CalcRemoveRule.")
    } else if (condition.isEmpty) { // only projection
      val projectionExprs = projection.map(getExprCodeGenerator.generateExpression)
      s"""
         |${operatorCtx.reuseInputUnboxingCode()}
         |${consumeProcess(multipleCtx, projectionExprs)}
         |""".stripMargin
    } else {
      val filterCondition = getExprCodeGenerator.generateExpression(condition.get)
      // only filter
      if (onlyFilter) {
        s"""
           |${operatorCtx.reuseInputUnboxingCode()}
           |${filterCondition.code}
           |if (${filterCondition.resultTerm}) {
           |  ${consumeProcess(multipleCtx, input)}
           |}
           |""".stripMargin
      } else { // both filter and projection
        val filterInputCode = operatorCtx.reuseInputUnboxingCode()
        val filterInputSet = Set(operatorCtx.reusableInputUnboxingExprs.keySet.toSeq: _*)

        // if any filter conditions, projection code will enter an new scope
        val projectionExprs = projection.map(getExprCodeGenerator.generateExpression)

        val projectionInputCode = operatorCtx.reusableInputUnboxingExprs
          .filter(entry => !filterInputSet.contains(entry._1))
          .values
          .map(_.code)
          .mkString("\n")
        s"""
           |$filterInputCode
           |${filterCondition.code}
           |if (${filterCondition.resultTerm}) {
           |  $projectionInputCode
           |  ${consumeProcess(multipleCtx, projectionExprs)}
           |}
           |""".stripMargin
      }
    }
  }

  override def doProduceEndInput(multipleCtx: CodeGeneratorContext): Unit = {
    this.inputs(0).produceEndInput(multipleCtx)
  }

}
