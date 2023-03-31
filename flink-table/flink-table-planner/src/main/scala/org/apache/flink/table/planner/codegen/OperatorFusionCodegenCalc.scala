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
    val onlyFilter = projection.lengthCompare(inputs(0).getOutputType.getFieldCount) == 0 &&
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
         |${consumeProcess(multipleCtx, projectionExprs)}
         |""".stripMargin
    } else {
      val filterCondition = getExprCodeGenerator.generateExpression(condition.get)
      // only filter
      if (onlyFilter) {
        s"""
           |${filterCondition.code}
           |if (${filterCondition.resultTerm}) {
           |  ${consumeProcess(multipleCtx, input)}
           |}
           |""".stripMargin
      } else { // both filter and projection
        val filterInputCode = operatorCtx.reuseInputUnboxingCode()
        val filterInputSet = Set(operatorCtx.reusableInputUnboxingExprs.keySet.toSeq: _*)

        // if any filter conditions, projection code will enter an new scope
        // TODO filter 已经计算过的表达式，参考spark，在project中无需再重复计算，因此需要把其input表达式code置为空，不然会存在变量重复定义问题
        val projectionExprs = projection.map(getExprCodeGenerator.generateExpression)

        val projectionInputCode = operatorCtx.reusableInputUnboxingExprs
          .filter(entry => !filterInputSet.contains(entry._1))
          .values
          .map(_.code)
          .mkString("\n")
        s"""
           |${filterCondition.code}
           |if (${filterCondition.resultTerm}) {
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
