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

import org.apache.flink.table.data.binary.BinaryRowData
import org.apache.flink.table.planner.codegen.CodeGenUtils.{newName, DEFAULT_OUT_RECORD_TERM, DEFAULT_OUT_RECORD_WRITER_TERM}
import org.apache.flink.table.planner.codegen.GeneratedExpression.{NEVER_NULL, NO_CODE}
import org.apache.flink.table.types.logical.RowType

import scala.collection.mutable.ListBuffer

/** An interface for those physical operators that support multiple fusion codegen. */
trait OperatorFusionCodegenSupport {

  /** Prefix used in the current operator's variable names. */
  private def variablePrefix: String = this match {
    case _: OperatorFusionCodegenInput => "input_"
    case _: OperatorFusionCodegenCalc => "calc_"
    case _: OperatorFusionCodegenHashJoin => "hj_"
    case _: OperatorFusionCodegenOutput => "out_"
  }

  protected var managedMemoryFraction: Double = 0

  /** Which ExecNode is calling produce() of this one. It's itself for the first . */
  protected var output: OperatorFusionCodegenSupport = null

  /**
   * The input id (start from 1) corresponding to the parent inputs. e.g. the parent is a join
   * operator, depending on the side of the source, input id may be 1 (left side) or 2 (right side).
   */
  protected var inputIdOfOutputNode = 1

  val inputs: ListBuffer[OperatorFusionCodegenSupport] =
    new ListBuffer[OperatorFusionCodegenSupport]()

  def addInput(input: OperatorFusionCodegenSupport): Unit = {
    inputs += input
  }

  def getOutputType: RowType

  def getOperatorCtx: CodeGeneratorContext

  def getExprCodeGenerator: ExprCodeGenerator

  def getManagedMemory: Long = 0L

  final def setManagedMemFraction(managedMemoryFraction: Double) =
    this.managedMemoryFraction = managedMemoryFraction

  /**
   * This method is used to initialize operator code, it should be called before initCode & openCode
   * & closeCode.
   */
  def initializeOperator(): Unit = {}

  final def addReusableInitCode(fusionCtx: CodeGeneratorContext): Unit = {
    val operatorCtx = getOperatorCtx
    // add operator reusable member and inner class definition to multiple codegen ctx
    fusionCtx.addReusableMember(operatorCtx.reuseMemberCode())
    fusionCtx.addReusableInnerClass(
      newName(this.getClass.getCanonicalName),
      operatorCtx.reuseInnerClassDefinitionCode())

    // add init code
    val initCode = operatorCtx.reuseInitCode()
    if (!initCode.isEmpty) {
      val initMethodTerm = newName(variablePrefix + "init")
      fusionCtx.addReusableMember(
        s"""
           |private void $initMethodTerm(Object[] references) throws Exception {
           |  ${operatorCtx.reuseInitCode()}
           |}
     """.stripMargin)

      val refs =
        fusionCtx.addReusableObject(operatorCtx.references.toArray, variablePrefix + "Refs")
      fusionCtx.addReusableInitStatement(s"$initMethodTerm($refs);")
    }
  }

  final def addReusableOpenCode(fusionCtx: CodeGeneratorContext): Unit = {
    // add open code
    fusionCtx.addReusableOpenStatement(getOperatorCtx.reuseOpenCode())
  }

  final def addReusableCloseCode(fusionCtx: CodeGeneratorContext): Unit = {
    // add close code
    fusionCtx.addReusableCloseStatement(getOperatorCtx.reuseCloseCode())
  }

  final def produceProcess(
      multipleCtx: CodeGeneratorContext,
      inputIdOfOutputNode: Int,
      output: OperatorFusionCodegenSupport): Unit = {
    this.output = output
    this.inputIdOfOutputNode = inputIdOfOutputNode
    doProduceProcess(multipleCtx)
  }

  protected def doProduceProcess(multipleCtx: CodeGeneratorContext): Unit

  final def consumeProcess(
      multipleCtx: CodeGeneratorContext,
      outputVars: Seq[GeneratedExpression],
      row: String = null): String = {
    val inputVars = if (outputVars != null) {
      assert(outputVars.length == getOutputType.getFieldCount)
      outputVars
    } else {
      assert(row != null, "outputVars and row can't both be null.")
      getExprCodeGenerator.generateInputAccessExprs()
    }
    val rowVar = prepareRowVar(row, outputVars)

    // we need to bind out ctx before call its consume to reuse the input expression
    if (inputIdOfOutputNode == 1) {
      output.getExprCodeGenerator.bindInputWithExpr(getOutputType, inputVars, rowVar.resultTerm)
    } else {
      output.getExprCodeGenerator.bindSecondInputWithExpr(
        getOutputType,
        inputVars,
        rowVar.resultTerm)
    }

    // we always pass column vars and row var to parent simultaneously, the output decide to use which one
    s"""
       |${output.doConsumeProcess(multipleCtx, inputIdOfOutputNode, inputVars, rowVar)}
     """.stripMargin
  }

  def doConsumeProcess(
      multipleCtx: CodeGeneratorContext,
      inputId: Int,
      input: Seq[GeneratedExpression],
      row: GeneratedExpression): String = {
    throw new UnsupportedOperationException
  }

  final def produceEndInput(multipleCtx: CodeGeneratorContext): Unit = {
    doProduceEndInput(multipleCtx)
  }

  protected def doProduceEndInput(multipleCtx: CodeGeneratorContext): Unit

  final def consumeEndInput(multipleCtx: CodeGeneratorContext): String = {
    s"""
       |${output.doConsumeEndInput(multipleCtx, inputIdOfOutputNode)}
     """.stripMargin
  }

  def doConsumeEndInput(multipleCtx: CodeGeneratorContext, inputId: Int): String = {
    consumeEndInput(multipleCtx)
  }

  private def prepareRowVar(row: String, colVars: Seq[GeneratedExpression]): GeneratedExpression = {
    if (row != null) {
      new GeneratedExpression(row, NEVER_NULL, NO_CODE, getOutputType)
    } else {
      getExprCodeGenerator.generateResultExpression(
        // need copy the colVars first to avoid it code is used during generate row
        colVars.map(_.copyExpr),
        getOutputType,
        classOf[BinaryRowData],
        newName(variablePrefix + DEFAULT_OUT_RECORD_TERM),
        Some(newName(variablePrefix + DEFAULT_OUT_RECORD_WRITER_TERM))
      )
    }
  }

  def prepareInputRowVar(
      inputId: Int,
      row: String,
      colVars: Seq[GeneratedExpression]): GeneratedExpression = {
    val inputOp = inputs(inputId)
    if (row != null) {
      new GeneratedExpression(row, NEVER_NULL, NO_CODE, inputOp.getOutputType)
    } else {
      inputOp.getExprCodeGenerator.generateResultExpression(
        colVars,
        inputOp.getOutputType,
        classOf[BinaryRowData],
        newName(variablePrefix + "input"),
        Some(newName(variablePrefix + "inputWriter"))
      )
    }
  }

}
