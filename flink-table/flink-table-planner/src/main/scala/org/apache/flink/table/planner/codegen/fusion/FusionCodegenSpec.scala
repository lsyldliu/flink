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

import org.apache.flink.table.data.{BoxedWrapperRowData, RowData}
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, ExprCodeGenerator, GeneratedExpression}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{newName, DEFAULT_OUT_RECORD_WRITER_TERM}
import org.apache.flink.table.planner.codegen.GeneratedExpression.{NEVER_NULL, NO_CODE}
import org.apache.flink.table.types.logical.RowType

import scala.collection.mutable.ListBuffer

/** An interface for those physical operators that support multiple fusion codegen. */
trait FusionCodegenSpec {

  /** Prefix used in the current operator's variable names. */
  protected def variablePrefix: String = this match {
    case _: InputFusionCodegenSpec => "input_"
    case _: CalcFusionCodegenSpec => "calc_"
    case _: HashJoinFusionCodegenSpec => "hj_"
    case _: HashAggFusionCodegenSpec => "hashagg_"
    case _: LocalHashAggFusionCodegenSpec => "localagg_"
    case _: RootFusionCodegenSpec => "root_"
  }

  protected var managedMemoryFraction: Double = 0

  /** Which ExecNode is calling produce() of this one. It's itself for the first . */
  protected var output: FusionCodegenSpec = null

  /**
   * The input id (start from 1) corresponding to the parent inputs. e.g. the parent is a join
   * operator, depending on the side of the source, input id may be 1 (left side) or 2 (right side).
   */
  protected var inputIdOfOutputNode = 1

  protected var cachedDoConsumeFunction: String = null

  protected var inputRowTerm: String = newName(variablePrefix + "inputRow")

  val inputs: ListBuffer[FusionCodegenSpec] =
    new ListBuffer[FusionCodegenSpec]()

  def addInput(input: FusionCodegenSpec): Unit = {
    inputs += input
  }

  /**
   * The subset of inputSet those should be evaluated before this plan.
   *
   * We will use this to insert some code to access those columns that are actually used by current
   * plan before calling doConsume().
   */
  def usedInputs: Set[Int] = Set.empty

  /**
   * Returns source code to evaluate the variables for required attributes, and clear the code of
   * evaluated variables, to prevent them to be evaluated twice.
   */
  protected def evaluateRequiredVariables(
      inputExprs: Seq[GeneratedExpression],
      requiredInput: Set[Int]): String = {
    val evaluateVars = new StringBuilder
    requiredInput.foreach(
      index => {
        val expr = inputExprs(index)
        if (!expr.codeUsed) {
          evaluateVars.append(expr.getCode + "\n")
        }
      })
    evaluateVars.toString()
  }

  def getOutputType: RowType

  def getOperatorCtx: CodeGeneratorContext

  def getExprCodeGenerator: ExprCodeGenerator

  def getManagedMemory: Long = 0L

  /**
   * Like HashJoin operator, it emit records in process and endInput method, so we need to wrap the
   * consumed code to a seperated method.
   */
  def needConstructDoConsumeFunction: Boolean = false

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
      output: FusionCodegenSpec): Unit = {
    this.output = output
    this.inputIdOfOutputNode = inputIdOfOutputNode
    doProduceProcess(multipleCtx)
  }

  protected def doProduceProcess(multipleCtx: CodeGeneratorContext): Unit

  final def consumeProcess(outputVars: Seq[GeneratedExpression], row: String = null): String = {
    val inputVars = if (outputVars != null) {
      assert(outputVars.length == getOutputType.getFieldCount)
      outputVars
    } else {
      assert(row != null, "outputVars and row can't both be null.")
      // this should use the row to generate field expr directly instead of using getExprCodeGenerator which is not corrected way
      getExprCodeGenerator.generateInputAccessExprs()
    }

    // we need to bind out ctx before call its consume to reuse the input expression
    if (inputIdOfOutputNode == 1) {
      output.getExprCodeGenerator.bindInputWithExpr(getOutputType, inputVars, row)
    } else {
      output.getExprCodeGenerator.bindSecondInputWithExpr(getOutputType, inputVars, row)
    }

    // evaluate the expr code which will be used more than one in advance to avoid evaluated more time, Calc need it currently
    val evaluated = evaluateRequiredVariables(inputVars, output.usedInputs)
    // we always pass column vars and row var to parent simultaneously, the output decide to use which one
    s"""
       |  // evaluate the required expr in advance
       |$evaluated
       |${output.doConsumeProcess(inputIdOfOutputNode, inputVars, row)}
     """.stripMargin
  }

  def doConsumeProcess(inputId: Int, input: Seq[GeneratedExpression], row: String): String = {
    throw new UnsupportedOperationException
  }

  final def produceEndInput(multipleCtx: CodeGeneratorContext): Unit = {
    doProduceEndInput(multipleCtx)
  }

  protected def doProduceEndInput(multipleCtx: CodeGeneratorContext): Unit

  final def consumeEndInput(): String = {
    s"""
       |${output.doConsumeEndInput(inputIdOfOutputNode)}
     """.stripMargin
  }

  def doConsumeEndInput(inputId: Int): String = {
    consumeEndInput()
  }

  private def prepareRowVar(row: String, colVars: Seq[GeneratedExpression]): GeneratedExpression = {
    if (row != null) {
      new GeneratedExpression(row, NEVER_NULL, NO_CODE, getOutputType)
    } else {
      getExprCodeGenerator.generateResultExpression(
        // need copy the colVars first to avoid it code is used during generate row
        colVars.map(_.copyExpr),
        getOutputType,
        classOf[BoxedWrapperRowData],
        newName(variablePrefix + "outRow"),
        Some(newName(variablePrefix + DEFAULT_OUT_RECORD_WRITER_TERM))
      )
    }
  }

  final def prepareInputRowVar(
      inputId: Int,
      rowTypeClazz: Class[_ <: RowData],
      row: String,
      colVars: Seq[GeneratedExpression]): GeneratedExpression = {
    val inputOp = inputs(inputId - 1)
    if (row != null) {
      new GeneratedExpression(row, NEVER_NULL, NO_CODE, inputOp.getOutputType)
    } else {
      inputOp.getExprCodeGenerator.generateResultExpression(
        colVars,
        inputOp.getOutputType,
        rowTypeClazz,
        inputRowTerm,
        Some(newName(variablePrefix + "inputWriter"))
      )
    }
  }

}
