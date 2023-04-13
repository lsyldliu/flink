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

import org.apache.flink.core.memory.ManagedMemoryUseCase
import org.apache.flink.streaming.api.operators.{AbstractStreamOperatorV2, BoundedMultiInput, Input, InputSelectable, InputSelection, MultipleInputStreamOperator, StreamOperatorParameters}
import org.apache.flink.streaming.runtime.tasks.StreamTask
import org.apache.flink.table.data.{BoxedWrapperRowData, RowData}
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, ExprCodeGenerator, GeneratedExpression}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{className, newName}
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator.{addReuseOutElement, generateCollect, OUT_ELEMENT, STREAM_RECORD}
import org.apache.flink.table.planner.utils.Logging
import org.apache.flink.table.runtime.generated.GeneratedOperator
import org.apache.flink.table.runtime.operators.multipleinput.OperatorFusionCodegenFactory
import org.apache.flink.table.runtime.operators.multipleinput.input.{InputSelectionHandler, MultipleInputSpec}
import org.apache.flink.table.types.logical.RowType

import java.util

import scala.collection.mutable.ListBuffer

class OperatorFusionCodegenOutput(operatorCtx: CodeGeneratorContext)
  extends OperatorFusionCodegenSupport
  with Logging {

  private lazy val exprCodeGenerator = new ExprCodeGenerator(operatorCtx, false, true)

  override def getOutputType: RowType = inputs(0).getOutputType

  override def getExprCodeGenerator: ExprCodeGenerator = exprCodeGenerator

  override def doProduceProcess(multipleCtx: CodeGeneratorContext): Unit = {
    throw new UnsupportedOperationException
  }

  override def doProduceEndInput(multipleCtx: CodeGeneratorContext): Unit = {
    throw new UnsupportedOperationException
  }

  override def doConsumeProcess(
      inputId: Int,
      input: Seq[GeneratedExpression],
      row: String): String = {
    addReuseOutElement(operatorCtx)
    val rowVar = prepareInputRowVar(1, classOf[BoxedWrapperRowData], row, input)
    s"""
       |${rowVar.getCode}
       |${generateCollect(rowVar.resultTerm)}
       |""".stripMargin
  }

  override def doConsumeEndInput(inputId: Int): String = ""

  def generateMultipleOperator(
      inputSpecs: util.List[MultipleInputSpec]): (OperatorFusionCodegenFactory[RowData], Long) = {
    val multipleCtx = new CodeGeneratorContext(operatorCtx.tableConfig, operatorCtx.classLoader)

    multipleCtx.addReusableMember(
      s"""
         |private ${className[StreamTask[_, _]]}<?, ?> getContainingTask() {
         |  return parameters.getContainingTask();
         |}
         """.stripMargin)

    multipleCtx.addReusableMember(
      s"""
         |private long computeMemorySize(double operatorFraction) {
         |  final double multipleFraction = parameters
         |    .getStreamConfig()
         |    .getManagedMemoryFractionOperatorUseCaseOfSlot(
         |      ${className[ManagedMemoryUseCase]}.OPERATOR,
         |      getRuntimeContext().getTaskManagerRuntimeInfo().getConfiguration(),
         |      getRuntimeContext().getUserCodeClassLoader());
         |  return getContainingTask()
         |    .getEnvironment()
         |    .getMemoryManager()
         |    .computeMemorySize(multipleFraction * operatorFraction);
         |}
         """.stripMargin
    )

    // generate process code
    inputs(0).produceProcess(multipleCtx, 1, this)
    // generate endInput code
    inputs(0).produceEndInput(multipleCtx)

    val (operators, totalManagedMemory) = getAllOperatorAndMem
    var iter = operators.iterator
    while (iter.hasNext) {
      val op = iter.next
      op.initializeOperator
      // init code
      op.addReusableInitCode(multipleCtx)
      // open code
      op.addReusableOpenCode(multipleCtx)
    }

    iter = operators.descendingIterator
    while (iter.hasNext) {
      val op = iter.next
      // close code
      op.addReusableCloseCode(multipleCtx)
    }

    multipleCtx.addReusableMember(
      s"private final $STREAM_RECORD $OUT_ELEMENT = new $STREAM_RECORD(null);")

    multipleCtx.addReusableMember(
      s"private final ${className[StreamOperatorParameters[_]]} parameters;")
    multipleCtx.addReusableMember(
      s"private final ${className[InputSelectionHandler]} inputSelectionHandler;")

    val inputSpecRefs = multipleCtx.addReusableObject(inputSpecs, "inputSpecRefs")
    multipleCtx.addReusableInitStatement(
      s"this.inputSelectionHandler = new ${className[InputSelectionHandler]}($inputSpecRefs);")

    val operatorName = newName("BatchMultipleInputStreamOperator")
    val operatorCode =
      s"""
      public final class $operatorName extends ${className[AbstractStreamOperatorV2[_]]}
          implements ${className[MultipleInputStreamOperator[_]]},
          ${className[InputSelectable]},
          ${className[BoundedMultiInput]} {

        ${multipleCtx.reuseMemberCode()}

        public $operatorName(
          Object[] references,
          ${className[StreamOperatorParameters[_]]} parameters) throws Exception {
          super(parameters, ${inputSpecs.size()});
          this.parameters = parameters;
          ${multipleCtx.reuseInitCode()}
        }

        @Override
        public void open() throws Exception {
          super.open();
          ${multipleCtx.reuseOpenCode()}
        }

        @Override
        public ${className[util.List[Input[_]]]} getInputs() {
          return ${className[util.Arrays]}.asList(${multipleCtx.reuseMultipleProcessCode()});
        }

        @Override
        public void endInput(int inputId) throws Exception {
          inputSelectionHandler.endInput(inputId);
          switch(inputId) {
            ${multipleCtx.reuseMultipleEndInputCode()}
          }
        }
        
        @Override
        public ${className[InputSelection]} nextSelection() {
          return inputSelectionHandler.getInputSelection();
        }

        @Override
        public void finish() throws Exception {
            ${multipleCtx.reuseFinishCode()}
            super.finish();
        }

        @Override
        public void close() throws Exception {
           super.close();
           ${multipleCtx.reuseCloseCode()}
        }

        ${multipleCtx.reuseInnerClassDefinitionCode()}
      }
    """.stripMargin

    // println(operatorCode)

    LOG.debug(s"Compiling BatchMultipleInputStreamOperator Code:\n$operatorName")
    LOG.trace(s"Code: \n$operatorCode")
    (
      new OperatorFusionCodegenFactory(
        new GeneratedOperator(
          operatorName,
          operatorCode,
          multipleCtx.references.toArray,
          multipleCtx.tableConfig)),
      totalManagedMemory)
  }

  private def getAllOperatorAndMem(): (util.ArrayDeque[OperatorFusionCodegenSupport], Long) = {
    val operators = new util.ArrayDeque[OperatorFusionCodegenSupport]
    getAllOperators(this, operators)

    var totalManagedMemory: Long = 0
    operators.forEach(op => totalManagedMemory += op.getManagedMemory)

    operators.forEach(
      op =>
        if (totalManagedMemory != 0) {
          op.setManagedMemFraction(op.getManagedMemory * 1.0 / totalManagedMemory)
        })

    (operators, totalManagedMemory)
  }

  private def getAllOperators(
      outputOp: OperatorFusionCodegenSupport,
      operators: util.ArrayDeque[OperatorFusionCodegenSupport]): Unit = {
    operators.add(outputOp)
    // visit all input operator recursively
    val inputOps: ListBuffer[OperatorFusionCodegenSupport] = outputOp.inputs
    inputOps.foreach(op => getAllOperators(op, operators))
  }

  override def getOperatorCtx: CodeGeneratorContext = operatorCtx
}
