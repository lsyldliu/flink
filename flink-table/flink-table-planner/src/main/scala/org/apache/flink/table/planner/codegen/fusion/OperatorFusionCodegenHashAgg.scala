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
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.binary.BinaryRowData
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, CodeGenUtils, ExprCodeGenerator, GeneratedExpression}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{newName, newNames}
import org.apache.flink.table.planner.codegen.agg.batch.{AggCodeGenHelper, HashAggCodeGenHelper}
import org.apache.flink.table.planner.codegen.agg.batch.AggCodeGenHelper.{buildAggregateArgsMapping, genAggregateByFlatAggregateBufferExpr, genFlatAggBufferExprs, genGetValueFromFlatAggregateBuffer, genInitFlatAggregateBufferExr}
import org.apache.flink.table.planner.codegen.agg.batch.HashAggCodeGenHelper.{buildAggregateAggBuffMapping, genAggregateExpr, genHashAggValueOutputExprs, genReusableEmptyAggBuffer}
import org.apache.flink.table.planner.plan.utils.AggregateInfoList
import org.apache.flink.table.planner.typeutils.RowTypeUtils
import org.apache.flink.table.runtime.util.KeyValueIterator
import org.apache.flink.table.runtime.util.collections.binary.{BytesHashMap, BytesMap}
import org.apache.flink.table.types.logical.{LogicalType, RowType}

import org.apache.calcite.tools.RelBuilder

/**
 * HashAgg operator, it consume upstream records in process method, only emit record in endInput
 * method.
 */
class OperatorFusionCodegenHashAgg(
    operatorCtx: CodeGeneratorContext,
    builder: RelBuilder,
    aggInfoList: AggregateInfoList,
    outputType: RowType,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    isMerge: Boolean,
    managedMemory: Long,
    maxNumFileHandles: Int,
    compressionEnabled: Boolean,
    compressionBlockSize: Int)
  extends OperatorFusionCodegenSupport {

  override def getOutputType: RowType = outputType

  override def getOperatorCtx: CodeGeneratorContext = operatorCtx

  override def getExprCodeGenerator: ExprCodeGenerator = exprCodeGenerator

  private lazy val exprCodeGenerator = new ExprCodeGenerator(operatorCtx, false, true)

  private lazy val inputType = inputs(0).getOutputType
  private lazy val aggInfos = aggInfoList.aggInfos
  private lazy val aggBufferPrefix: String = newName("hashagg")
  private lazy val aggBufferNames =
    AggCodeGenHelper.getAggBufferNames(aggBufferPrefix, auxGrouping, aggInfos)
  private lazy val aggBufferTypes =
    AggCodeGenHelper.getAggBufferTypes(inputType, auxGrouping, aggInfos)
  private lazy val groupKeyRowType = RowTypeUtils.projectRowType(inputType, grouping)
  private lazy val aggBufferRowType = RowType.of(aggBufferTypes.flatten, aggBufferNames.flatten)

  private lazy val argsMapping: Array[Array[(Int, LogicalType)]] = buildAggregateArgsMapping(
    isMerge,
    grouping.length,
    inputType,
    auxGrouping,
    aggInfos,
    aggBufferTypes)

  private lazy val aggBuffMapping: Array[Array[(Int, LogicalType)]] = buildAggregateAggBuffMapping(
    aggBufferTypes)

  // The name for BytesMap
  private lazy val aggregateMapTerm: String = newName("aggregateMap")

  private var aggBufferExprs: Seq[GeneratedExpression] = _
  private var initAggBufferCode: String = _
  private var hasInput: String = _

  override def getManagedMemory: Long = managedMemory

  override protected def doProduceProcess(multipleCtx: CodeGeneratorContext): Unit = {
    assert(inputs.size == 1)
    this.inputs(0).produceProcess(multipleCtx, 1, this)
  }

  override protected def doProduceEndInput(multipleCtx: CodeGeneratorContext): Unit = {
    this.inputs(0).produceEndInput(multipleCtx)
  }

  override def doConsumeProcess(
      inputId: Int,
      input: Seq[GeneratedExpression],
      row: String): String = {
    // get input row term first fro auxGrouping codegen
    if (row != null) {
      inputRowTerm = row
    } else {
      inputRowTerm = getExprCodeGenerator.getInput1Term()
    }

    if (grouping.isEmpty) {
      doConsumeProcessWithoutKeys(input)
    } else {
      doConsumeProcessWithKeys(input)
    }
  }

  override def doConsumeEndInput(inputId: Int): String = {
    // the emit record logic we can construct it to a seperated method
    if (grouping.isEmpty) {
      doConsumeEndInputWithoutKeys
    } else {
      doConsumeEndInputWithKeys
    }
  }

  private def doConsumeProcessWithKeys(input: Seq[GeneratedExpression]): String = {
    // gen code to do group key projection from input
    val Seq(currentKeyTerm, currentKeyWriterTerm) = newNames("currentKey", "currentKeyWriter")

    val lookupInfoTypeTerm = classOf[BytesMap.LookupInfo[_, _]].getCanonicalName
    val binaryRowTypeTerm = classOf[BinaryRowData].getName
    val Seq(lookupInfo, currentAggBufferTerm) = newNames("lookupInfo", "currentAggBuffer")

    // evaluate input field access for group key projection and aggregate buffer update
    // val inputExprCode = input.map(x => x.getCode()).mkString("\n").trim

    // project key row from input
    val keyProjectionExpr = generateRowExpr(
      grouping,
      input,
      groupKeyRowType,
      classOf[BinaryRowData],
      outRecordTerm = currentKeyTerm,
      outRecordWriterTerm = currentKeyWriterTerm)

    // gen code to create empty agg buffer
    val initedAggBuffer = genReusableEmptyAggBuffer(
      operatorCtx,
      getExprCodeGenerator,
      builder,
      inputRowTerm,
      inputType,
      auxGrouping,
      aggInfos,
      aggBufferRowType)
    val lazyInitAggBufferCode = if (auxGrouping.isEmpty) {
      // create an empty agg buffer and initialized make it reusable
      operatorCtx.addReusableOpenStatement(initedAggBuffer.code)
      ""
    } else {
      s"""
         |// lazy init agg buffer (with auxGrouping)
         |${initedAggBuffer.getCode}
       """.stripMargin
    }

    // generate code to update agg buffer
    getExprCodeGenerator.bindSecondInput(aggBufferRowType, currentAggBufferTerm)

    val aggBufferAccessCode =
      getExprCodeGenerator.generateInputAccessExprs(2).map(e => e.getCode).mkString("\n")

    val aggregateExpr = genAggregateExpr(
      isMerge,
      operatorCtx,
      getExprCodeGenerator,
      builder,
      inputType,
      auxGrouping,
      aggInfos,
      argsMapping,
      aggBuffMapping,
      currentAggBufferTerm,
      aggBufferRowType
    )

    val processCode =
      s"""
         | // project key from input
         |${keyProjectionExpr.getCode}
         |
         | // look up output buffer using current group key
         |$lookupInfoTypeTerm $lookupInfo = ($lookupInfoTypeTerm) $aggregateMapTerm.lookup($currentKeyTerm);
         |$binaryRowTypeTerm $currentAggBufferTerm = ($binaryRowTypeTerm) $lookupInfo.getValue();
         |
         |if (!$lookupInfo.isFound()) {
         |  $lazyInitAggBufferCode
         |  // append empty agg buffer into aggregate map for current group key
         |  try {
         |    $currentAggBufferTerm =
         |      $aggregateMapTerm.append($lookupInfo, ${initedAggBuffer.resultTerm});
         |  } catch (java.io.EOFException exp) {
         |    throw new OutOfMemoryError("Global HashAgg doesn't support fallback to sort-based agg currently.");
         |  }
         |}
         | // aggregate buffer fields access
         |$aggBufferAccessCode
         | // do aggregate and update agg buffer
         |${aggregateExpr.getCode}
         |""".stripMargin.trim

    processCode
  }

  private def doConsumeEndInputWithKeys(): String = {
    // here need a new ExprCodeGenerator because we need to generate the key and result expr
    // prepare iteration var terms
    val Seq(reuseAggMapKeyTerm, reuseAggBufferTerm) = newNames("reuseAggMapKey", "reuseAggBuffer")
    // gen code to prepare agg output using agg buffer and key from the aggregate map
    val iteratorTerm = CodeGenUtils.newName("iterator")
    val iteratorType = classOf[KeyValueIterator[_, _]].getCanonicalName
    val rowDataType = classOf[RowData].getCanonicalName

    // generate value exprs from agg buffer
    val valueExprs = genHashAggValueOutputExprs(
      isMerge,
      operatorCtx,
      getExprCodeGenerator,
      builder,
      auxGrouping,
      aggInfos,
      argsMapping,
      aggBuffMapping,
      inputType,
      reuseAggBufferTerm,
      aggBufferRowType)

    // generate group key exprs from map key
    getExprCodeGenerator.bindInputWithExpr(groupKeyRowType, null, inputTerm = reuseAggMapKeyTerm)
    val keyExprs = getExprCodeGenerator.generateInputAccessExprs(1)

    val resultVars = keyExprs ++ valueExprs
    val endInputCode =
      s"""
         |$rowDataType $reuseAggMapKeyTerm;
         |$rowDataType $reuseAggBufferTerm;
         |$iteratorType<$rowDataType, $rowDataType> $iteratorTerm =
         |  $aggregateMapTerm.getEntryIterator(false); // reuse key/value during iterating
         |while ($iteratorTerm.advanceNext()) {
         |   // set result and output
         |   $reuseAggMapKeyTerm = ($rowDataType)$iteratorTerm.getKey();
         |   $reuseAggBufferTerm = ($rowDataType)$iteratorTerm.getValue();
         |   
         |   // consume the row of agg produce 
         |   ${consumeProcess(resultVars)}
         |}
       """.stripMargin

    val endInputMethodTerm = newName(variablePrefix + "withKeyEndInput")
    operatorCtx.addReusableMember(
      s"""
         |private void $endInputMethodTerm() throws Exception {
         |  $endInputCode
         |}
       """.stripMargin
    )
    s"$endInputMethodTerm();"
  }

  private def doConsumeProcessWithoutKeys(input: Seq[GeneratedExpression]): String = {
    aggBufferExprs = genFlatAggBufferExprs(
      isMerge,
      operatorCtx,
      getExprCodeGenerator,
      builder,
      auxGrouping,
      aggInfos,
      argsMapping,
      aggBufferPrefix,
      aggBufferNames,
      aggBufferTypes
    )

    initAggBufferCode = genInitFlatAggregateBufferExr(
      operatorCtx,
      exprCodeGenerator,
      builder,
      inputType,
      inputRowTerm,
      grouping,
      auxGrouping,
      aggInfos,
      aggBufferExprs)

    val aggregateCode = genAggregateByFlatAggregateBufferExpr(
      isMerge,
      operatorCtx,
      getExprCodeGenerator,
      builder,
      auxGrouping,
      aggInfos,
      argsMapping,
      aggBufferPrefix,
      aggBufferTypes,
      aggBufferExprs)

    hasInput = newName("hasInput")
    operatorCtx.addReusableMember(s"private boolean $hasInput = false;")

    s"""
       |if (!$hasInput) {
       |  $hasInput = true;
       |  // init agg buffer
       |  $initAggBufferCode
       |}
       |// update agg buffer to do aggregate
       |$aggregateCode
       |""".stripMargin.trim
  }

  private def doConsumeEndInputWithoutKeys(): String = {
    val resultExprs = genGetValueFromFlatAggregateBuffer(
      isMerge,
      operatorCtx,
      getExprCodeGenerator,
      builder,
      auxGrouping,
      aggInfos,
      argsMapping,
      aggBufferPrefix,
      aggBufferNames,
      aggBufferTypes)
    val endInputMethodTerm = newName(variablePrefix + "withoutKeyEndInput")
    operatorCtx.addReusableMember(
      s"""
         |private void $endInputMethodTerm() throws Exception {
         |  // if the input is empty in final phase, we should output default values
         |  if (!$hasInput) {
         |    $initAggBufferCode
         |  }
         |  // consume the agg output 
         |  ${consumeProcess(resultExprs)}
         |}
       """.stripMargin
    )
    s"$endInputMethodTerm();"
  }

  /**
   * This method is used to initialize operator code, it should be called before initCode & openCode
   * & closeCode.
   */
  override def initializeOperator(): Unit = {
    if (grouping.nonEmpty) {
      val Seq(groupKeyTypesTerm, aggBufferTypesTerm) =
        newNames("groupKeyTypes", "aggBufferTypes")
      // gen code to create groupKey, aggBuffer Type array
      // it will be used in BytesHashMap and BufferedKVExternalSorter if enable fallback
      HashAggCodeGenHelper.prepareHashAggKVTypes(
        operatorCtx,
        groupKeyTypesTerm,
        aggBufferTypesTerm,
        groupKeyRowType,
        aggBufferRowType)

      // create aggregate map
      val memorySizeTerm = newName("memorySize")
      val mapTypeTerm = classOf[BytesHashMap].getName
      operatorCtx.addReusableMember(s"private transient $mapTypeTerm $aggregateMapTerm;")
      operatorCtx.addReusableOpenStatement(
        s"""
           |long $memorySizeTerm = computeMemorySize($managedMemoryFraction);
           |$aggregateMapTerm = new $mapTypeTerm(
           |  getContainingTask(),
           |  getContainingTask().getEnvironment().getMemoryManager(),
           |  $memorySizeTerm,
           |  $groupKeyTypesTerm,
           |  $aggBufferTypesTerm);
       """.stripMargin)

      // close aggregate map and release memory segments
      operatorCtx.addReusableCloseStatement(s"$aggregateMapTerm.free();")
    }
  }

  private def generateRowExpr(
      inputMapping: Array[Int],
      input: Seq[GeneratedExpression],
      returnRow: RowType,
      returnTypeClazz: Class[_ <: RowData],
      outRecordTerm: String,
      outRecordWriterTerm: String,
      reusedOutRecord: Boolean = true): GeneratedExpression = {
    val fieldExprs = inputMapping.map(idx => input(idx))
    getExprCodeGenerator.generateResultExpression(
      fieldExprs,
      returnRow,
      returnTypeClazz,
      outRow = outRecordTerm,
      outRowWriter = Option(outRecordWriterTerm),
      reusedOutRow = reusedOutRecord)
  }
}
