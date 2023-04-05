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

import org.apache.flink.table.data.TimestampData
import org.apache.flink.table.planner.codegen.CodeGenUtils.{newName, newNames, BINARY_ROW, ROW_DATA}
import org.apache.flink.table.planner.codegen.LongHashJoinGenerator.{genGetLongKey, genProjection}
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec
import org.apache.flink.table.runtime.hashtable.LongHybridHashTable
import org.apache.flink.table.runtime.operators.join.{FlinkJoinType, HashJoinType}
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer
import org.apache.flink.table.runtime.util.RowIterator
import org.apache.flink.table.types.logical.{LocalZonedTimestampType, LogicalType, RowType, TimestampType}
import org.apache.flink.table.types.logical.LogicalTypeRoot.{BIGINT, DATE, DOUBLE, FLOAT, INTEGER, SMALLINT, TIME_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE, TIMESTAMP_WITHOUT_TIME_ZONE, TINYINT}

class OperatorFusionCodegenHashJoin(
    operatorCtx: CodeGeneratorContext,
    outputType: RowType,
    leftIsBuild: Boolean,
    joinSpec: JoinSpec,
    estimatedLeftAvgRowSize: Int,
    estimatedRightAvgRowSize: Int,
    estimatedLeftRowCount: Long,
    estimatedRightRowCount: Long,
    tryDistinctBuildRow: Boolean,
    managedMemory: Long,
    compressionEnabled: Boolean,
    compressionBlockSize: Int)
  extends OperatorFusionCodegenSupport {

  override def getOutputType: RowType = outputType

  override def getExprCodeGenerator: ExprCodeGenerator = exprCodeGenerator

  private lazy val exprCodeGenerator = new ExprCodeGenerator(operatorCtx, false, true)

  private lazy val joinType = joinSpec.getJoinType
  private lazy val hashJoinType = HashJoinType.of(
    leftIsBuild,
    joinType.isLeftOuter,
    joinType.isRightOuter,
    joinType == FlinkJoinType.SEMI,
    joinType == FlinkJoinType.ANTI)
  private lazy val keyType =
    RowType.of(joinSpec.getLeftKeys.map(idx => inputs(0).getOutputType.getTypeAt(idx)): _*)
  private lazy val (buildKeys, probeKeys) = leftIsBuild match {
    case true => (joinSpec.getLeftKeys, joinSpec.getRightKeys)
    case false => (joinSpec.getRightKeys, joinSpec.getLeftKeys)
  }
  private lazy val (buildType, probeType) = leftIsBuild match {
    case true => (inputs(0).getOutputType, inputs(1).getOutputType)
    case false => (inputs(1).getOutputType, inputs(0).getOutputType)
  }
  private lazy val (buildRowSize, buildRowCount) = leftIsBuild match {
    case true => (estimatedLeftAvgRowSize, estimatedLeftRowCount)
    case false => (estimatedRightAvgRowSize, estimatedRightRowCount)
  }

  private lazy val Seq(buildToBinaryRow, probeToBinaryRow) =
    newNames("buildToBinaryRow", "probeToBinaryRow")

  private lazy val hashTableTerm = newName("hashTable")

  override def doProduceProcess(multipleCtx: CodeGeneratorContext): Unit = {
    assert(inputs.size == 2)
    // call build side first, then call probe side
    if (leftIsBuild) {
      // build side
      this.inputs(0).produceProcess(multipleCtx, 1, this)
      // probe side
      this.inputs(1).produceProcess(multipleCtx, 2, this)
    } else {
      // build side
      this.inputs(1).produceProcess(multipleCtx, 2, this)
      // probe side
      this.inputs(0).produceProcess(multipleCtx, 1, this)
    }
  }

  override def doProduceEndInput(multipleCtx: CodeGeneratorContext): Unit = {
    // call build side first, then call probe side
    if (leftIsBuild) {
      // build side
      this.inputs(0).produceEndInput(multipleCtx)
      // probe side
      this.inputs(1).produceEndInput(multipleCtx)
    } else {
      // build side
      this.inputs(1).produceEndInput(multipleCtx)
      // probe side
      this.inputs(0).produceEndInput(multipleCtx)
    }
  }

  override def doConsumeProcess(
      multipleCtx: CodeGeneratorContext,
      inputId: Int,
      input: Seq[GeneratedExpression],
      row: GeneratedExpression): String = {
    // only probe side will call the consumeProcess method to consume the output record
    if (leftIsBuild) {
      if (inputId == 1) {
        codegenBuild(input, row)
      } else {
        hashJoinType match {
          case HashJoinType.INNER =>
            codegenInnerProbe(multipleCtx, input)
          case _ =>
            throw new UnsupportedOperationException(
              s"Multiple fusion codegen doesn't support $hashJoinType now.")
        }
      }
    } else {
      if (inputId == 1) {
        hashJoinType match {
          case HashJoinType.INNER =>
            codegenInnerProbe(multipleCtx, input)
          case _ =>
            throw new UnsupportedOperationException(
              s"Multiple fusion codegen doesn't support $hashJoinType now.")
        }
      } else {
        codegenBuild(input, row)
      }
    }
  }

  private def codegenBuild(input: Seq[GeneratedExpression], row: GeneratedExpression): String = {
    val (nullCheckBuildCode, nullCheckBuildTerm) =
      genAnyNullsInKeys(buildKeys, input)
    s"""
       |$nullCheckBuildCode
       |if (!$nullCheckBuildTerm) {
       |  ${row.getCode}
       |  $hashTableTerm.putBuildRow(${row.resultTerm} instanceof $BINARY_ROW ?
       |    ($BINARY_ROW) ${row.resultTerm} : $buildToBinaryRow.apply(${row.resultTerm}));
       |}
       """.stripMargin
  }

  private def codegenInnerProbe(
      multipleCtx: CodeGeneratorContext,
      input: Seq[GeneratedExpression]): String = {
    val (keyEv, anyNull) = genStreamSideJoinKey(keyType, probeKeys, input)
    val keyCode = keyEv.getCode
    val (matched, checkCondition, buildVars) = getJoinCondition(buildType)
    val resultVars = input ++ buildVars
    val buildIterTerm = newName("buildIter")
    // val probeInputRow = prepareInputRowVar(inputId, row.resultTerm, input)
    s"""
       |// generate join key for probe side
       |$keyCode
       |// find matches from hash table
       |${classOf[RowIterator[_]].getCanonicalName} $buildIterTerm = $anyNull ?
       |  null : $hashTableTerm.get(${keyEv.resultTerm});
       |if ($buildIterTerm != null ) {
       |  while ($buildIterTerm.advanceNext()) {
       |    $ROW_DATA $matched = $buildIterTerm.getRow();
       |    $checkCondition {
       |      ${consumeProcess(multipleCtx, resultVars)}
       |    }
       |  }
       |}
           """.stripMargin
  }

  override def doConsumeEndInput(multipleCtx: CodeGeneratorContext, inputId: Int): String = {
    // If the hash table spill to disk during runtime, the probe endInput also need to
    // consumeProcess to consume the spilled record
    if (leftIsBuild) {
      if (inputId == 1) {
        s"""
           |LOG.info("Finish build phase.");
           |$hashTableTerm.endBuild();
       """.stripMargin
      } else {
        // TODO support spill disk of hash table
        consumeEndInput(multipleCtx)
      }
    } else {
      if (inputId == 1) {
        consumeEndInput(multipleCtx)
      } else {
        s"""
           |LOG.info("Finish build phase.");
           |$hashTableTerm.endBuild();
       """.stripMargin
      }
    }
  }

  /**
   * Returns the code for generating join key for stream side, and expression of whether the key has
   * any null in it or not.
   */
  protected def genStreamSideJoinKey(
      keyType: RowType,
      probeKeyMapping: Array[Int],
      input: Seq[GeneratedExpression]): (GeneratedExpression, String) = {
    val supports =
      joinSpec.getFilterNulls.forall(b => b) &&
        keyType.getFieldCount == 1 && {
          keyType.getTypeAt(0).getTypeRoot match {
            case BIGINT | INTEGER | SMALLINT | TINYINT | FLOAT | DOUBLE | DATE |
                TIME_WITHOUT_TIME_ZONE =>
              true
            case TIMESTAMP_WITHOUT_TIME_ZONE =>
              val timestampType = keyType.getTypeAt(0).asInstanceOf[TimestampType]
              TimestampData.isCompact(timestampType.getPrecision)
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
              val lzTs = keyType.getTypeAt(0).asInstanceOf[LocalZonedTimestampType]
              TimestampData.isCompact(lzTs.getPrecision)
            case _ => false
          }
        }

    if (supports) {
      // generate the join key as Long
      val ev = input(probeKeyMapping(0))
      (ev, ev.nullTerm)
    } else {
      // generate the join key as UnsafeRow
      throw new UnsupportedOperationException(
        s"Multiple fusion codegen doesn't support multiple keys now.")
    }
  }

  def genAnyNullsInKeys(
      keyMapping: Array[Int],
      input: Seq[GeneratedExpression]): (String, String) = {
    val builder = new StringBuilder
    val codeBuilder = new StringBuilder
    val anyNullTerm = newName("anyNull")

    keyMapping.foreach(
      key => {
        codeBuilder.append(input(key).getCode + "\n")
        builder.append(s"$anyNullTerm |= ${input(key).nullTerm};")
      })
    (
      s"""
         |boolean $anyNullTerm = false;
         |$codeBuilder
         |$builder
     """.stripMargin,
      anyNullTerm)
  }

  protected def getJoinCondition(buildType: RowType): (String, String, Seq[GeneratedExpression]) = {
    val buildRow = newName("buildRow")
    val buildVars = genBuildSideVars(buildRow, buildType)
    val checkCondition = if (joinSpec.getNonEquiCondition.isPresent) {
      val expr = exprCodeGenerator.generateExpression(joinSpec.getNonEquiCondition.get)
      val skipRow = s"${expr.nullTerm} || !${expr.resultTerm}"
      s"""
         |${expr.getCode}
         |if (!($skipRow))
       """.stripMargin
    } else {
      ""
    }
    (buildRow, checkCondition, buildVars)
  }

  /** Generates the code for variables of build side. */
  protected def genBuildSideVars(buildRow: String, buildType: RowType): Seq[GeneratedExpression] = {
    val exprGenerator = new ExprCodeGenerator(
      new CodeGeneratorContext(operatorCtx.tableConfig, operatorCtx.classLoader),
      false,
      true)
      .bindInput(buildType, inputTerm = buildRow)

    hashJoinType match {
      case HashJoinType.INNER =>
        exprGenerator.generateInputAccessExprs()
      case _ =>
        throw new IllegalArgumentException(
          s"JoinCodegenSupport.genBuildSideVars should not take $hashJoinType as the JoinType")
    }
  }

  override def getOperatorCtx: CodeGeneratorContext = operatorCtx

  override def getManagedMemory: Long = managedMemory

  /**
   * This method is used to initialize operator code, it should be called before initCode & openCode
   * & closeCode.
   */
  override def initializeOperator(): Unit = {
    val buildSer = new BinaryRowDataSerializer(buildType.getFieldCount)
    val buildSerTerm = operatorCtx.addReusableObject(buildSer, "buildSer")
    val probeSer = new BinaryRowDataSerializer(probeType.getFieldCount)
    val probeSerTerm = operatorCtx.addReusableObject(probeSer, "probeSer")

    val bGenProj =
      genProjection(
        operatorCtx.tableConfig,
        operatorCtx.classLoader,
        buildType.getChildren.toArray(Array[LogicalType]()))
    operatorCtx.addReusableInnerClass(bGenProj.getClassName, bGenProj.getCode)
    val pGenProj =
      genProjection(
        operatorCtx.tableConfig,
        operatorCtx.classLoader,
        probeType.getChildren.toArray(Array[LogicalType]()))
    operatorCtx.addReusableInnerClass(pGenProj.getClassName, pGenProj.getCode)

    operatorCtx.addReusableMember(s"${bGenProj.getClassName} $buildToBinaryRow;")
    val buildProjRefs = operatorCtx.addReusableObject(bGenProj.getReferences, "buildProjRefs")
    operatorCtx.addReusableInitStatement(
      s"$buildToBinaryRow = new ${bGenProj.getClassName}($buildProjRefs);")

    operatorCtx.addReusableMember(s"${pGenProj.getClassName} $probeToBinaryRow;")
    val probeProjRefs = operatorCtx.addReusableObject(pGenProj.getReferences, "probeProjRefs")
    operatorCtx.addReusableInitStatement(
      s"$probeToBinaryRow = new ${pGenProj.getClassName}($probeProjRefs);")

    val hashTableClassTerm = newName("LongHashTable")
    val tableCode =
      s"""
         |public class $hashTableClassTerm extends ${classOf[LongHybridHashTable].getCanonicalName} {
         |
         |  public $hashTableClassTerm(long memorySize) {
         |    super(getContainingTask(),
         |      $compressionEnabled, $compressionBlockSize,
         |      $buildSerTerm, $probeSerTerm,
         |      getContainingTask().getEnvironment().getMemoryManager(),
         |      memorySize,
         |      getContainingTask().getEnvironment().getIOManager(),
         |      $buildRowSize,
         |      ${buildRowCount}L / getRuntimeContext().getNumberOfParallelSubtasks());
         |  }
         |
         |  @Override
         |  public long getBuildLongKey($ROW_DATA row) {
         |    ${genGetLongKey(keyType, buildKeys, "row")}
         |  }
         |
         |  @Override
         |  public long getProbeLongKey($ROW_DATA row) {
         |    ${genGetLongKey(keyType, probeKeys, "row")}
         |  }
         |
         |  @Override
         |  public $BINARY_ROW probeToBinary($ROW_DATA row) {
         |    if (row instanceof $BINARY_ROW) {
         |      return ($BINARY_ROW) row;
         |    } else {
         |      return $probeToBinaryRow.apply(row);
         |    }
         |  }
         |}
       """.stripMargin
    operatorCtx.addReusableInnerClass(hashTableClassTerm, tableCode)
    operatorCtx.addReusableMember(s"$hashTableClassTerm $hashTableTerm;")
    val memorySizeTerm = newName("memorySize")
    operatorCtx.addReusableOpenStatement(
      s"long $memorySizeTerm = computeMemorySize($managedMemoryFraction);")
    operatorCtx.addReusableOpenStatement(
      s"$hashTableTerm = new $hashTableClassTerm($memorySizeTerm);")

    operatorCtx.addReusableCloseStatement(s"""
                                             |if (this.$hashTableTerm != null) {
                                             |  this.$hashTableTerm.close();
                                             |  this.$hashTableTerm.free();
                                             |  this.$hashTableTerm = null;
                                             |}
       """.stripMargin)
  }
}
