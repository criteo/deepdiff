/*
 * Copyright 2020 Criteo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.criteo.deepdiff.test_utils

import com.criteo.deepdiff._
import org.apache.spark.sql.types.StructType

/** Used to create DataSetDiffs in concise manner. */
final case class DatasetDiffsBuilder[Key, Record] private (
    private val multipleMatches: Option[Diff[Key, DifferentExample[Seq[Record]]]] = None,
    private val recordDiffs: Option[CommonDiffs[Key, Record]] = None,
    private val kindOfDifferentRecords: Option[KindOfDifferentRecords] = None,
    private val fieldDiffs: List[(String, LeftRightDiffs[Key, Any])] = Nil,
    private val explodedArrays: List[(String, RecordDiffs[Key, Record])] = Nil,
    private val schemas: Option[DatasetsSchema] = None
) {
  def multipleMatches[L <: Record, R <: Record](
      count: Int = -1,
      examples: Seq[(Key, Seq[L], Seq[R])] = Nil): DatasetDiffsBuilder[Key, Record] = {
    if (multipleMatches.isDefined) {
      throw new IllegalStateException("checkMultipleMatches() has already been called once.")
    }
    copy(
      multipleMatches = Some(
        Diff(
          count = if (count >= 0) count else examples.length,
          examples.map({ case (key, left, right) => key -> DifferentExample(left, right) }).toMap
        )))
  }

  def kindOfDifferent(content: Int = 0,
                      leftOnly: Int = 0,
                      rightOnly: Int = 0,
                      leftAndRightOnly: Int = 0): DatasetDiffsBuilder[Key, Record] = {
    if (kindOfDifferentRecords.isDefined) {
      throw new IllegalStateException("checkDifferentTypes() has already been called once.")
    }
    copy(kindOfDifferentRecords = Some(KindOfDifferentRecords(content, leftOnly, rightOnly, leftAndRightOnly)))
  }

  def record[L <: Record, R <: Record](identical: Int = 0,
                                       different: Int = -1,
                                       leftOnly: Int = -1,
                                       rightOnly: Int = -1,
                                       diffExamples: Seq[(Key, L, R)] = Nil,
                                       leftExamples: Seq[(Key, L)] = Nil,
                                       rightExamples: Seq[(Key, R)] = Nil): DatasetDiffsBuilder[Key, Record] = {
    if (recordDiffs.isDefined) {
      throw new IllegalStateException("checkRows() has already been called once.")
    }
    copy(
      recordDiffs =
        Some(buildCommonDiff(identical, different, leftOnly, rightOnly, diffExamples, leftExamples, rightExamples)))
  }

  def common[T](name: String,
                identical: Int = 0,
                different: Int = -1,
                leftOnly: Int = -1,
                rightOnly: Int = -1,
                diffExamples: Seq[(Key, T, T)] = Nil,
                leftExamples: Seq[(Key, T)] = Nil,
                rightExamples: Seq[(Key, T)] = Nil): DatasetDiffsBuilder[Key, Record] = {
    assertUniqueField(name)
    copy(
      fieldDiffs = (
        name -> buildCommonDiff(identical, different, leftOnly, rightOnly, diffExamples, leftExamples, rightExamples)
      ) :: fieldDiffs
    )
  }

  def leftOnly[T](name: String,
                  identical: Int = 0,
                  leftOnly: Int = -1,
                  leftExamples: Seq[(Key, T)] = Nil): DatasetDiffsBuilder[Key, Record] = {
    assertUniqueField(name)
    copy(
      fieldDiffs = (
        name -> LeftOnlyDiffs(
          identical,
          leftOnly = Diff(
            count = if (leftOnly >= 0) leftOnly else leftExamples.length,
            leftExamples.map({ case (key, left) => key -> LeftOnlyExample(left) }).toMap
          )
        )
      ) :: fieldDiffs
    )
  }

  def rightOnly[T](name: String,
                   identical: Int = 0,
                   rightOnly: Int = -1,
                   rightExamples: Seq[(Key, T)] = Nil): DatasetDiffsBuilder[Key, Record] = {
    assertUniqueField(name)
    copy(
      fieldDiffs = (
        name -> RightOnlyDiffs(
          identical,
          rightOnly = Diff(
            count = if (rightOnly >= 0) rightOnly else rightExamples.length,
            rightExamples.map({ case (key, right) => key -> RightOnlyExample(right) }).toMap
          )
        )
      ) :: fieldDiffs
    )
  }

  def explodedArray(name: String, builder: DatasetDiffsBuilder[Key, Record]): DatasetDiffsBuilder[Key, Record] = {
    assertUniqueExplodedArray(name)
    copy(
      explodedArrays = (name -> builder.result.root) :: explodedArrays
    )
  }

  def explodedArray(name: String, recordDiffs: RecordDiffs[Key, Record]): DatasetDiffsBuilder[Key, Record] = {
    assertUniqueExplodedArray(name)
    copy(
      explodedArrays = (name -> recordDiffs) :: explodedArrays
    )
  }

  def schema(left: StructType, right: StructType): DatasetDiffsBuilder[Key, Record] = {
    assert(schemas.isEmpty)
    copy(
      schemas = Some(DatasetsSchema(left, right))
    )
  }

  private[test_utils] def schemaIfAbsent(left: StructType, right: StructType): DatasetDiffsBuilder[Key, Record] = {
    copy(
      schemas = schemas.orElse(Some(DatasetsSchema(left, right)))
    )
  }

  private[test_utils] def transform[R](transformRecord: Record => R,
                                       transformField: Any => Any): DatasetDiffsBuilder[Key, R] = {
    val result =
      RecordDiffsUtils.transform(root, transformRecord, transformField).asInstanceOf[MatchedRecordsDiffs[Key, R]]
    new DatasetDiffsBuilder(
      multipleMatches = Some(result.multipleMatches),
      kindOfDifferentRecords = kindOfDifferentRecords,
      recordDiffs = Some(result.recordDiffs),
      fieldDiffs = result.fieldsDiffs.toList,
      explodedArrays = explodedArrays.map({
        case (name, e) => name -> RecordDiffsUtils.transform(e, transformRecord, transformField)
      }),
      schemas = schemas
    )
  }

  private[test_utils] def reverseLeftAndRight: DatasetDiffsBuilder[Key, Record] = {
    val result = RecordDiffsUtils.reverseLeftAndRight(root).asInstanceOf[MatchedRecordsDiffs[Key, Record]]
    copy(
      multipleMatches = Some(result.multipleMatches),
      recordDiffs = Some(result.recordDiffs),
      fieldDiffs = result.fieldsDiffs.toList,
      kindOfDifferentRecords = Some(result.kindOfDifferentRecords),
      explodedArrays = explodedArrays.map({ case (name, e) => name -> RecordDiffsUtils.reverseLeftAndRight(e) }),
      schemas = schemas.map(m => m.copy(m.right, m.left))
    )
  }

  private def assertUniqueField(name: String): Unit = {
    if (fieldDiffs.map(_._1).contains(name)) {
      throw new IllegalStateException(s"checkField($name, ...) has already been called once.")
    }
  }

  private def assertUniqueExplodedArray(name: String): Unit = {
    if (explodedArrays.map(_._1).contains(name)) {
      throw new IllegalStateException(s"explodedArray($name, ...) has already been called once.")
    }
  }

  private def buildCommonDiff[T](identical: Int,
                                 different: Int,
                                 leftOnly: Int,
                                 rightOnly: Int,
                                 diffExamples: Seq[(Key, T, T)],
                                 leftExamples: Seq[(Key, T)],
                                 rightExamples: Seq[(Key, T)]): CommonDiffs[Key, T] = {
    CommonDiffs[Key, T](
      identical,
      different = Diff(
        count = if (different >= 0) different else diffExamples.length,
        diffExamples.map({ case (key, left, right) => key -> DifferentExample(left, right) }).toMap
      ),
      leftOnly = Diff(
        count = if (leftOnly >= 0) leftOnly else leftExamples.length,
        leftExamples.map({ case (key, left) => key -> LeftOnlyExample(left) }).toMap
      ),
      rightOnly = Diff(
        count = if (rightOnly >= 0) rightOnly else rightExamples.length,
        rightExamples.map({ case (key, right) => key -> RightOnlyExample(right) }).toMap
      )
    )
  }

  lazy val result: DatasetDiffs[Key, Record] = {
    DatasetDiffs[Key, Record](
      root = root,
      explodedArrays = explodedArrays.toMap,
      schema = schemas.getOrElse(DatasetDiffsBuilder.emptySchema)
    )
  }

  private def root: MatchedRecordsDiffs[Key, Record] = {
    var checker = this
    checker = if (recordDiffs.isEmpty) checker.record() else checker
    checker = if (multipleMatches.isEmpty) checker.multipleMatches() else checker
    checker = if (kindOfDifferentRecords.isEmpty) checker.kindOfDifferent() else checker

    MatchedRecordsDiffs(
      checker.multipleMatches.get,
      checker.recordDiffs.get,
      checker.kindOfDifferentRecords.get,
      checker.fieldDiffs.toMap
    )
  }
}

object DatasetDiffsBuilder {
  val emptySchema: DatasetsSchema = DatasetsSchema(StructType(Nil), StructType(Nil))
  def apply[Key, Record](): DatasetDiffsBuilder[Key, Record] = new DatasetDiffsBuilder[Key, Record]()

  def leftOnlyRecordDiffs[Key, Record](fields: Set[String],
                                       leftOnly: Int = -1,
                                       leftExamples: Seq[(Key, Record)] = Nil): LeftOnlyRecordDiffs[Key, Record] = {
    LeftOnlyRecordDiffs(
      records = Diff(
        count = if (leftOnly >= 0) leftOnly else leftExamples.length,
        leftExamples.map({ case (key, left) => key -> LeftOnlyExample(left) }).toMap
      ),
      fields
    )
  }
  def rightOnlyRecordDiffs[Key, Record](fields: Set[String],
                                        rightOnly: Int = -1,
                                        rightExamples: Seq[(Key, Record)] = Nil): RightOnlyRecordDiffs[Key, Record] = {
    RightOnlyRecordDiffs(
      records = Diff(
        count = if (rightOnly >= 0) rightOnly else rightExamples.length,
        rightExamples.map({ case (key, right) => key -> RightOnlyExample(right) }).toMap
      ),
      fields
    )
  }
}
