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
import com.criteo.deepdiff.raw_part.{
  CommonRawDiffsPart,
  DatasetRawDiffsPart,
  MatchedRecordsRawDiffsPart,
  RawDiffPart
}
import org.scalatest.AppendedClues
import org.scalatest.exceptions.TestFailedException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class CheckerSpec extends AnyFlatSpec with Matchers with AppendedClues {
  import SparkUtils._
  import org.apache.spark.sql.types._

  private val record = Map.empty[String, Any]
  private def diff[K, Example <: DiffExample[_]](count: Int, examples: (K, Example)*): Diff[K, Example] =
    Diff(count, examples.toMap)

  it should "work on empty binaryRecordDiff" in {
    val emptyDataSetDiffs = DatasetDiffsBuilderSpec.emptyDatasetDiffs
    val emptyMatchedRecordRawDiffsPart = MatchedRecordsRawDiffsPart[String, Map[String, Any]](
      multipleMatches = RawDiffPart(0, Nil),
      kindOfDifferentRecords = KindOfDifferentRecords(hasDifferentNotNullField = 0,
                                                      hasOnlyLeftOnlyFields = 0,
                                                      hasOnlyRightOnlyFields = 0,
                                                      hasOnlyLeftAndRightOnlyFields = 0),
      recordDiffs = CommonRawDiffsPart(
        identical = 0,
        different = RawDiffPart(0, Nil),
        leftOnly = RawDiffPart(0, Nil),
        rightOnly = RawDiffPart(0, Nil)
      ),
      fieldsDiffs = Map.empty
    )
    val emptyDataSetRawDiffPart = DatasetRawDiffsPart(
      root = emptyMatchedRecordRawDiffsPart,
      explodedArrays = Map.empty
    )
    val builder = DatasetDiffsBuilder[String, Map[String, Any]]()

    Checker.check(result = emptyDataSetDiffs, expected = emptyDataSetDiffs)
    Checker.check(result = emptyDataSetDiffs, expected = builder)
    Checker.check(result = emptyDataSetRawDiffPart, expected = builder)
    Checker.check(result = emptyMatchedRecordRawDiffsPart, expected = builder)
  }

  private val dataSetRawDiffsPart = MatchedRecordsRawDiffsPart[String, Map[String, Any]](
    multipleMatches = RawDiffPart(count = 10,
                                  examples = List("a" -> DifferentExample(Nil, Seq(Map("key" -> "a"))),
                                                  "a2" -> DifferentExample(Seq(Map("key" -> "a2")), Nil))),
    kindOfDifferentRecords = KindOfDifferentRecords(hasDifferentNotNullField = 1,
                                                    hasOnlyLeftOnlyFields = 2,
                                                    hasOnlyRightOnlyFields = 3,
                                                    hasOnlyLeftAndRightOnlyFields = 4),
    recordDiffs = CommonRawDiffsPart(
      identical = 11,
      different = RawDiffPart(
        count = 12,
        examples = List("b" -> DifferentExample(Map("key" -> "b", "field" -> 2), Map("key" -> "b", "field" -> 20)))
      ),
      leftOnly = RawDiffPart(
        count = 13,
        examples = List("c" -> LeftOnlyExample(Map("key" -> "c")),
                        "c2" -> LeftOnlyExample(record),
                        "c3" -> LeftOnlyExample(record))
      ),
      rightOnly = RawDiffPart(
        count = 14,
        examples = List("d" -> RightOnlyExample(record), "d2" -> RightOnlyExample(Map("key" -> "d2")))
      )
    ),
    fieldsDiffs = Map(
      "field" -> CommonRawDiffsPart(
        identical = 21,
        different =
          RawDiffPart(count = 22, examples = List("e" -> DifferentExample(1, 2), "e2" -> DifferentExample(3, 4))),
        leftOnly =
          RawDiffPart(count = 23,
                      examples =
                        List("f" -> LeftOnlyExample(3), "f2" -> LeftOnlyExample(7), "f3" -> LeftOnlyExample(11))),
        rightOnly = RawDiffPart(count = 24, examples = List("g" -> RightOnlyExample(4)))
      )
    )
  )

  private val simpleBuilder = DatasetDiffsBuilder[String, Map[String, Any]]()
    .multipleMatches(count = 10,
                     examples = Seq(("a", Nil, Seq(Map("key" -> "a"))), ("a2", Seq(Map("key" -> "a2")), Nil)))
    .kindOfDifferent(content = 1, leftOnly = 2, rightOnly = 3, leftAndRightOnly = 4)
    .record(
      identical = 11,
      different = 12,
      leftOnly = 13,
      rightOnly = 14,
      diffExamples = Seq(("b", Map("key" -> "b", "field" -> 2), Map("key" -> "b", "field" -> 20))),
      leftExamples = Seq(("c", Map("key" -> "c")), ("c2", record), ("c3", record)),
      rightExamples = Seq(("d", record), ("d2", Map("key" -> "d2")))
    )
    .common(
      "field",
      identical = 21,
      different = 22,
      leftOnly = 23,
      rightOnly = 24,
      diffExamples = Seq(("e", 1, 2), ("e2", 3, 4)),
      leftExamples = Seq(("f", 3), ("f2", 7), ("f3", 11)),
      rightExamples = Seq(("g", 4))
    )
  private val builder =
    simpleBuilder
      .explodedArray(
        "array",
        DatasetDiffsBuilder[String, Map[String, Any]]()
          .multipleMatches(7, examples = Seq(("e1", Nil, Seq(Map("key" -> "e1")))))
          .kindOfDifferent(content = 10, leftOnly = 20, rightOnly = 30, leftAndRightOnly = 40)
          .record(identical = 21, different = 22, leftOnly = 23, rightOnly = 24)
          .leftOnly("left", identical = 31, leftOnly = 32, leftExamples = Seq(("h", 33)))
          .rightOnly("right", identical = 41, rightOnly = 42, rightExamples = Seq(("i", 43)))
      )
      .explodedArray(
        "left",
        DatasetDiffsBuilder.leftOnlyRecordDiffs(Set("key"), leftExamples = Seq(("a1", Map("key" -> "a1")))))
      .explodedArray("right",
                     DatasetDiffsBuilder.rightOnlyRecordDiffs(Set("key", "c"),
                                                              rightExamples = Seq(("b1", Map("key" -> "b1")),
                                                                                  ("b2", Map("key" -> "b2")))))
      .schema(left = struct("a" -> IntegerType, "b" -> StringType),
              right = struct("a" -> IntegerType, "b" -> LongType, "c" -> StringType))

  private val expectedDataSetDiffs = builder.result

  it should "validate a DataSetDiffsPart" in {
    val simpleExpectedDataSetDiffs = simpleBuilder.result
    Checker.check(result = simpleExpectedDataSetDiffs, expected = simpleExpectedDataSetDiffs)
    Checker.check(result = simpleExpectedDataSetDiffs, expected = simpleBuilder)
    Checker.check(result = dataSetRawDiffsPart, expected = simpleBuilder)
  }

  it should "detect differences in multiple matches" in {
    for (
      multipleMatches <- Seq[Diff[String, DifferentExample[Seq[Map[String, Any]]]]](
        // different count
        expectedDataSetDiffs.root.multipleMatches.copy(count = 1),
        // no examples
        diff(count = 10),
        // missing example a2
        diff(count = 10, "a" -> DifferentExample(Nil, Seq(Map("key" -> "a")))),
        // different example a (Nil vs Seq(record))
        diff(count = 10,
             "a" -> DifferentExample(Seq(record), Seq(Map("key" -> "a"))),
             "a2" -> DifferentExample(Seq(Map("key" -> "a2")), Nil)),
        // different example a2 (record is different, a2 vs a22)
        diff(count = 10,
             "a" -> DifferentExample(Nil, Seq(Map("key" -> "a"))),
             "a2" -> DifferentExample(Seq(Map("key" -> "a22").asInstanceOf[Map[String, Any]]), Nil)),
        // more examples
        diff(count = 10,
             "a" -> DifferentExample(Nil, Seq(Map("key" -> "a"))),
             "a2" -> DifferentExample(Seq(Map("key" -> "a2")), Nil),
             "a3" -> DifferentExample(Nil, Seq(record)))
      )
    ) {
      withClue(multipleMatches) {
        val changed =
          expectedDataSetDiffs.copy(root = expectedDataSetDiffs.root.copy(multipleMatches = multipleMatches))
        a[TestFailedException] should be thrownBy {
          Checker.check(result = expectedDataSetDiffs, expected = changed)
        }
        a[TestFailedException] should be thrownBy {
          Checker.check(result = changed, expected = builder)
        }
      }
    }
  }

  it should "detect differences in different row types" in {
    for (
      kindOfDifferentRecords <- Seq[KindOfDifferentRecords](
        // different hasDifferentNotNullField
        KindOfDifferentRecords(hasDifferentNotNullField = 10,
                               hasOnlyLeftOnlyFields = 2,
                               hasOnlyRightOnlyFields = 3,
                               hasOnlyLeftAndRightOnlyFields = 4),
        // different hasOnlyLeftOnlyFields
        KindOfDifferentRecords(hasDifferentNotNullField = 1,
                               hasOnlyLeftOnlyFields = 20,
                               hasOnlyRightOnlyFields = 3,
                               hasOnlyLeftAndRightOnlyFields = 4),
        // different hasOnlyRightOnlyFields
        KindOfDifferentRecords(hasDifferentNotNullField = 1,
                               hasOnlyLeftOnlyFields = 2,
                               hasOnlyRightOnlyFields = 30,
                               hasOnlyLeftAndRightOnlyFields = 4),
        // different hasOnlyLeftAndRightOnlyFields
        KindOfDifferentRecords(hasDifferentNotNullField = 1,
                               hasOnlyLeftOnlyFields = 2,
                               hasOnlyRightOnlyFields = 3,
                               hasOnlyLeftAndRightOnlyFields = 40)
      )
    ) {
      withClue(kindOfDifferentRecords) {
        val changed = expectedDataSetDiffs.copy(
          root = expectedDataSetDiffs.root.copy(kindOfDifferentRecords = kindOfDifferentRecords))
        a[TestFailedException] should be thrownBy {
          Checker.check(result = expectedDataSetDiffs, expected = changed)
        }
        a[TestFailedException] should be thrownBy {
          Checker.check(result = changed, expected = builder)
        }
      }
    }
  }

  it should "detect differences in fields" in {
    val field = expectedDataSetDiffs.root.fieldsDiffs("field").asInstanceOf[CommonDiffs[String, Integer]]
    for (
      leftRightDiffs <- Seq[LeftRightDiffs[String, Integer]](
        // different count
        field.copy(identical = 100),
        field.copy(different = field.different.copy(count = 100)),
        field.copy(leftOnly = field.leftOnly.copy(count = 100)),
        field.copy(rightOnly = field.rightOnly.copy(count = 100)),
        // missing examples
        field.copy(different = diff(22)),
        field.copy(leftOnly = diff(23)),
        field.copy(rightOnly = diff(24)),
        // different examples
        field.copy(different = diff(22, "e" -> DifferentExample(1, 2))),
        field.copy(different = diff(22, "e" -> DifferentExample(1, 20), "e2" -> DifferentExample(3, 4))),
        field.copy(different = diff(22, "e" -> DifferentExample(10, 2), "e2" -> DifferentExample(3, 4))),
        field.copy(different =
          diff(22, "e" -> DifferentExample(1, 2), "e2" -> DifferentExample(3, 4), "e3" -> DifferentExample(5, 6))),
        field.copy(leftOnly = diff(count = 23, "f" -> LeftOnlyExample(3), "f3" -> LeftOnlyExample(11))),
        field.copy(leftOnly =
          diff(count = 23, "f" -> LeftOnlyExample(3), "f2" -> LeftOnlyExample(70), "f3" -> LeftOnlyExample(11))),
        field.copy(
          leftOnly = diff(count = 23,
                          "f" -> LeftOnlyExample(3),
                          "f2" -> LeftOnlyExample(7),
                          "f3" -> LeftOnlyExample(11),
                          "f4" -> LeftOnlyExample(15))),
        field.copy(rightOnly = diff(count = 24, "g" -> RightOnlyExample(8))),
        field.copy(rightOnly = diff(count = 24, "g" -> RightOnlyExample(4), "g2" -> RightOnlyExample(8)))
      )
    ) {
      withClue(leftRightDiffs) {
        val changed = expectedDataSetDiffs.copy(
          root = expectedDataSetDiffs.root.copy(
            fieldsDiffs = expectedDataSetDiffs.root.fieldsDiffs ++ Map("field" -> leftRightDiffs)
          ))
        a[TestFailedException] should be thrownBy {
          Checker.check(result = expectedDataSetDiffs, expected = changed)
        }
        a[TestFailedException] should be thrownBy {
          Checker.check(result = changed, expected = builder)
        }
      }
    }
  }

  it should "check the exploded arrays" in {
    val array = expectedDataSetDiffs.explodedArrays("array").asInstanceOf[MatchedRecordsDiffs[String, Map[String, Any]]]
    for (
      explodedArray <- Seq[(String, RecordDiffs[String, Map[String, Any]])](
        // missing examples
        "array" -> array.copy(multipleMatches = diff(7)),
        // different count
        "array" -> array.copy(multipleMatches = diff(10, "e1" -> DifferentExample(Nil, Seq(Map("key" -> "e1"))))),
        // left - different fields
        "left" -> DatasetDiffsBuilder.leftOnlyRecordDiffs(Set("key", "new"),
                                                          leftExamples = Seq(("a1", Map("key" -> "a1")))),
        "left" -> DatasetDiffsBuilder.leftOnlyRecordDiffs(Set("key2"), leftExamples = Seq(("a1", Map("key" -> "a1")))),
        "left" -> DatasetDiffsBuilder.leftOnlyRecordDiffs(Set.empty, leftExamples = Seq(("a1", Map("key" -> "a1")))),
        // left - different example
        "left" -> DatasetDiffsBuilder.leftOnlyRecordDiffs(Set("key"), leftExamples = Nil),
        "left" -> DatasetDiffsBuilder
          .leftOnlyRecordDiffs(Set("key"), leftExamples = Seq(("a1", Map("key" -> "a1")), ("a2", Map("key" -> "a2")))),
        "left" -> DatasetDiffsBuilder.leftOnlyRecordDiffs(Set("key"), leftExamples = Seq(("a2", Map("key" -> "a2")))),
        // right - different fields
        "right" -> DatasetDiffsBuilder.rightOnlyRecordDiffs(Set("key", "c2"),
                                                            rightExamples = Seq(("b1", Map("key" -> "b1")),
                                                                                ("b2", Map("key" -> "b2")))),
        "right" -> DatasetDiffsBuilder.rightOnlyRecordDiffs(Set("key"),
                                                            rightExamples = Seq(("b1", Map("key" -> "b1")),
                                                                                ("b2", Map("key" -> "b2")))),
        "right" -> DatasetDiffsBuilder.rightOnlyRecordDiffs(Set("key", "c", "c2"),
                                                            rightExamples = Seq(("b1", Map("key" -> "b1")),
                                                                                ("b2", Map("key" -> "b2")))),
        // right - different example
        "right" -> DatasetDiffsBuilder.rightOnlyRecordDiffs(Set("key", "c"),
                                                            rightExamples = Seq(("b1", Map("key" -> "b1")))),
        "right" -> DatasetDiffsBuilder.rightOnlyRecordDiffs(Set("key", "c"),
                                                            rightExamples = Seq(("b1", Map("key" -> "b1")),
                                                                                ("b2X", Map("key" -> "b2X")))),
        "right" -> DatasetDiffsBuilder.rightOnlyRecordDiffs(
          Set("key", "c"),
          rightExamples = Seq(("b1", Map("key" -> "b1")), ("b2", Map("key" -> "b2")), ("b3", Map("key" -> "b3"))))
      )
    ) {
      withClue(explodedArray) {
        val changed =
          expectedDataSetDiffs.copy(explodedArrays = expectedDataSetDiffs.explodedArrays ++ Map(explodedArray))
        a[TestFailedException] should be thrownBy {
          Checker.check(result = expectedDataSetDiffs, expected = changed)
        }
        a[TestFailedException] should be thrownBy {
          Checker.check(result = changed, expected = builder)
        }
      }
    }
  }

  it should "check the metadata" in {
    for (
      left <- Seq(
        struct("a" -> LongType, "b" -> StringType), // changed a type
        struct("a" -> IntegerType, "b" -> StringType, "c" -> LongType), // new field
        struct("a" -> IntegerType) // missing field
      )
    ) {
      val changed = expectedDataSetDiffs.copy(
        schema = expectedDataSetDiffs.schema.copy(left = left)
      )
      a[TestFailedException] should be thrownBy {
        Checker.check(result = expectedDataSetDiffs, expected = changed)
      }
      a[TestFailedException] should be thrownBy {
        Checker.check(result = changed, expected = builder)
      }
    }
    for (
      right <- Seq(
        struct("a" -> IntegerType, "b" -> ByteType, "c" -> StringType), // changed b type
        struct("a" -> IntegerType, "b" -> LongType, "c" -> StringType, "d" -> BooleanType), // new field
        struct("a" -> IntegerType, "b" -> LongType) // missing field
      )
    ) {
      val changed = expectedDataSetDiffs.copy(
        schema = expectedDataSetDiffs.schema.copy(right = right)
      )
      a[TestFailedException] should be thrownBy {
        Checker.check(result = expectedDataSetDiffs, expected = changed)
      }
      a[TestFailedException] should be thrownBy {
        Checker.check(result = changed, expected = builder)
      }
    }
  }

  // Ordering of the fields has little importance and it makes tests a lot easier.
  it should "support different ordering of the metadata schemas" in {
    for (
      (schemaA, schemaB) <- Seq[(StructType, StructType)](
        // root ordering
        (struct("a" -> IntegerType, "b" -> StringType), struct("b" -> StringType, "a" -> IntegerType)),
        // nested struct
        (struct("a" -> IntegerType, "b" -> struct("c" -> ByteType, "d" -> LongType)),
         struct("a" -> IntegerType, "b" -> struct("d" -> LongType, "c" -> ByteType))),
        (struct("a" -> IntegerType, "b" -> struct("c" -> ByteType, "d" -> LongType)),
         struct("b" -> struct("d" -> LongType, "c" -> ByteType), "a" -> IntegerType)),
        // array of struct
        (struct("a" -> IntegerType, "b" -> ArrayType(struct("c" -> ByteType, "d" -> LongType))),
         struct("a" -> IntegerType, "b" -> ArrayType(struct("d" -> LongType, "c" -> ByteType)))),
        (struct("a" -> IntegerType, "b" -> ArrayType(struct("c" -> ByteType, "d" -> LongType))),
         struct("b" -> ArrayType(struct("d" -> LongType, "c" -> ByteType)), "a" -> IntegerType))
      )
    ) {
      val a = expectedDataSetDiffs.copy(schema = DatasetsSchema(left = schemaA, right = schemaA))
      val b = expectedDataSetDiffs.copy(schema = DatasetsSchema(left = schemaB, right = schemaB))
      Checker.check(result = a, expected = b)
    }
  }
}
