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
import com.criteo.deepdiff.test_utils.SparkUtils.struct
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object DatasetDiffsBuilderSpec {
  val emptyDatasetDiffs = DatasetDiffs(
    root = MatchedRecordsDiffs[String, Map[String, Any]](
      multipleMatches = Diff(0, Map.empty),
      kindOfDifferentRecords = KindOfDifferentRecords(hasDifferentNotNullField = 0,
                                                      hasOnlyLeftOnlyFields = 0,
                                                      hasOnlyRightOnlyFields = 0,
                                                      hasOnlyLeftAndRightOnlyFields = 0),
      recordDiffs = CommonDiffs(
        identical = 0,
        different = Diff(0, Map.empty),
        leftOnly = Diff(0, Map.empty),
        rightOnly = Diff(0, Map.empty)
      ),
      fieldsDiffs = Map.empty
    ),
    explodedArrays = Map.empty,
    schema = DatasetDiffsBuilder.emptySchema
  )
}

final class DatasetDiffsBuilderSpec extends AnyFlatSpec with Matchers {
  import DatasetDiffsBuilderSpec._

  private def diff[K, Example <: DiffExample[_]](count: Int, examples: (K, Example)*): Diff[K, Example] =
    Diff(count, examples.toMap)

  private val record = Map.empty[String, Any]

  it should "generate properly an empty DataSetDiffs" in {
    DatasetDiffsBuilder[String, Map[String, Any]]().result should be(emptyDatasetDiffs)
  }

  it should "build correctly the metadata" in {
    val l = struct("f1" -> IntegerType, "f2" -> LongType)
    val r = struct("f1" -> IntegerType, "f3" -> StringType)
    DatasetDiffsBuilder[String, Map[String, Any]]()
      .schema(l, r)
      .result should be(emptyDatasetDiffs.copy(schema = DatasetsSchema(l, r)))

    an[AssertionError] should be thrownBy {
      DatasetDiffsBuilder[String, Map[String, Any]]()
        .schema(l, r)
        .schema(l, r)
    }

    val l2 = struct("z1" -> IntegerType, "z2" -> LongType)
    val r2 = struct("z1" -> IntegerType, "z3" -> StringType)
    DatasetDiffsBuilder[String, Map[String, Any]]()
      .schema(l, r)
      .schemaIfAbsent(l2, r2)
      .result should be(emptyDatasetDiffs.copy(schema = DatasetsSchema(l, r)))

    DatasetDiffsBuilder[String, Map[String, Any]]()
      .schemaIfAbsent(l2, r2)
      .result should be(emptyDatasetDiffs.copy(schema = DatasetsSchema(l2, r2)))
  }

  it should "build correctly a DataSetDiffs" in {
    val builder = DatasetDiffsBuilder[String, Map[String, Any]]()
      .multipleMatches(count = 10,
                       examples = Seq(("a", Nil, Seq(Map("key" -> "a"))),
                                      ("a2", Nil, Seq(Map("key" -> "a2"), Map("key" -> "a2")))))
      .kindOfDifferent(content = 1, leftOnly = 2, rightOnly = 3, leftAndRightOnly = 4)
      .record(
        identical = 11,
        different = 12,
        leftOnly = 13,
        rightOnly = 14,
        diffExamples = Seq(("b", Map("key" -> "b", "field" -> 1), Map("key" -> "b", "field" -> 2))),
        leftExamples = Seq(("c", record), ("c2", Map("key" -> "c2")), ("c3", record)),
        rightExamples = Seq(("d", Map("key" -> "d")), ("d2", record))
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
      .leftOnly(
        "left",
        identical = 31,
        leftOnly = 32,
        leftExamples = Seq(("h", 33))
      )
      .rightOnly(
        "right",
        identical = 41,
        rightOnly = 42,
        rightExamples = Seq(("i", 43))
      )
    val expected = DatasetDiffs(
      root = MatchedRecordsDiffs[String, Map[String, Any]](
        multipleMatches = diff(count = 10,
                               "a" -> DifferentExample(Nil, Seq(Map("key" -> "a"))),
                               "a2" -> DifferentExample(Nil, Seq(Map("key" -> "a2"), Map("key" -> "a2")))),
        kindOfDifferentRecords = KindOfDifferentRecords(hasDifferentNotNullField = 1,
                                                        hasOnlyLeftOnlyFields = 2,
                                                        hasOnlyRightOnlyFields = 3,
                                                        hasOnlyLeftAndRightOnlyFields = 4),
        recordDiffs = CommonDiffs(
          identical = 11,
          different =
            diff(count = 12, "b" -> DifferentExample(Map("key" -> "b", "field" -> 1), Map("key" -> "b", "field" -> 2))),
          leftOnly = diff(count = 13,
                          "c" -> LeftOnlyExample(record),
                          "c2" -> LeftOnlyExample(Map("key" -> "c2")),
                          "c3" -> LeftOnlyExample(record)),
          rightOnly = diff(count = 14, "d" -> RightOnlyExample(Map("key" -> "d")), "d2" -> RightOnlyExample(record))
        ),
        fieldsDiffs = Map(
          "field" -> CommonDiffs(
            identical = 21,
            different = diff(count = 22, "e" -> DifferentExample(1, 2), "e2" -> DifferentExample(3, 4)),
            leftOnly =
              diff(count = 23, "f" -> LeftOnlyExample(3), "f2" -> LeftOnlyExample(7), "f3" -> LeftOnlyExample(11)),
            rightOnly = diff(count = 24, "g" -> RightOnlyExample(4))
          ),
          "left" -> LeftOnlyDiffs(
            nulls = 31,
            leftOnly = diff(count = 32, "h" -> LeftOnlyExample(33))
          ),
          "right" -> RightOnlyDiffs(
            nulls = 41,
            rightOnly = diff(count = 42, "i" -> RightOnlyExample(43))
          )
        )
      ),
      explodedArrays = Map.empty,
      schema = DatasetDiffsBuilder.emptySchema
    )
    builder.result should be(expected)
    DatasetDiffsBuilder[String, Map[String, Any]]()
      .explodedArray("array", builder)
      .result should be(
      emptyDatasetDiffs.copy(explodedArrays = Map(
        "array" -> expected.root
      ))
    )
  }

  it should "infer count from the number of examples if not specified" in {
    val builder = DatasetDiffsBuilder[String, Map[String, Any]]()
      .multipleMatches(examples = Seq(("a", Nil, Seq(record)), ("a2", Nil, Seq(record))))
      .kindOfDifferent()
      .record(
        diffExamples = Seq(("b", record, record)),
        leftExamples = Seq(("c", record), ("c2", record), ("c3", record)),
        rightExamples = Seq(("d", record), ("d2", record))
      )
      .common("field",
              diffExamples = Seq(("e", 1, 2), ("e2", 3, 4)),
              leftExamples = Seq(("f", 3), ("f2", 7), ("f3", 11)),
              rightExamples = Seq(("g", 4)))
      .leftOnly(
        "left",
        leftExamples = Seq(("h", 33))
      )
      .rightOnly(
        "right",
        rightExamples = Seq(("i", 43), ("j", 44))
      )
    val expected = DatasetDiffs(
      root = MatchedRecordsDiffs[String, Map[String, Any]](
        multipleMatches = diff(count = 2,
                               "a" -> DifferentExample(Seq.empty, Seq(record)),
                               "a2" -> DifferentExample(Seq.empty, Seq(record))),
        kindOfDifferentRecords = KindOfDifferentRecords(hasDifferentNotNullField = 0,
                                                        hasOnlyLeftOnlyFields = 0,
                                                        hasOnlyRightOnlyFields = 0,
                                                        hasOnlyLeftAndRightOnlyFields = 0),
        recordDiffs = CommonDiffs(
          identical = 0,
          different = diff(count = 1, "b" -> DifferentExample(record, record)),
          leftOnly = diff(count = 3,
                          "c" -> LeftOnlyExample(record),
                          "c2" -> LeftOnlyExample(record),
                          "c3" -> LeftOnlyExample(record)),
          rightOnly = diff(count = 2, "d" -> RightOnlyExample(record), "d2" -> RightOnlyExample(record))
        ),
        fieldsDiffs = Map(
          "field" -> CommonDiffs(
            identical = 0,
            different = diff(count = 2, "e" -> DifferentExample(1, 2), "e2" -> DifferentExample(3, 4)),
            leftOnly =
              diff(count = 3, "f" -> LeftOnlyExample(3), "f2" -> LeftOnlyExample(7), "f3" -> LeftOnlyExample(11)),
            rightOnly = diff(count = 1, "g" -> RightOnlyExample(4))
          ),
          "left" -> LeftOnlyDiffs(
            nulls = 0,
            leftOnly = diff(count = 1, "h" -> LeftOnlyExample(33))
          ),
          "right" -> RightOnlyDiffs(
            nulls = 0,
            rightOnly = diff(count = 2, "i" -> RightOnlyExample(43), "j" -> RightOnlyExample(44))
          )
        )
      ),
      explodedArrays = Map.empty,
      schema = DatasetDiffsBuilder.emptySchema
    )

    builder.result should be(expected)
    DatasetDiffsBuilder[String, Map[String, Any]]()
      .explodedArray("array", builder)
      .result should be(
      emptyDatasetDiffs.copy(explodedArrays = Map(
        "array" -> expected.root
      ))
    )
  }

  it should "build left/right only exploded arrays correctly" in {
    DatasetDiffsBuilder[String, Map[String, Any]]()
      .explodedArray("left",
                     DatasetDiffsBuilder.leftOnlyRecordDiffs(Set("key"),
                                                             leftOnly = 12,
                                                             leftExamples = Seq(("a1", Map("key" -> "a1")))))
      .explodedArray("right",
                     DatasetDiffsBuilder.rightOnlyRecordDiffs(Set("key", "c"),
                                                              rightOnly = 24,
                                                              rightExamples = Seq(("b1", Map("key" -> "b1")),
                                                                                  ("b2", Map("key" -> "b2")))))
      .result should be(
      emptyDatasetDiffs.copy(explodedArrays = Map(
        "left" -> LeftOnlyRecordDiffs(
          records = diff(count = 12, "a1" -> LeftOnlyExample(Map("key" -> "a1"))),
          fields = Set("key")
        ),
        "right" -> RightOnlyRecordDiffs(
          records = diff(count = 24,
                         "b1" -> RightOnlyExample(Map("key" -> "b1")),
                         "b2" -> RightOnlyExample(Map("key" -> "b2"))),
          fields = Set("key", "c")
        )
      ))
    )

    DatasetDiffsBuilder[String, Map[String, Any]]()
      .explodedArray("left",
                     DatasetDiffsBuilder.leftOnlyRecordDiffs(Set("key"),
                                                             leftExamples = Seq(("a1", Map("key" -> "a1")))))
      .explodedArray("right",
                     DatasetDiffsBuilder.rightOnlyRecordDiffs(Set("key", "c"),
                                                              rightExamples = Seq(("b1", Map("key" -> "b1")),
                                                                                  ("b2", Map("key" -> "b2")))))
      .result should be(
      emptyDatasetDiffs.copy(explodedArrays = Map(
        "left" -> LeftOnlyRecordDiffs(
          records = diff(count = 1, "a1" -> LeftOnlyExample(Map("key" -> "a1"))),
          fields = Set("key")
        ),
        "right" -> RightOnlyRecordDiffs(
          records =
            diff(count = 2, "b1" -> RightOnlyExample(Map("key" -> "b1")), "b2" -> RightOnlyExample(Map("key" -> "b2"))),
          fields = Set("key", "c")
        )
      ))
    )
  }
}
