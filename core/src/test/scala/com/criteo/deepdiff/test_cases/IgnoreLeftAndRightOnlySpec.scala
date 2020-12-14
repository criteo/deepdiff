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

package com.criteo.deepdiff.test_cases

import com.criteo.deepdiff.KeyExample
import com.criteo.deepdiff.test_utils.SparkUtils.struct
import com.criteo.deepdiff.test_utils.{DatasetDiffsBuilder, DeepDiffSpec}
import org.apache.spark.sql.types._

object IgnoreLeftAndRightOnlySpec {
  final case class KeyUserOnly(key: String = null, user: String = null)
  final case class ValueTotalOnly(value: java.lang.Float = null, total: java.lang.Double = null)
  final case class Simple(key: String, user: String = null, count: java.lang.Integer = null)
  final case class Alternative(
      key: String,
      user: String = null,
      value: java.lang.Float = null,
      total: java.lang.Double = null
  )
  final case class Record(
      key: String,
      user: String = null,
      count: java.lang.Integer = null,
      simple: Simple = null,
      array: Seq[Simple] = null
  )
  final case class PrunedRecord(
      key: String,
      user: String = null,
      simple: KeyUserOnly = null,
      array: Seq[KeyUserOnly] = null
  )
  final case class DifferentRecord(
      key: String,
      user: String = null,
      simple: ValueTotalOnly = null,
      array: Seq[ValueTotalOnly] = null
  )
}

final class IgnoreLeftAndRightOnlySpec extends DeepDiffSpec {
  import DeepDiffSpec._
  import IgnoreLeftAndRightOnlySpec._
  import com.criteo.deepdiff.test_utils.EasyKeyExample._

  // language=HOCON
  private val conf = """keys = ["key"]
                       |ignore {
                       |  left-only = true
                       |  right-only = true
                       |}
                       |""".stripMargin

  "Deep Diff" should {
    "be able to ignore L/R-only fields" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .testReversedLeftAndRight()
        .testPruned(struct("key" -> StringType, "user" -> StringType))
        .conf(conf)
        .compare(
          left = Seq(
            Simple("identical", user = null, count = null),
            Simple("record-only-count", user = null, count = 2),
            Simple("record-only-user", user = "3", count = 3)
          ),
          right = Seq(
            KeyUserOnly("identical"),
            KeyUserOnly("record-only-count"),
            KeyUserOnly("record-only-user")
          )
        )
        .expectRaw(
          DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
            .kindOfDifferent(leftOnly = 1)
            .record(
              identical = 2,
              diffExamples = Seq(
                (
                  Key("record-only-user"),
                  Map("key" -> "record-only-user", "user" -> "3"),
                  Map("key" -> "record-only-user", "user" -> null)
                )
              )
            )
            .common("key", identical = 3)
            .common("user", identical = 2, leftExamples = Seq((Key("record-only-user"), "3"))))
    }

    val simpleLeft = Seq(
      Simple("identical-null"),
      Simple("identical", user = "2"),
      Simple("left-only-count", user = "3", count = 3),
      Simple("right-only-value", user = "4"),
      Simple("right-only-total", user = "5"),
      Simple("all-only", user = "6", count = 6),
      Simple("different", user = "7")
    )
    val alternativeRight = Seq(
      Alternative("identical-null"),
      Alternative("identical", user = "2"),
      Alternative("left-only-count", user = "3"),
      Alternative("right-only-value", user = "4", value = 44f),
      Alternative("right-only-total", user = "5", total = 555d),
      Alternative("all-only", user = "6", value = 66f, total = 666d),
      Alternative("different", user = "7_7")
    )
    "be able to ignore only left-only fields" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .testPruned(
          left = struct("key" -> StringType, "user" -> StringType),
          right = struct("key" -> StringType, "user" -> StringType, "value" -> FloatType, "total" -> DoubleType)
        )
        .conf( // language=HOCON
          """keys = ["key"]
            |ignore.left-only = true
            |""".stripMargin)
        .compare(simpleLeft, alternativeRight)
        .expectRaw(
          DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
            .kindOfDifferent(content = 1, rightOnly = 3)
            .record(
              identical = 3,
              diffExamples = Seq(
                (Key("right-only-value"),
                 Map("key" -> "right-only-value", "user" -> "4"),
                 Map("key" -> "right-only-value", "user" -> "4", "value" -> 44f, "total" -> null)),
                (Key("right-only-total"),
                 Map("key" -> "right-only-total", "user" -> "5"),
                 Map("key" -> "right-only-total", "user" -> "5", "value" -> null, "total" -> 555d)),
                (Key("all-only"),
                 Map("key" -> "all-only", "user" -> "6"),
                 Map("key" -> "all-only", "user" -> "6", "value" -> 66f, "total" -> 666d)),
                (Key("different"),
                 Map("key" -> "different", "user" -> "7"),
                 Map("key" -> "different", "user" -> "7_7", "value" -> null, "total" -> null))
              )
            )
            .common("key", identical = 7)
            .common("user", identical = 6, diffExamples = Seq((Key("different"), "7", "7_7")))
            .rightOnly("value",
                       identical = 5,
                       rightExamples = Seq((Key("all-only"), 66f), (Key("right-only-value"), 44f)))
            .rightOnly("total",
                       identical = 5,
                       rightExamples = Seq((Key("all-only"), 666d), (Key("right-only-total"), 555d)))
        )
    }

    "be able to ignore only right-only fields" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .testPruned(
          left = struct("key" -> StringType, "user" -> StringType, "count" -> IntegerType),
          right = struct("key" -> StringType, "user" -> StringType)
        )
        .conf( // language=HOCON
          """keys = ["key"]
            |ignore.right-only = true
            |""".stripMargin)
        .compare(simpleLeft, alternativeRight)
        .expectRaw(
          DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
            .kindOfDifferent(content = 1, leftOnly = 2)
            .record(
              identical = 4,
              diffExamples = Seq(
                (Key("left-only-count"),
                 Map("key" -> "left-only-count", "user" -> "3", "count" -> 3),
                 Map("key" -> "left-only-count", "user" -> "3")),
                (Key("all-only"),
                 Map("key" -> "all-only", "user" -> "6", "count" -> 6),
                 Map("key" -> "all-only", "user" -> "6")),
                (Key("different"),
                 Map("key" -> "different", "user" -> "7", "count" -> null),
                 Map("key" -> "different", "user" -> "7_7"))
              )
            )
            .common("key", identical = 7)
            .common("user", identical = 6, diffExamples = Seq((Key("different"), "7", "7_7")))
            .leftOnly("count", identical = 5, leftExamples = Seq((Key("left-only-count"), 3), (Key("all-only"), 6)))
        )
    }

    "be able to ignore left-only and right-only fields" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .testPruned(struct("key" -> StringType, "user" -> StringType))
        .testReversedLeftAndRight()
        .conf(conf)
        .compare(simpleLeft, alternativeRight)
        .expectRaw(
          DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
            .kindOfDifferent(content = 1)
            .record(
              identical = 6,
              diffExamples = Seq(
                (Key("different"), Map("key" -> "different", "user" -> "7"), Map("key" -> "different", "user" -> "7_7"))
              )
            )
            .common("key", identical = 7)
            .common("user", identical = 6, diffExamples = Seq((Key("different"), "7", "7_7"))))
    }

    "be able to ignore L/R-only field in a nested struct" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .testReversedLeftAndRight()
        .testPruned(struct(
          "key" -> StringType,
          "user" -> StringType,
          "simple" -> struct("key" -> StringType, "user" -> StringType),
          "array" -> ArrayType(struct("key" -> StringType, "user" -> StringType))
        ))
        .conf(conf)
        .compare(
          left = Seq(
            Record("identical-null"),
            Record("identical", simple = Simple("b", user = null, count = null)),
            Record("record-only-count", simple = Simple("c", user = null, count = 3)),
            Record("record-only-user", simple = Simple("d", user = "4", count = null))
          ),
          right = Seq(
            PrunedRecord("identical-null"),
            PrunedRecord("identical", simple = KeyUserOnly("b")),
            PrunedRecord("record-only-count", simple = KeyUserOnly("c")),
            PrunedRecord("record-only-user", simple = KeyUserOnly("d"))
          )
        )
        .expectRaw(
          DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
            .kindOfDifferent(leftOnly = 1)
            .record(
              identical = 3,
              diffExamples = Seq(
                (
                  Key("record-only-user"),
                  Map(
                    "key" -> "record-only-user",
                    "user" -> null,
                    "simple" -> Map("key" -> "d", "user" -> "4"),
                    "array" -> null
                  ),
                  Map(
                    "key" -> "record-only-user",
                    "user" -> null,
                    "simple" -> Map("key" -> "d", "user" -> null),
                    "array" -> null
                  )
                )
              )
            )
            .common("key", identical = 4)
            .common("user", identical = 4)
            .common(
              "simple",
              identical = 3,
              diffExamples =
                Seq((Key("record-only-user"), Map("key" -> "d", "user" -> "4"), Map("key" -> "d", "user" -> null)))
            )
            .common("simple.key", identical = 3)
            .common("simple.user", identical = 2, leftExamples = Seq((Key("record-only-user"), "4")))
            .common("array", identical = 4)
            .common("array (struct)")
            .common("array.key")
            .common("array.user"))
    }

    "be able to ignore L/R-only field in a struct array" which {
      val testCase = DeepDiffTestCase
        .testReversedLeftAndRight()
        .testPruned(struct(
          "key" -> StringType,
          "user" -> StringType,
          "simple" -> struct("key" -> StringType, "user" -> StringType),
          "array" -> ArrayType(struct("key" -> StringType, "user" -> StringType))
        ))
        .compare(
          left = Seq(
            Record("identical-null"),
            Record("identical", array = Seq(Simple("b", user = null, count = null))),
            Record("record-only-count", array = Seq(Simple("c", user = null, count = 3))),
            Record("record-only-user", array = Seq(Simple("diff", user = "4", count = null)))
          ),
          right = Seq(
            PrunedRecord("identical-null"),
            PrunedRecord("identical", array = Seq(KeyUserOnly("b"))),
            PrunedRecord("record-only-count", array = Seq(KeyUserOnly("c"))),
            PrunedRecord("record-only-user", array = Seq(KeyUserOnly("diff")))
          )
        )
      val builder = DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
        .kindOfDifferent(leftOnly = 1)
        .record(
          identical = 3,
          diffExamples = Seq(
            (
              Key("record-only-user"),
              Map(
                "key" -> "record-only-user",
                "user" -> null,
                "simple" -> null,
                "array" -> Seq(Map("key" -> "diff", "user" -> "4"))
              ),
              Map(
                "key" -> "record-only-user",
                "user" -> null,
                "simple" -> null,
                "array" -> Seq(Map("key" -> "diff", "user" -> null))
              )
            )
          )
        )
        .common("key", identical = 4)
        .common("user", identical = 4)
        .common(
          "array",
          identical = 3,
          diffExamples = Seq(
            (Key("record-only-user"),
             Seq(Map("key" -> "diff", "user" -> "4")),
             Seq(Map("key" -> "diff", "user" -> null)))
          )
        )
        .common("simple", identical = 4)
        .common("simple.key")
        .common("simple.user")

      "is NOT exploded" when runningDeepDiffTestCase {
        testCase
          .conf(conf)
          .expectRaw(
            builder
              .common(
                "array (struct)",
                identical = 2,
                diffExamples = Seq(
                  (
                    Key("record-only-user") >> Key(0),
                    Map("key" -> "diff", "user" -> "4"),
                    Map("key" -> "diff", "user" -> null)
                  )
                )
              )
              .common("array.key", identical = 3)
              .common("array.user", identical = 2, leftExamples = Seq((Key("record-only-user") >> Key(0), "4"))))
      }
      "is exploded" when runningDeepDiffTestCase {
        testCase
          .conf( // language=HOCON
            """keys = ["key"]
                |ignore {
                |  left-only = true
                |  right-only = true
                |}
                |exploded-arrays {
                |  array = { keys = ["key"] }
                |}
                |""".stripMargin)
          .expectRaw(
            builder
              .explodedArray(
                "array",
                DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
                  .record(
                    identical = 2,
                    diffExamples = Seq(
                      (
                        Key("record-only-user") >> Key("diff"),
                        Map("key" -> "diff", "user" -> "4"),
                        Map("key" -> "diff", "user" -> null)
                      )
                    )
                  )
                  .kindOfDifferent(leftOnly = 1)
                  .common("key", identical = 3)
                  .common("user", identical = 2, leftExamples = Seq((Key("record-only-user") >> Key("diff"), "4")))
              ))
      }
    }

    "be able to ignore the whole nested struct and struct arrays if there are only L/R-only fields" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .testPruned(struct("key" -> StringType, "user" -> StringType))
        .conf(conf)
        .compare(
          left = Seq(
            PrunedRecord("identical-null"),
            PrunedRecord("identical", user = "2"),
            PrunedRecord("struct", user = "3", simple = KeyUserOnly(key = "33", user = "333")),
            PrunedRecord("left-struct-null", user = "4", simple = KeyUserOnly()),
            PrunedRecord("right-struct-null", user = "5"),
            PrunedRecord("left-struct-only", user = "6", simple = KeyUserOnly(key = "66", user = "666")),
            PrunedRecord("right-struct-only", user = "7"),
            PrunedRecord("left-array-empty", user = "8", array = Seq.empty),
            PrunedRecord("right-array-empty", user = "9"),
            PrunedRecord("array", user = "10", array = Seq(KeyUserOnly(key = "10", user = "1010"))),
            PrunedRecord("left-array-only", user = "11", array = Seq(KeyUserOnly(key = "11", user = "1111"))),
            PrunedRecord("right-array-only", user = "12"),
            PrunedRecord("different", user = "13")
          ),
          right = Seq(
            DifferentRecord("identical-null"),
            DifferentRecord("identical", user = "2"),
            DifferentRecord("struct", user = "3", simple = ValueTotalOnly(value = 3f, total = 33d)),
            DifferentRecord("left-struct-null", user = "4"),
            DifferentRecord("right-struct-null", user = "5", simple = ValueTotalOnly()),
            DifferentRecord("left-struct-only", user = "6"),
            DifferentRecord("right-struct-only", user = "7", simple = ValueTotalOnly(value = 7f, total = 77d)),
            DifferentRecord("left-array-empty", user = "8"),
            DifferentRecord("right-array-empty", user = "9", array = Seq.empty),
            DifferentRecord("array", user = "10", array = Seq(ValueTotalOnly(value = 10f, total = 1010d))),
            DifferentRecord("left-array-only", user = "11"),
            DifferentRecord("right-array-only", user = "12", array = Seq(ValueTotalOnly(value = 12f, total = 1212d))),
            DifferentRecord("different", user = "2345")
          )
        )
        .expectRaw(
          DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
            .kindOfDifferent(content = 1)
            .record(
              identical = 12,
              diffExamples = Seq(
                (
                  Key("different"),
                  Map("key" -> "different", "user" -> "13"),
                  Map("key" -> "different", "user" -> "2345")
                )
              )
            )
            .common("key", identical = 13)
            .common("user", identical = 12, diffExamples = Seq((Key("different"), "13", "2345")))
        )
    }

    "raise an exception when there is no common field" in {
      a[Exception] should be thrownBy runningDeepDiffTestCase {
        DeepDiffTestCase
          .testPruned()
          .conf(conf)
          .compare(
            left = Seq(KeyUserOnly()),
            right = Seq(ValueTotalOnly())
          )
          .expectRaw(DatasetDiffsBuilder[KeyExample, Map[String, Any]]())
      }
    }
  }
}
