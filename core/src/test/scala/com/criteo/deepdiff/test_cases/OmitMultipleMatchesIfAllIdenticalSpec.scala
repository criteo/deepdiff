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

object OmitMultipleMatchesIfAllIdenticalSpec {
  final case class Record(key: String, user: String = null, count: java.lang.Integer = null)
  final case class BigRecord(
      key: String,
      user: String = null,
      count: java.lang.Integer = null,
      value: java.lang.Float = null
  )
  final case class AlternativeRecord(
      key: String,
      user: String = null,
      count: java.lang.Integer = null,
      total: java.lang.Double = null
  )
  final case class ApproxRecord(key: String, total: java.lang.Double)
}

final class OmitMultipleMatchesIfAllIdenticalSpec extends DeepDiffSpec {
  import DeepDiffSpec._
  import OmitMultipleMatchesIfAllIdenticalSpec._
  import com.criteo.deepdiff.test_utils.EasyKeyExample._

  "Deep Diff" when {
    "specified to ignore multiple matches" should {
      "treat multiple identical records as a single one" when runningDeepDiffTestCase {
        DeepDiffTestCase
          .conf( // language=HOCON
            """keys = ["key"]
              |multiple-matches.omit-if-all-identical = true
              |""".stripMargin)
          .compare(
            left = Seq(
              Record("identical-null"),
              Record("identical-multiple", count = 1),
              Record("identical-multiple", count = 1),
              Record("identical-left-multiple", count = 2),
              Record("identical-left-multiple", count = 2),
              Record("identical-right-multiple", count = 3),
              Record("different-multiple", count = 4),
              Record("different-multiple", count = 4),
              Record("different-left-multiple", count = 5),
              Record("different-left-multiple", count = 5),
              Record("different-right-multiple", count = 6),
              Record("left-only-multiple", count = 7),
              Record("left-only-multiple", count = 7),
              Record("left-not-multiple", count = 9),
              Record("left-not-multiple", count = 99),
              Record("right-not-multiple", count = 10),
              Record("left-only-not-multiple", count = 11),
              Record("left-only-not-multiple", count = 1111)
            ),
            right = Seq(
              BigRecord("identical-null"),
              BigRecord("identical-multiple", count = 1),
              BigRecord("identical-multiple", count = 1),
              BigRecord("identical-left-multiple", count = 2),
              BigRecord("identical-right-multiple", count = 3),
              BigRecord("identical-right-multiple", count = 3),
              BigRecord("different-multiple", count = 40),
              BigRecord("different-multiple", count = 40),
              BigRecord("different-left-multiple", count = 50),
              BigRecord("different-right-multiple", count = 60),
              BigRecord("different-right-multiple", count = 60),
              BigRecord("right-only-multiple", count = 8, value = 8f),
              BigRecord("right-only-multiple", count = 8, value = 8f),
              BigRecord("left-not-multiple", count = 9),
              BigRecord("right-not-multiple", count = 10),
              BigRecord("right-not-multiple", count = 1010),
              BigRecord("right-only-not-multiple", count = 12),
              BigRecord("right-only-not-multiple", count = 1212),
              BigRecord("right-only-not-multiple-2", count = 13, value = 13f),
              BigRecord("right-only-not-multiple-2", count = 13, value = 1313f)
            )
          )
          .expect(
            DatasetDiffsBuilder[KeyExample, Product]()
              .multipleMatches(
                examples = Seq(
                  (
                    Key("left-not-multiple"),
                    Seq(Record("left-not-multiple", count = 9), Record("left-not-multiple", count = 99)),
                    Seq(BigRecord("left-not-multiple", count = 9))
                  ),
                  (
                    Key("right-not-multiple"),
                    Seq(Record("right-not-multiple", count = 10)),
                    Seq(BigRecord("right-not-multiple", count = 10), BigRecord("right-not-multiple", count = 1010))
                  ),
                  (
                    Key("left-only-not-multiple"),
                    Seq(Record("left-only-not-multiple", count = 11), Record("left-only-not-multiple", count = 1111)),
                    Seq.empty
                  ),
                  (
                    Key("right-only-not-multiple"),
                    Seq.empty,
                    Seq(
                      BigRecord("right-only-not-multiple", count = 12),
                      BigRecord("right-only-not-multiple", count = 1212)
                    )
                  ),
                  (
                    Key("right-only-not-multiple-2"),
                    Seq.empty,
                    Seq(
                      BigRecord("right-only-not-multiple-2", count = 13, value = 13f),
                      BigRecord("right-only-not-multiple-2", count = 13, value = 1313f)
                    )
                  )
                )
              )
              .kindOfDifferent(content = 3)
              .record(
                identical = 4,
                diffExamples = Seq(
                  (
                    Key("different-multiple"),
                    Record("different-multiple", count = 4),
                    BigRecord("different-multiple", count = 40)
                  ),
                  (
                    Key("different-left-multiple"),
                    Record("different-left-multiple", count = 5),
                    BigRecord("different-left-multiple", count = 50)
                  ),
                  (
                    Key("different-right-multiple"),
                    Record("different-right-multiple", count = 6),
                    BigRecord("different-right-multiple", count = 60)
                  )
                ),
                leftExamples = Seq((Key("left-only-multiple"), Record("left-only-multiple", count = 7))),
                rightExamples =
                  Seq((Key("right-only-multiple"), BigRecord("right-only-multiple", count = 8, value = 8f)))
              )
              .common("key", identical = 7)
              .common("user", identical = 7)
              .common(
                "count",
                identical = 4,
                diffExamples = Seq(
                  (Key("different-multiple"), 4, 40),
                  (Key("different-left-multiple"), 5, 50),
                  (Key("different-right-multiple"), 6, 60)
                )
              )
              .rightOnly("value", identical = 7)
          )
      }

      "not take into account tolerances" when runningDeepDiffTestCase {
        DeepDiffTestCase
          .conf( // language=HOCON
            """keys = ["key"]
              |multiple-matches.omit-if-all-identical = true
              |tolerances {
              |  total = { absolute = 10, relative = .1, satisfies = "any" }
              |}
              |""".stripMargin)
          .testReversedLeftAndRight()
          .compare(
            left = Seq(
              ApproxRecord("multiple", total = 2d),
              ApproxRecord("multiple", total = 2d),
              ApproxRecord("multiple", total = 2d),
              ApproxRecord("multiple-absolute-and-relative", total = 1.99),
              ApproxRecord("multiple-absolute-and-relative", total = 2d),
              ApproxRecord("multiple-absolute-and-relative", total = 2.01),
              ApproxRecord("multiple-absolute", total = 1d),
              ApproxRecord("multiple-absolute", total = 2d),
              ApproxRecord("multiple-absolute", total = 3d),
              ApproxRecord("multiple-relative", total = 980d),
              ApproxRecord("multiple-relative", total = 1000d),
              ApproxRecord("multiple-relative", total = 1010d)
            ),
            right = Seq(
              ApproxRecord("multiple", total = 2d),
              ApproxRecord("multiple-absolute-and-relative", total = 2d)
            )
          )
          .expect(
            DatasetDiffsBuilder[KeyExample, Product]()
              .multipleMatches(
                count = 3,
                examples = Seq(
                  (Key("multiple-absolute-and-relative"),
                   Seq(
                     ApproxRecord("multiple-absolute-and-relative", total = 1.99),
                     ApproxRecord("multiple-absolute-and-relative", total = 2d),
                     ApproxRecord("multiple-absolute-and-relative", total = 2.01)
                   ),
                   Seq(ApproxRecord("multiple-absolute-and-relative", total = 2d))),
                  (Key("multiple-absolute"),
                   Seq(ApproxRecord("multiple-absolute", total = 1d),
                       ApproxRecord("multiple-absolute", total = 2d),
                       ApproxRecord("multiple-absolute", total = 3d)),
                   Nil),
                  (Key("multiple-relative"),
                   Seq(ApproxRecord("multiple-relative", total = 980d),
                       ApproxRecord("multiple-relative", total = 1000d),
                       ApproxRecord("multiple-relative", total = 1010d)),
                   Nil)
                )
              )
              .record(identical = 1)
              .common("key", identical = 1)
              .common("total", identical = 1)
          )
      }

      "be able to ignore fields during comparison" when runningDeepDiffTestCase {
        DeepDiffTestCase
          .testReversedLeftAndRight()
          .testPruned(
            struct(
              "key" -> StringType,
              "count" -> IntegerType
            ))
          .conf( // language=HOCON
            """keys = ["key"]
              |multiple-matches.omit-if-all-identical = true
              |ignore.fields =["user", "value"]
              |""".stripMargin)
          .compare(
            left = Seq(
              Record("identical-null"),
              Record("identical", count = 2),
              Record("identical-multiple", count = 3),
              Record("identical-multiple", user = "3", count = 3),
              Record("identical-multiple", user = "33", count = 3),
              Record("identical-left-multiple", count = 4),
              Record("identical-left-multiple", user = "4", count = 4),
              Record("identical-left-multiple", user = "44", count = 4),
              Record("identical-right-multiple", count = 5),
              Record("different-multiple", count = 6),
              Record("different-multiple", user = "6", count = 6),
              Record("different-multiple", user = "66", count = 6),
              Record("left-not-multiple", count = 7),
              Record("left-not-multiple", user = "7", count = 77)
            ),
            right = Seq(
              BigRecord("identical-null"),
              BigRecord("identical", count = 2),
              BigRecord("identical-multiple", count = 3),
              BigRecord("identical-multiple", count = 3, value = 3f),
              BigRecord("identical-multiple", count = 3, value = 33f),
              BigRecord("identical-multiple", user = "3", count = 3, value = 3f),
              BigRecord("identical-multiple", user = "3", count = 3),
              BigRecord("identical-multiple", user = "33", count = 3),
              BigRecord("identical-left-multiple", count = 4),
              BigRecord("identical-right-multiple", count = 5),
              BigRecord("identical-right-multiple", count = 5, value = 5f),
              BigRecord("identical-right-multiple", count = 5, value = 55f),
              BigRecord("identical-right-multiple", user = "5", count = 5, value = 5f),
              BigRecord("identical-right-multiple", user = "5", count = 5),
              BigRecord("identical-right-multiple", user = "55", count = 5),
              BigRecord("different-multiple", count = 60),
              BigRecord("different-multiple", count = 60, value = 6f),
              BigRecord("different-multiple", count = 60, value = 66f),
              BigRecord("different-multiple", user = "6", count = 60, value = 6f),
              BigRecord("different-multiple", user = "6", count = 60),
              BigRecord("different-multiple", user = "66", count = 60),
              BigRecord("left-not-multiple", count = 7),
              BigRecord("right-not-multiple", count = 7),
              BigRecord("right-not-multiple", user = "7", count = 77, value = 7f)
            )
          )
          .expectRaw(
            DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
              .multipleMatches(
                examples = Seq(
                  (
                    Key("left-not-multiple"),
                    Seq(Map("key" -> "left-not-multiple", "count" -> 7),
                        Map("key" -> "left-not-multiple", "count" -> 77)),
                    Seq(Map("key" -> "left-not-multiple", "count" -> 7))
                  ),
                  (
                    Key("right-not-multiple"),
                    Seq.empty,
                    Seq(Map("key" -> "right-not-multiple", "count" -> 7),
                        Map("key" -> "right-not-multiple", "count" -> 77))
                  )
                )
              )
              .kindOfDifferent(content = 1)
              .record(
                identical = 5,
                diffExamples = Seq(
                  (
                    Key("different-multiple"),
                    Map("key" -> "different-multiple", "count" -> 6),
                    Map("key" -> "different-multiple", "count" -> 60)
                  )
                )
              )
              .common("key", identical = 6)
              .common(
                "count",
                identical = 5,
                diffExamples = Seq(
                  (Key("different-multiple"), 6, 60)
                )
              ))
      }

      "be able to ignore left and right only fields during comparison" when runningDeepDiffTestCase {
        DeepDiffTestCase
          .testPruned(struct("key" -> StringType, "user" -> StringType, "count" -> IntegerType))
          .conf( // language=HOCON
            """keys = ["key"]
              |multiple-matches.omit-if-all-identical = true
              |ignore {
              |  left-only = true
              |  right-only = true
              |}
              |""".stripMargin)
          .compare(
            left = Seq(
              AlternativeRecord("identical-null"),
              AlternativeRecord("identical", count = 2),
              AlternativeRecord("identical-multiple", count = 3),
              AlternativeRecord("identical-multiple", count = 3, total = 3d),
              AlternativeRecord("identical-multiple", count = 3, total = 33d),
              AlternativeRecord("identical-left-multiple", count = 4),
              AlternativeRecord("identical-left-multiple", count = 4, total = 4d),
              AlternativeRecord("identical-left-multiple", count = 4, total = 44d),
              AlternativeRecord("identical-right-multiple", count = 5),
              AlternativeRecord("different-multiple", count = 6),
              AlternativeRecord("different-multiple", count = 6, total = 6d),
              AlternativeRecord("different-multiple", count = 6, total = 66d),
              AlternativeRecord("left-not-multiple", count = 7),
              AlternativeRecord("left-not-multiple", count = 77, total = 77d)
            ),
            right = Seq(
              BigRecord("identical-null"),
              BigRecord("identical", count = 2),
              BigRecord("identical-multiple", count = 3),
              BigRecord("identical-multiple", count = 3, value = 3f),
              BigRecord("identical-multiple", count = 3, value = 33f),
              BigRecord("identical-left-multiple", count = 4),
              BigRecord("identical-right-multiple", count = 5),
              BigRecord("identical-right-multiple", count = 5, value = 5f),
              BigRecord("identical-right-multiple", count = 5, value = 55f),
              BigRecord("different-multiple", count = 60),
              BigRecord("different-multiple", count = 60, value = 6f),
              BigRecord("different-multiple", count = 60, value = 66f),
              BigRecord("left-not-multiple", count = 7),
              BigRecord("right-not-multiple", count = 7),
              BigRecord("right-not-multiple", count = 77, value = 7f)
            )
          )
          .expectRaw(
            DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
              .multipleMatches(
                examples = Seq(
                  (
                    Key("left-not-multiple"),
                    Seq(
                      Map("key" -> "left-not-multiple", "user" -> null, "count" -> 7),
                      Map("key" -> "left-not-multiple", "user" -> null, "count" -> 77)
                    ),
                    Seq(Map("key" -> "left-not-multiple", "user" -> null, "count" -> 7))
                  ),
                  (
                    Key("right-not-multiple"),
                    Seq.empty,
                    Seq(
                      Map("key" -> "right-not-multiple", "user" -> null, "count" -> 7),
                      Map("key" -> "right-not-multiple", "user" -> null, "count" -> 77)
                    )
                  )
                )
              )
              .kindOfDifferent(content = 1)
              .record(
                identical = 5,
                diffExamples = Seq(
                  (
                    Key("different-multiple"),
                    Map("key" -> "different-multiple", "user" -> null, "count" -> 6),
                    Map("key" -> "different-multiple", "user" -> null, "count" -> 60)
                  )
                )
              )
              .common("key", identical = 6)
              .common("user", identical = 6)
              .common(
                "count",
                identical = 5,
                diffExamples = Seq(
                  (Key("different-multiple"), 6, 60)
                )
              )
          )
      }

      "be able to ignore left/right only fields and common fields during comparison" when runningDeepDiffTestCase {
        DeepDiffTestCase
          .testPruned(struct("key" -> StringType, "count" -> IntegerType))
          .conf( // language=HOCON
            """keys = ["key"]
              |multiple-matches.omit-if-all-identical = true
              |ignore {
              |  fields = ["user"]
              |  left-only = true
              |  right-only = true
              |}
              |""".stripMargin)
          .compare(
            left = Seq(
              AlternativeRecord("identical-null"),
              AlternativeRecord("identical", count = 2),
              AlternativeRecord("identical-multiple", count = 3),
              AlternativeRecord("identical-multiple", count = 3, total = 3d),
              AlternativeRecord("identical-multiple", count = 3, total = 33d),
              AlternativeRecord("identical-multiple", user = "3", count = 3, total = 3d),
              AlternativeRecord("identical-multiple", user = "3", count = 3),
              AlternativeRecord("identical-multiple", user = "33", count = 3),
              AlternativeRecord("identical-left-multiple", count = 4),
              AlternativeRecord("identical-left-multiple", count = 4, total = 4d),
              AlternativeRecord("identical-left-multiple", count = 4, total = 44d),
              AlternativeRecord("identical-left-multiple", user = "4", count = 4, total = 4d),
              AlternativeRecord("identical-left-multiple", user = "4", count = 4),
              AlternativeRecord("identical-left-multiple", user = "44", count = 4),
              AlternativeRecord("identical-right-multiple", count = 5),
              AlternativeRecord("different-multiple", count = 6),
              AlternativeRecord("different-multiple", count = 6, total = 6d),
              AlternativeRecord("different-multiple", count = 6, total = 66d),
              AlternativeRecord("different-multiple", user = "6", count = 6, total = 6d),
              AlternativeRecord("different-multiple", user = "6", count = 6),
              AlternativeRecord("different-multiple", user = "66", count = 6),
              AlternativeRecord("left-not-multiple", count = 7),
              AlternativeRecord("left-not-multiple", user = "77", count = 77, total = 77d)
            ),
            right = Seq(
              BigRecord("identical-null"),
              BigRecord("identical", count = 2),
              BigRecord("identical-multiple", count = 3),
              BigRecord("identical-multiple", count = 3, value = 3f),
              BigRecord("identical-multiple", count = 3, value = 33f),
              BigRecord("identical-multiple", user = "3", count = 3, value = 3f),
              BigRecord("identical-multiple", user = "3", count = 3),
              BigRecord("identical-multiple", user = "33", count = 3),
              BigRecord("identical-left-multiple", count = 4),
              BigRecord("identical-right-multiple", count = 5),
              BigRecord("identical-right-multiple", count = 5, value = 5f),
              BigRecord("identical-right-multiple", count = 5, value = 55f),
              BigRecord("identical-right-multiple", user = "5", count = 5, value = 5f),
              BigRecord("identical-right-multiple", user = "5", count = 5),
              BigRecord("identical-right-multiple", user = "55", count = 5),
              BigRecord("different-multiple", count = 60),
              BigRecord("different-multiple", count = 60, value = 6f),
              BigRecord("different-multiple", count = 60, value = 66f),
              BigRecord("different-multiple", user = "6", count = 60, value = 6f),
              BigRecord("different-multiple", user = "6", count = 60),
              BigRecord("different-multiple", user = "66", count = 60),
              BigRecord("left-not-multiple", count = 7),
              BigRecord("right-not-multiple", count = 7),
              BigRecord("right-not-multiple", user = "7", count = 77, value = 7f)
            )
          )
          .expectRaw(
            DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
              .multipleMatches(
                examples = Seq(
                  (
                    Key("left-not-multiple"),
                    Seq(
                      Map("key" -> "left-not-multiple", "count" -> 7),
                      Map("key" -> "left-not-multiple", "count" -> 77)
                    ),
                    Seq(Map("key" -> "left-not-multiple", "count" -> 7))
                  ),
                  (
                    Key("right-not-multiple"),
                    Seq.empty,
                    Seq(
                      Map("key" -> "right-not-multiple", "count" -> 7),
                      Map("key" -> "right-not-multiple", "count" -> 77)
                    )
                  )
                )
              )
              .kindOfDifferent(content = 1)
              .record(
                identical = 5,
                diffExamples = Seq(
                  (
                    Key("different-multiple"),
                    Map("key" -> "different-multiple", "count" -> 6),
                    Map("key" -> "different-multiple", "count" -> 60)
                  )
                )
              )
              .common("key", identical = 6)
              .common(
                "count",
                identical = 5,
                diffExamples = Seq(
                  (Key("different-multiple"), 6, 60)
                )
              )
          )
      }
    }
  }
}
