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

object IgnoreSpec {
  final case class KeyOnly(key: String)
  final case class Simple(key: String, user: String = null, count: java.lang.Integer = null)
  final case class Record(
      key: String,
      user: String = null,
      count: java.lang.Integer = null,
      simple: Simple = null,
      array: Seq[Simple] = null
  )
  final case class PrunedRecord(
      key: String,
      simple: KeyOnly = null,
      array: Seq[KeyOnly] = null
  )
}

final class IgnoreSpec extends DeepDiffSpec {
  import DeepDiffSpec._
  import IgnoreSpec._
  import com.criteo.deepdiff.test_utils.EasyKeyExample._

  "Deep Diff" should {
    "be able to ignore a field" which {
      // language=HOCON
      val ignoreCountConf = """keys = ["key"]
                              |ignore.fields = ["count"]
                              |""".stripMargin
      "is common" when runningDeepDiffTestCase {
        DeepDiffTestCase
          .testPruned(
            struct(
              "key" -> StringType,
              "user" -> StringType
            ))
          .conf(ignoreCountConf)
          .compare(
            left = Seq(
              Simple("identical", user = null, count = 1),
              Simple("different-count", user = "2", count = 2),
              Simple("left-only-count", user = "3", count = 3),
              Simple("right-only-count", user = "4", count = null),
              Simple("null-count", user = "5", count = null),
              Simple("different-user", user = "6", count = null)
            ),
            right = Seq(
              Simple("identical", user = null, count = 1),
              Simple("different-count", user = "2", count = 2),
              Simple("left-only-count", user = "3", count = null),
              Simple("right-only-count", user = "4", count = 4),
              Simple("null-count", user = "5", count = null),
              Simple("different-user", user = "66", count = null)
            )
          )
          .expectRaw(
            DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
              .kindOfDifferent(content = 1)
              .record(
                identical = 5,
                diffExamples = Seq(
                  (
                    Key("different-user"),
                    Map("key" -> "different-user", "user" -> "6"),
                    Map("key" -> "different-user", "user" -> "66")
                  )
                )
              )
              .common("key", identical = 6)
              .common("user", identical = 5, diffExamples = Seq((Key("different-user"), "6", "66")))
          )
      }
      "is left/right only" when runningDeepDiffTestCase {
        DeepDiffTestCase
          .testReversedLeftAndRight()
          .testPruned(left = struct(
                        "key" -> StringType,
                        "user" -> StringType
                      ),
                      right = struct(
                        "key" -> StringType
                      ))
          .conf(ignoreCountConf)
          .compare(
            left = Seq(
              Simple("identical", user = null, count = null),
              Simple("record-only-count", user = null, count = 2),
              Simple("record-only-user", user = "3", count = 3)
            ),
            right = Seq(
              KeyOnly("identical"),
              KeyOnly("record-only-count"),
              KeyOnly("record-only-user")
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
                    Map("key" -> "record-only-user")
                  )
                )
              )
              .common("key", identical = 3)
              .leftOnly("user", identical = 2, leftExamples = Seq((Key("record-only-user"), "3")))
          )
      }
    }

    "be able to ignore a nested field" which {
      // language=HOCON
      val ignoreSimpleCountConf = """keys = ["key"]
                                    |ignore.fields =["simple.count"]
                                    |""".stripMargin
      "is common" when runningDeepDiffTestCase {
        DeepDiffTestCase
          .testPruned(struct(
            "key" -> StringType,
            "user" -> StringType,
            "count" -> IntegerType,
            "simple" -> struct(
              "key" -> StringType,
              "user" -> StringType
            ),
            "array" -> ArrayType(
              struct(
                "key" -> StringType,
                "user" -> StringType,
                "count" -> IntegerType
              ))
          ))
          .conf(ignoreSimpleCountConf)
          .compare(
            left = Seq(
              Record("identical-null"),
              Record("identical", simple = Simple("b", user = "2", count = 2)),
              Record("different-count", simple = Simple("c", user = "3", count = 3)),
              Record("left-only-count", simple = Simple("d", user = "4", count = 4)),
              Record("right-only-count", simple = Simple("e", user = "5", count = null)),
              Record("null-count", simple = Simple("f", user = "6", count = null)),
              Record("different-user", simple = Simple("g", user = "7", count = null))
            ),
            right = Seq(
              Record("identical-null"),
              Record("identical", simple = Simple("b", user = "2", count = 2)),
              Record("different-count", simple = Simple("c", user = "3", count = 33)),
              Record("left-only-count", simple = Simple("d", user = "4", count = null)),
              Record("right-only-count", simple = Simple("e", user = "5", count = 5)),
              Record("null-count", simple = Simple("f", user = "6", count = null)),
              Record("different-user", simple = Simple("g", user = "77", count = null))
            )
          )
          .expectRaw(
            DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
              .kindOfDifferent(content = 1)
              .record(
                identical = 6,
                diffExamples = Seq(
                  (
                    Key("different-user"),
                    Map(
                      "key" -> "different-user",
                      "user" -> null,
                      "count" -> null,
                      "simple" -> Map("key" -> "g", "user" -> "7"),
                      "array" -> null
                    ),
                    Map(
                      "key" -> "different-user",
                      "user" -> null,
                      "count" -> null,
                      "simple" -> Map("key" -> "g", "user" -> "77"),
                      "array" -> null
                    )
                  )
                )
              )
              .common("key", identical = 7)
              .common("user", identical = 7)
              .common("count", identical = 7)
              .common(
                "simple",
                identical = 6,
                diffExamples =
                  Seq((Key("different-user"), Map("key" -> "g", "user" -> "7"), Map("key" -> "g", "user" -> "77")))
              )
              .common("simple.key", identical = 6)
              .common("simple.user", identical = 5, diffExamples = Seq((Key("different-user"), "7", "77")))
              .common("array", identical = 7)
              .common("array (struct)")
              .common("array.key")
              .common("array.user")
              .common("array.count")
          )
      }

      "is left/right only" when runningDeepDiffTestCase {
        DeepDiffTestCase
          .testReversedLeftAndRight()
          .testPruned(
            left = struct(
              "key" -> StringType,
              "user" -> StringType,
              "count" -> IntegerType,
              "simple" -> struct(
                "key" -> StringType,
                "user" -> StringType
              ),
              "array" -> ArrayType(
                struct(
                  "key" -> StringType,
                  "user" -> StringType,
                  "count" -> IntegerType
                ))
            ),
            right = struct(
              "key" -> StringType,
              "simple" -> struct(
                "key" -> StringType
              ),
              "array" -> ArrayType(struct(
                "key" -> StringType
              ))
            )
          )
          .conf(ignoreSimpleCountConf)
          .compare(
            left = Seq(
              Record("identical-null"),
              Record("identical", simple = Simple("b", user = null, count = null)),
              Record("record-only-count", simple = Simple("c", user = null, count = 3)),
              Record("record-only-user", simple = Simple("d", user = "4", count = null))
            ),
            right = Seq(
              PrunedRecord("identical-null"),
              PrunedRecord("identical", simple = KeyOnly("b")),
              PrunedRecord("record-only-count", simple = KeyOnly("c")),
              PrunedRecord("record-only-user", simple = KeyOnly("d"))
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
                      "count" -> null,
                      "simple" -> Map("key" -> "d", "user" -> "4"),
                      "array" -> null
                    ),
                    Map("key" -> "record-only-user", "simple" -> Map("key" -> "d"), "array" -> null)
                  )
                )
              )
              .common("key", identical = 4)
              .leftOnly("user", identical = 4)
              .leftOnly("count", identical = 4)
              .common(
                "simple",
                identical = 3,
                diffExamples = Seq((Key("record-only-user"), Map("key" -> "d", "user" -> "4"), Map("key" -> "d")))
              )
              .common("simple.key", identical = 3)
              .leftOnly("simple.user", identical = 2, leftExamples = Seq((Key("record-only-user"), "4")))
              .common("array", identical = 4)
              .common("array (struct)")
              .common("array.key")
              .leftOnly("array.user")
              .leftOnly("array.count")
          )
      }
    }

    "be able to ignore a field in a struct array" which {
      "is common" when {
        val testCase = DeepDiffTestCase
          .testPruned()
          .testPruned(struct(
            "key" -> StringType,
            "user" -> StringType,
            "count" -> IntegerType,
            "simple" -> struct(
              "key" -> StringType,
              "user" -> StringType,
              "count" -> IntegerType
            ),
            "array" -> ArrayType(struct(
              "key" -> StringType,
              "user" -> StringType
            ))
          ))
          .compare(
            left = Seq(
              Record("identical-null"),
              Record("identical", array = Seq(Simple("b", user = "2", count = 2))),
              Record("different-count", array = Seq(Simple("c", user = "3", count = 3))),
              Record("left-only-count", array = Seq(Simple("d", user = "4", count = 4))),
              Record("right-only-count", array = Seq(Simple("e", user = "5", count = null))),
              Record("null-count", array = Seq(Simple("f", user = "6", count = null))),
              Record("different-user", array = Seq(Simple("diff", user = "7", count = null)))
            ),
            right = Seq(
              Record("identical-null"),
              Record("identical", array = Seq(Simple("b", user = "2", count = 2))),
              Record("different-count", array = Seq(Simple("c", user = "3", count = 33))),
              Record("left-only-count", array = Seq(Simple("d", user = "4", count = null))),
              Record("right-only-count", array = Seq(Simple("e", user = "5", count = 5))),
              Record("null-count", array = Seq(Simple("f", user = "6", count = null))),
              Record("different-user", array = Seq(Simple("diff", user = "77", count = null)))
            )
          )
        val builder = DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
          .kindOfDifferent(content = 1)
          .record(
            identical = 6,
            diffExamples = Seq(
              (
                Key("different-user"),
                Map(
                  "key" -> "different-user",
                  "user" -> null,
                  "count" -> null,
                  "simple" -> null,
                  "array" -> Seq(Map("key" -> "diff", "user" -> "7"))
                ),
                Map(
                  "key" -> "different-user",
                  "user" -> null,
                  "count" -> null,
                  "simple" -> null,
                  "array" -> Seq(Map("key" -> "diff", "user" -> "77"))
                )
              )
            )
          )
          .common("key", identical = 7)
          .common("user", identical = 7)
          .common("count", identical = 7)
          .common(
            "array",
            identical = 6,
            diffExamples = Seq(
              (
                Key("different-user"),
                Seq(Map("key" -> "diff", "user" -> "7")),
                Seq(Map("key" -> "diff", "user" -> "77"))
              )
            )
          )
          .common("simple", identical = 7)
          .common("simple.key")
          .common("simple.user")
          .common("simple.count")

        "NOT exploded" when runningDeepDiffTestCase {
          testCase
            .conf( // language=HOCON
              """keys = ["key"]
                |ignore.fields =["array.count"]
                |""".stripMargin)
            .expectRaw(
              builder
                .common(
                  "array (struct)",
                  identical = 5,
                  diffExamples = Seq(
                    (
                      Key("different-user") >> Key(0),
                      Map("key" -> "diff", "user" -> "7"),
                      Map("key" -> "diff", "user" -> "77")
                    )
                  )
                )
                .common("array.key", identical = 6)
                .common(
                  "array.user",
                  identical = 5,
                  diffExamples = Seq((Key("different-user") >> Key(0), "7", "77"))
                )
            )
        }
        "exploded" when runningDeepDiffTestCase {
          testCase
            .conf( // language=HOCON
              """keys = ["key"]
                |ignore.fields =["array.count"]
                |exploded-arrays {
                |  array = { keys = ["key"] }
                |}
                |""".stripMargin)
            .expectRaw(
              builder
                .explodedArray(
                  "array",
                  DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
                    .record(identical = 5,
                            diffExamples = Seq(
                              (
                                Key("different-user") >> Key("diff"),
                                Map("key" -> "diff", "user" -> "7"),
                                Map("key" -> "diff", "user" -> "77")
                              )
                            ))
                    .kindOfDifferent(content = 1)
                    .common("key", identical = 6)
                    .common(
                      "user",
                      identical = 5,
                      diffExamples = Seq((Key("different-user") >> Key("diff"), "7", "77"))
                    )
                )
            )

        }
      }
      "is left/right only" when {
        val testCase = DeepDiffTestCase
          .testReversedLeftAndRight()
          .testPruned(
            left = struct(
              "key" -> StringType,
              "user" -> StringType,
              "count" -> IntegerType,
              "simple" -> struct(
                "key" -> StringType,
                "user" -> StringType,
                "count" -> IntegerType
              ),
              "array" -> ArrayType(
                struct(
                  "key" -> StringType,
                  "user" -> StringType
                ))
            ),
            right = struct(
              "key" -> StringType,
              "simple" -> struct(
                "key" -> StringType
              ),
              "array" -> ArrayType(
                struct(
                  "key" -> StringType
                ))
            )
          )
          .compare(
            left = Seq(
              Record("identical-null"),
              Record("identical", array = Seq(Simple("b", user = null, count = null))),
              Record("record-only-count", array = Seq(Simple("c", user = null, count = 3))),
              Record("record-only-user", array = Seq(Simple("diff", user = "4", count = null)))
            ),
            right = Seq(
              PrunedRecord("identical-null"),
              PrunedRecord("identical", array = Seq(KeyOnly("b"))),
              PrunedRecord("record-only-count", array = Seq(KeyOnly("c"))),
              PrunedRecord("record-only-user", array = Seq(KeyOnly("diff")))
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
                  "count" -> null,
                  "simple" -> null,
                  "array" -> Seq(Map("key" -> "diff", "user" -> "4"))
                ),
                Map("key" -> "record-only-user", "simple" -> null, "array" -> Seq(Map("key" -> "diff")))
              )
            )
          )
          .common("key", identical = 4)
          .leftOnly("user", identical = 4)
          .leftOnly("count", identical = 4)
          .common("simple", identical = 4)
          .common("simple.key")
          .leftOnly("simple.user")
          .leftOnly("simple.count")
          .common(
            "array",
            identical = 3,
            diffExamples =
              Seq((Key("record-only-user"), Seq(Map("key" -> "diff", "user" -> "4")), Seq(Map("key" -> "diff"))))
          )
        "NOT exploded" when runningDeepDiffTestCase {
          testCase
            .conf( // language=HOCON
              """keys = ["key"]
                |ignore.fields =["array.count"]
                |""".stripMargin)
            .expectRaw(
              builder
                .common(
                  "array (struct)",
                  identical = 2,
                  diffExamples =
                    Seq((Key("record-only-user") >> Key(0), Map("key" -> "diff", "user" -> "4"), Map("key" -> "diff")))
                )
                .common("array.key", identical = 3)
                .leftOnly("array.user", identical = 2, leftExamples = Seq((Key("record-only-user") >> Key(0), "4")))
            )
        }
        "exploded" when runningDeepDiffTestCase {
          testCase
            .conf( // language=HOCON
              """keys = ["key"]
                |ignore.fields =["array.count"]
                |exploded-arrays {
                |  array = { keys = ["key"] }
                |}
                |""".stripMargin)
            .expectRaw(
              builder
                .explodedArray(
                  "array",
                  DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
                    .record(identical = 2,
                            diffExamples = Seq(
                              (Key("record-only-user") >> Key("diff"),
                               Map("key" -> "diff", "user" -> "4"),
                               Map("key" -> "diff"))
                            ))
                    .kindOfDifferent(leftOnly = 1)
                    .common("key", identical = 3)
                    .leftOnly("user", identical = 2, leftExamples = Seq((Key("record-only-user") >> Key("diff"), "4")))
                )
            )

        }
      }
    }

    "be able to ignore multiple fields" in {
      DeepDiffTestCase
        .testPruned(struct(
          "key" -> StringType,
          "user" -> StringType,
          "simple" -> struct(
            "key" -> StringType,
            "user" -> StringType
          ),
          "array" -> ArrayType(struct(
            "key" -> StringType,
            "user" -> StringType
          ))
        ))
        .conf( // language=HOCON
          """keys = ["key"]
            |ignore.fields =["count", "simple.count", "array.count"]
            |""".stripMargin)
        .compare(
          left = Seq(
            Record("identical-null"),
            Record(
              "identical",
              user = "b",
              count = 2,
              simple = Simple("bb", user = "22", count = 22),
              array = Seq(Simple("bbb", user = "222", count = 222))
            ),
            Record(
              "different-count",
              user = "c",
              count = 3,
              simple = Simple("cc", user = "33", count = 33),
              array = Seq(Simple("ccc", user = "333", count = 333))
            ),
            Record(
              "left-only-count",
              user = "d",
              count = 4,
              simple = Simple("dd", user = "44", count = 44),
              array = Seq(Simple("ddd", user = "444", count = 444))
            ),
            Record(
              "right-only-count",
              user = "e",
              count = null,
              simple = Simple("ee", user = "55", count = null),
              array = Seq(Simple("eee", user = "555", count = null))
            ),
            Record(
              "null-count",
              user = "f",
              count = null,
              simple = Simple("ff", user = "66", count = null),
              array = Seq(Simple("fff", user = "666", count = null))
            )
          ),
          right = Seq(
            Record("identical-null"),
            Record(
              "identical",
              user = "b",
              count = 2,
              simple = Simple("bb", user = "22", count = 22),
              array = Seq(Simple("bbb", user = "222", count = 222))
            ),
            Record(
              "different-count",
              user = "c",
              count = 9,
              simple = Simple("cc", user = "33", count = 99),
              array = Seq(Simple("ccc", user = "333", count = 999))
            ),
            Record(
              "left-only-count",
              user = "d",
              count = null,
              simple = Simple("dd", user = "44", count = null),
              array = Seq(Simple("ddd", user = "444", count = null))
            ),
            Record(
              "right-only-count",
              user = "e",
              count = 5,
              simple = Simple("ee", user = "55", count = 55),
              array = Seq(Simple("eee", user = "555", count = 555))
            ),
            Record(
              "null-count",
              user = "f",
              count = null,
              simple = Simple("ff", user = "66", count = null),
              array = Seq(Simple("fff", user = "666", count = null))
            )
          )
        )
        .expectRaw(
          DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
            .record(identical = 6)
            .common("key", identical = 6)
            .common("user", identical = 6)
            .common("simple", identical = 6)
            .common("simple.key", identical = 5)
            .common("simple.user", identical = 5)
            .common("array", identical = 6)
            .common("array (struct)", identical = 5)
            .common("array.key", identical = 5)
            .common("array.user", identical = 5)
        )
    }

    "be able to ignore all fields on one side of a nested struct / array of structs" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .testReversedLeftAndRight()
        .testPruned(
          left = struct(
            "key" -> StringType,
            "user" -> StringType,
            "count" -> IntegerType,
            "simple" -> struct(
              "user" -> StringType,
              "count" -> IntegerType
            ),
            "array" -> ArrayType(
              struct(
                "user" -> StringType,
                "count" -> IntegerType
              ))
          ),
          right = struct(
            "key" -> StringType
          )
        )
        .conf( // language=HOCON
          """keys = ["key"]
            |ignore.fields =["simple.key", "array.key"]
            |""".stripMargin)
        .compare(
          left = Seq(
            Record("identical-null"),
            Record("identical"),
            Record("left-only", simple = Simple(key = "b", user = "3"), array = Seq(Simple(key = "b", count = 3)))
          ),
          right = Seq(
            PrunedRecord("identical-null"),
            PrunedRecord("identical", simple = KeyOnly("a"), array = Seq(KeyOnly("a"))),
            PrunedRecord("left-only", simple = KeyOnly("b"), array = Seq(KeyOnly("b")))
          )
        )
        .expectRaw(
          DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
            .kindOfDifferent(leftOnly = 1)
            .record(
              identical = 2,
              diffExamples = Seq(
                (
                  Key("left-only"),
                  Map(
                    "key" -> "left-only",
                    "user" -> null,
                    "count" -> null,
                    "simple" -> Map("user" -> "3", "count" -> null),
                    "array" -> Seq(Map("user" -> null, "count" -> 3))
                  ),
                  Map("key" -> "left-only")
                )
              )
            )
            .common("key", identical = 3)
            .leftOnly("user", identical = 3)
            .leftOnly("count", identical = 3)
            .leftOnly(
              "simple",
              identical = 2,
              leftExamples = Seq((Key("left-only"), Map("user" -> "3", "count" -> null)))
            )
            .leftOnly("simple.user")
            .leftOnly("simple.count")
            .leftOnly(
              "array",
              identical = 2,
              leftExamples = Seq((Key("left-only"), Seq(Map("user" -> null, "count" -> 3))))
            )
            .leftOnly(
              "array (struct)",
              leftExamples = Seq((Key("left-only") >> Key(0), Map("user" -> null, "count" -> 3)))
            )
            .leftOnly("array.user")
            .leftOnly("array.count")
        )
    }

    "be able to ignore all fields except the key in an exploded struct array" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .testPruned(struct(
          "key" -> StringType,
          "user" -> StringType,
          "count" -> IntegerType,
          "simple" -> struct(
            "key" -> StringType,
            "user" -> StringType,
            "count" -> IntegerType
          ),
          "array" -> ArrayType(struct(
            "key" -> StringType
          ))
        ))
        .conf( // language=HOCON
          """keys = ["key"]
            |ignore.fields =["array.user", "array.count"]
            |exploded-arrays {
            |  array = { keys = ["key"] }
            |}
            |""".stripMargin)
        .compare(
          left = Seq(
            Record("identical-null", array = null),
            Record("identical-empty", array = Seq.empty),
            Record("identical", array = Seq(Simple(key = "a"), Simple(key = "b"))),
            Record("different", array = Seq(Simple(key = "c")))
          ),
          right = Seq(
            Record("identical-null", array = null),
            Record("identical-empty", array = Seq.empty),
            Record("identical", array = Seq(Simple(key = "a", user = "7"), Simple(key = "b", count = 7))),
            Record("different", array = Seq(Simple(key = "d")))
          )
        )
        .expectRaw(
          DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
            .kindOfDifferent(leftAndRightOnly = 1)
            .record(
              identical = 3,
              diffExamples = Seq(
                (
                  Key("different"),
                  Map(
                    "key" -> "different",
                    "user" -> null,
                    "count" -> null,
                    "simple" -> null,
                    "array" -> Seq(Map("key" -> "c"))
                  ),
                  Map(
                    "key" -> "different",
                    "user" -> null,
                    "count" -> null,
                    "simple" -> null,
                    "array" -> Seq(Map("key" -> "d"))
                  )
                )
              )
            )
            .common("key", identical = 4)
            .common("user", identical = 4)
            .common("count", identical = 4)
            .common("simple", identical = 4)
            .common("simple.key")
            .common("simple.user")
            .common("simple.count")
            .common(
              "array",
              identical = 3,
              diffExamples = Seq((Key("different"), Seq(Map("key" -> "c")), Seq(Map("key" -> "d"))))
            )
            .explodedArray(
              "array",
              DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
                .record(identical = 2,
                        leftExamples = Seq((Key("different") >> Key("c"), Map("key" -> "c"))),
                        rightExamples = Seq((Key("different") >> Key("d"), Map("key" -> "d"))))
                .common("key", identical = 2)
            )
        )
    }

    "ignore whole nested struct" which {
      behave like ignoreWholeStruct( // language=HOCON
        """keys = ["key"]
          |ignore.fields =["simple"]
          |""".stripMargin)
    }

    "ignore the whole nested struct if all its fields are ignored" which {
      behave like ignoreWholeStruct( // language=HOCON
        """keys = ["key"]
          |ignore.fields =["simple.key", "simple.user", "simple.count"]
          |""".stripMargin)
    }

    "ignore whole struct arrays" which {
      behave like ignoreWholeArray( // language=HOCON
        """keys = ["key"]
          |ignore.fields =["array"]
          |""".stripMargin)
    }

    "ignore the whole struct arrays if all its fields are ignored" which {
      behave like ignoreWholeArray( // language=HOCON
        """keys = ["key"]
          |ignore.fields =["array.key", "array.user", "array.count"]
          |""".stripMargin)
    }

    "ignore whole exploded struct arrays" which {
      behave like ignoreWholeArray( // language=HOCON
        """keys = ["key"]
          |ignore.fields =["array"]
          |exploded-arrays {
          |  array = { keys = ["key"] }
          |}
          |""".stripMargin)
    }

    "ignore the whole exploded struct arrays if all its fields are ignored" which {
      behave like ignoreWholeArray( // language=HOCON
        """keys = ["key"]
          |ignore.fields =["array.key", "array.user", "array.count"]
          |exploded-arrays {
          |  array = { keys = ["key"] }
          |}
          |""".stripMargin)
    }

    "raise an exception if a key field is ignored" in {
      a[Exception] should be thrownBy {
        behave like runningDeepDiffTestCase(
          validConfTestCase( // language=HOCON
            """keys = ["key"]
              |ignore.fields =["key"]
              |""".stripMargin)
        )
      }
    }

    "raise an exception if a exploded struct array key field is ignored (and not all fields are ignored)" in {
      a[Exception] should be thrownBy {
        behave like runningDeepDiffTestCase(
          validConfTestCase( // language=HOCON
            """keys = ["key"]
              |ignore.fields =["array.key"]
              |exploded-arrays {
              |  array = { keys = ["key"] }
              |}
              |""".stripMargin)
        )
      }
    }
  }

  private def validConfTestCase(conf: String): DeepDiffTestCase[Properties.AllMandatory] = {
    DeepDiffTestCase
      .conf(conf)
      .compare(
        left = Seq(Record("identical-null")),
        right = Seq(Record("identical-null"))
      )
      .expectRaw(
        DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
          .record(identical = 1)
          .common("key", identical = 1)
          .common("user", identical = 1)
          .common("count", identical = 1)
          .common("simple", identical = 1)
          .common("simple.key")
          .common("simple.user")
          .common("simple.count")
          .common("array", identical = 1)
          .common("array (struct)")
          .common("array.key")
          .common("array.user")
          .common("array.count")
      )
  }

  private def ignoreWholeArray(conf: String): Unit = {
    "is common" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .testPruned(
          struct(
            "key" -> StringType,
            "user" -> StringType,
            "count" -> IntegerType,
            "simple" -> struct(
              "key" -> StringType,
              "user" -> StringType,
              "count" -> IntegerType
            )
          ))
        .conf(conf)
        .compare(
          left = Seq(
            Record("identical-null"),
            Record("identical", user = "B", array = Seq(Simple("b", user = "2", count = 2))),
            Record("different-array", user = "C", array = Seq(Simple("c", user = "3", count = 3))),
            Record("left-only-array", user = "D", array = Seq(Simple("d", user = "4", count = 4))),
            Record("right-only-array", user = "E", array = null),
            Record("null-array", user = "F", array = null),
            Record("left-empty-array", user = "G", array = Seq.empty),
            Record("right-empty-array", user = "H", array = Seq(Simple("h", user = "8", count = 8))),
            Record("empty-array", user = "I", array = Seq.empty),
            Record("different", user = "J", array = Seq(Simple("j", user = "10", count = 10)))
          ),
          right = Seq(
            Record("identical-null"),
            Record("identical", user = "B", array = Seq(Simple("b", user = "2", count = 2))),
            Record("different-array", user = "C", array = Seq(Simple("cc", user = "33", count = 33))),
            Record("left-only-array", user = "D", array = null),
            Record("right-only-array", user = "E", array = Seq(Simple("e", user = "5", count = 5))),
            Record("null-array", user = "F", array = null),
            Record("left-empty-array", user = "G", array = Seq(Simple("g", user = "7", count = 7))),
            Record("right-empty-array", user = "H", array = Seq.empty),
            Record("empty-array", user = "I", array = Seq.empty),
            Record("different", user = "JJ", array = Seq(Simple("jj", user = "1010", count = 1010)))
          )
        )
        .expectRaw(
          DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
            .kindOfDifferent(content = 1)
            .record(
              identical = 9,
              diffExamples = Seq(
                (
                  Key("different"),
                  Map("key" -> "different", "user" -> "J", "count" -> null, "simple" -> null),
                  Map("key" -> "different", "user" -> "JJ", "count" -> null, "simple" -> null)
                )
              )
            )
            .common("key", identical = 10)
            .common("user", identical = 9, diffExamples = Seq((Key("different"), "J", "JJ")))
            .common("count", identical = 10)
            .common("simple", identical = 10)
            .common("simple.key")
            .common("simple.user")
            .common("simple.count")
        )
    }

    "is left/right only" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .testReversedLeftAndRight()
        .testPruned(
          left = struct(
            "key" -> StringType,
            "user" -> StringType,
            "count" -> IntegerType,
            "simple" -> struct(
              "key" -> StringType,
              "user" -> StringType,
              "count" -> IntegerType
            )
          ),
          right = struct(
            "key" -> StringType
          )
        )
        .conf(conf)
        .compare(
          left = Seq(
            Record("identical-null"),
            Record("identical", array = Seq(Simple("b", user = null, count = null))),
            Record("different", user = "C", array = Seq(Simple("c", user = null, count = null)))
          ),
          right = Seq(
            KeyOnly("identical-null"),
            KeyOnly("identical"),
            KeyOnly("different")
          )
        )
        .expectRaw(
          DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
            .kindOfDifferent(leftOnly = 1)
            .record(
              identical = 2,
              diffExamples = Seq(
                (
                  Key("different"),
                  Map(
                    "key" -> "different",
                    "user" -> "C",
                    "count" -> null,
                    "simple" -> null
                  ),
                  Map("key" -> "different")
                )
              )
            )
            .common("key", identical = 3)
            .leftOnly("user", identical = 2, leftExamples = Seq((Key("different"), "C")))
            .leftOnly("count", identical = 3)
            .leftOnly("simple", identical = 3)
            .leftOnly("simple.key")
            .leftOnly("simple.user")
            .leftOnly("simple.count")
        )
    }
  }

  private def ignoreWholeStruct(conf: String): Unit = {
    "is common" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .testPruned(
          struct(
            "key" -> StringType,
            "user" -> StringType,
            "count" -> IntegerType,
            "array" -> ArrayType(
              struct(
                "key" -> StringType,
                "user" -> StringType,
                "count" -> IntegerType
              ))
          ))
        .conf(conf)
        .compare(
          left = Seq(
            Record("identical-null"),
            Record("identical", user = "B", simple = Simple("b", user = "2", count = 2)),
            Record("different-simple", user = "C", simple = Simple("c", user = "3", count = 3)),
            Record("left-only-simple", user = "D", simple = Simple("d", user = "4", count = 4)),
            Record("right-only-simple", user = "E", simple = null),
            Record("null-simple", user = "F", simple = null),
            Record("different", user = "G", simple = Simple("g", user = "7", count = 7))
          ),
          right = Seq(
            Record("identical-null"),
            Record("identical", user = "B", simple = Simple("b", user = "2", count = 2)),
            Record("different-simple", user = "C", simple = Simple("cc", user = "33", count = 33)),
            Record("left-only-simple", user = "D", simple = null),
            Record("right-only-simple", user = "E", simple = Simple("e", user = "5", count = 5)),
            Record("null-simple", user = "F", simple = null),
            Record("different", user = "GG", simple = Simple("gg", user = "77", count = 77))
          )
        )
        .expectRaw(
          DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
            .kindOfDifferent(content = 1)
            .record(
              identical = 6,
              diffExamples = Seq(
                (
                  Key("different"),
                  Map(
                    "key" -> "different",
                    "user" -> "G",
                    "count" -> null,
                    "array" -> null
                  ),
                  Map(
                    "key" -> "different",
                    "user" -> "GG",
                    "count" -> null,
                    "array" -> null
                  )
                )
              )
            )
            .common("key", identical = 7)
            .common("user", identical = 6, diffExamples = Seq((Key("different"), "G", "GG")))
            .common("count", identical = 7)
            .common("array", identical = 7)
            .common("array (struct)")
            .common("array.key")
            .common("array.user")
            .common("array.count")
        )
    }

    "is left/right only" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .testReversedLeftAndRight()
        .testPruned(
          left = struct(
            "key" -> StringType,
            "user" -> StringType,
            "count" -> IntegerType,
            "array" -> ArrayType(
              struct(
                "key" -> StringType,
                "user" -> StringType,
                "count" -> IntegerType
              ))
          ),
          right = struct(
            "key" -> StringType
          )
        )
        .conf(conf)
        .compare(
          left = Seq(
            Record("identical-null"),
            Record("identical", simple = Simple("b", user = null, count = null)),
            Record("different", user = "C", simple = Simple("c", user = null, count = null))
          ),
          right = Seq(
            KeyOnly("identical-null"),
            KeyOnly("identical"),
            KeyOnly("different")
          )
        )
        .expectRaw(
          DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
            .kindOfDifferent(leftOnly = 1)
            .record(
              identical = 2,
              diffExamples = Seq(
                (
                  Key("different"),
                  Map(
                    "key" -> "different",
                    "user" -> "C",
                    "count" -> null,
                    "array" -> null
                  ),
                  Map("key" -> "different")
                )
              )
            )
            .common("key", identical = 3)
            .leftOnly("user", identical = 2, leftExamples = Seq((Key("different"), "C")))
            .leftOnly("count", identical = 3)
            .leftOnly("array", identical = 3)
            .leftOnly("array (struct)")
            .leftOnly("array.key")
            .leftOnly("array.user")
            .leftOnly("array.count")
        )
    }
  }
}
