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

object AliasSpec {
  final case class Nested(key: String, count: java.lang.Integer = null)
  final case class Record(
      key: String,
      count: java.lang.Integer = null,
      nested: Nested = null,
      array: Seq[Nested] = null,
      exploded_array: Seq[Nested] = null
  )
  final case class Nested2(key2: String, count2: java.lang.Integer = null)
  final case class Record2(
      key2: String,
      count2: java.lang.Integer = null,
      nested2: Nested2 = null,
      array2: Seq[Nested2] = null,
      exploded_array2: Seq[Nested2] = null
  )
  final case class Nested3(key3: String, count3: java.lang.Integer = null)
  final case class Record3(
      key3: String,
      count3: java.lang.Integer = null,
      nested3: Nested3 = null,
      array3: Seq[Nested3] = null,
      exploded_array3: Seq[Nested3] = null
  )
}

final class AliasSpec extends DeepDiffSpec {
  import AliasSpec._
  import DeepDiffSpec._
  import com.criteo.deepdiff.test_utils.EasyKeyExample._

  "Deep Diff" should {
    "use aliases" which {
      val left = Seq(
        Record("identical-null"),
        Record(
          key = "identical",
          count = 20,
          nested = Nested(key = "aa", count = 21),
          array = Seq(Nested(key = "ab", count = 22)),
          exploded_array = Seq(Nested(key = "ac", count = 23))
        ),
        Record(
          key = "different",
          count = 30,
          nested = Nested(key = "ba", count = 31),
          array = Seq(Nested(key = "bb", count = 32)),
          exploded_array = Seq(Nested(key = "bc", count = 33))
        )
      )
      val right = Seq(
        Record2("identical-null"),
        Record2(
          key2 = "identical",
          count2 = 20,
          nested2 = Nested2(key2 = "aa", count2 = 21),
          array2 = Seq(Nested2(key2 = "ab", count2 = 22)),
          exploded_array2 = Seq(Nested2(key2 = "ac", count2 = 23))
        ),
        Record2(
          key2 = "different",
          count2 = 300,
          nested2 = Nested2(key2 = "BA", count2 = 310),
          array2 = Seq(Nested2(key2 = "BB", count2 = 320)),
          exploded_array2 = Seq(Nested2(key2 = "bc", count2 = 330))
        )
      )
      val builder = DatasetDiffsBuilder[KeyExample, Product]()
        .kindOfDifferent(content = 1)
        .record(
          identical = 2,
          diffExamples = Seq(
            (
              Key("different"),
              left.last,
              Record(
                key = "different",
                count = 300,
                nested = Nested(key = "BA", count = 310),
                array = Seq(Nested(key = "BB", count = 320)),
                exploded_array = Seq(Nested(key = "bc", count = 330))
              )
            )
          )
        )
        .common("key", identical = 3)
        .common("count", identical = 2, diffExamples = Seq((Key("different"), 30, 300)))
        .common(
          "nested",
          identical = 2,
          diffExamples = Seq((Key("different"), Nested("ba", 31), Nested("BA", 310)))
        )
        .common("nested.key", identical = 1, diffExamples = Seq((Key("different"), "ba", "BA")))
        .common("nested.count", identical = 1, diffExamples = Seq((Key("different"), 31, 310)))
        .common(
          "array",
          identical = 2,
          diffExamples = Seq((Key("different"), Seq(Nested("bb", 32)), Seq(Nested("BB", 320))))
        )
        .common(
          "array (struct)",
          identical = 1,
          diffExamples = Seq((Key("different") >> Key(0), Nested("bb", 32), Nested("BB", 320)))
        )
        .common("array.key", identical = 1, diffExamples = Seq((Key("different") >> Key(0), "bb", "BB")))
        .common("array.count", identical = 1, diffExamples = Seq((Key("different") >> Key(0), 32, 320)))
        .common(
          "exploded_array",
          identical = 2,
          diffExamples = Seq((Key("different"), Seq(Nested("bc", 33)), Seq(Nested("bc", 330))))
        )
        .explodedArray(
          "exploded_array",
          DatasetDiffsBuilder[KeyExample, Product]()
            .record(identical = 1,
                    diffExamples = Seq((Key("different") >> Key("bc"), Nested("bc", 33), Nested("bc", 330))))
            .kindOfDifferent(content = 1)
            .common("key", identical = 2)
            .common(
              "count",
              identical = 1,
              diffExamples = Seq((Key("different") >> Key("bc"), 33, 330))
            )
        )

      "are used on one side" when runningDeepDiffTestCase {
        DeepDiffTestCase
          .testReversedLeftAndRight()
          .testPruned(struct(
            "key" -> StringType,
            "count" -> IntegerType,
            "nested" -> struct(
              "key" -> StringType,
              "count" -> IntegerType
            ),
            "array" -> ArrayType(struct(
              "key" -> StringType,
              "count" -> IntegerType
            )),
            "exploded_array" -> ArrayType(struct(
              "key" -> StringType,
              "count" -> IntegerType
            ))
          ))
          .conf( //language=HOCON
            """keys = ["key"]
              |exploded-arrays {
              |  exploded_array = { keys = ["key"] }
              |}
              |right-aliases {
              |  key2 = "key"
              |  count2 = "count"
              |  nested2 = "nested"
              |  "nested2.key2" = "key"
              |  "nested2.count2" = "count"
              |  "array2" = "array"
              |  "array2.key2" = "key"
              |  "array2.count2" = "count"
              |  "exploded_array2" = "exploded_array"
              |  "exploded_array2.key2" = "key"
              |  "exploded_array2.count2" = "count"
              |}
              |""".stripMargin)
          .withReversedConfig( //language=HOCON
            """keys = ["key"]
              |exploded-arrays {
              |  exploded_array = { keys = ["key"] }
              |}
              |left-aliases {
              |  key2 = "key"
              |  count2 = "count"
              |  nested2 = "nested"
              |  "nested2.key2" = "key"
              |  "nested2.count2" = "count"
              |  "array2" = "array"
              |  "array2.key2" = "key"
              |  "array2.count2" = "count"
              |  "exploded_array2" = "exploded_array"
              |  "exploded_array2.key2" = "key"
              |  "exploded_array2.count2" = "count"
              |}
              |""".stripMargin)
          .compare(left, right)
          .expect(builder)
      }

      "are used on both side" when runningDeepDiffTestCase {
        DeepDiffTestCase
          .testPruned(struct(
            "key" -> StringType,
            "count" -> IntegerType,
            "nested" -> struct(
              "key" -> StringType,
              "count" -> IntegerType
            ),
            "array" -> ArrayType(struct(
              "key" -> StringType,
              "count" -> IntegerType
            )),
            "exploded_array" -> ArrayType(struct(
              "key" -> StringType,
              "count" -> IntegerType
            ))
          ))
          .conf( //language=HOCON
            """keys = ["key"]
              |exploded-arrays {
              |  exploded_array = { keys = ["key"] }
              |}
              |left-aliases {
              |  key3 = "key"
              |  count3 = "count"
              |  nested3 = "nested"
              |  "nested3.key3" = "key"
              |  "nested3.count3" = "count"
              |  "array3" = "array"
              |  "array3.key3" = "key"
              |  "array3.count3" = "count"
              |  "exploded_array3" = "exploded_array"
              |  "exploded_array3.key3" = "key"
              |  "exploded_array3.count3" = "count"
              |}
              |right-aliases {
              |  key2 = "key"
              |  count2 = "count"
              |  nested2 = "nested"
              |  "nested2.key2" = "key"
              |  "nested2.count2" = "count"
              |  "array2" = "array"
              |  "array2.key2" = "key"
              |  "array2.count2" = "count"
              |  "exploded_array2" = "exploded_array"
              |  "exploded_array2.key2" = "key"
              |  "exploded_array2.count2" = "count"
              |}
              |""".stripMargin)
          .compare(
            left = Seq(
              Record3("identical-null"),
              Record3(
                key3 = "identical",
                count3 = 20,
                nested3 = Nested3(key3 = "aa", count3 = 21),
                array3 = Seq(Nested3(key3 = "ab", count3 = 22)),
                exploded_array3 = Seq(Nested3(key3 = "ac", count3 = 23))
              ),
              Record3(
                key3 = "different",
                count3 = 30,
                nested3 = Nested3(key3 = "ba", count3 = 31),
                array3 = Seq(Nested3(key3 = "bb", count3 = 32)),
                exploded_array3 = Seq(Nested3(key3 = "bc", count3 = 33))
              )
            ),
            right
          )
          .expect(builder)
      }

    }

    "use aliases before ignore fields" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .testPruned(struct(
          "key" -> StringType,
          "nested" -> struct(
            "key" -> StringType
          ),
          "array" -> ArrayType(struct(
            "key" -> StringType
          )),
          "exploded_array" -> ArrayType(struct(
            "key" -> StringType
          ))
        ))
        .conf( //language=HOCON
          """keys = ["key"]
            |exploded-arrays {
            |  exploded_array = { keys = ["key"] }
            |}
            |ignore.fields =["count", "nested.count", "array.count", "exploded_array.count"]
            |left-aliases {
            |  key3 = "key"
            |  count3 = "count"
            |  nested3 = "nested"
            |  "nested3.key3" = "key"
            |  "nested3.count3" = "count"
            |  "array3" = "array"
            |  "array3.key3" = "key"
            |  "array3.count3" = "count"
            |  "exploded_array3" = "exploded_array"
            |  "exploded_array3.key3" = "key"
            |  "exploded_array3.count3" = "count"
            |}
            |right-aliases {
            |  key2 = "key"
            |  count2 = "count"
            |  nested2 = "nested"
            |  "nested2.key2" = "key"
            |  "nested2.count2" = "count"
            |  "array2" = "array"
            |  "array2.key2" = "key"
            |  "array2.count2" = "count"
            |  "exploded_array2" = "exploded_array"
            |  "exploded_array2.key2" = "key"
            |  "exploded_array2.count2" = "count"
            |}
            |""".stripMargin)
        .compare(
          left = Seq(
            Record3(
              key3 = "identical",
              count3 = 20,
              nested3 = Nested3(key3 = "aa", count3 = 21),
              array3 = Seq(Nested3(key3 = "ab", count3 = 22)),
              exploded_array3 = Seq(Nested3(key3 = "ac", count3 = 23))
            ),
            Record3(
              key3 = "different",
              count3 = 30,
              nested3 = Nested3(key3 = "ba", count3 = 31),
              array3 = Seq(Nested3(key3 = "bb", count3 = 32)),
              exploded_array3 = Seq(Nested3(key3 = "bc", count3 = 33))
            )
          ),
          right = Seq(
            Record2(
              key2 = "identical",
              count2 = 200,
              nested2 = Nested2(key2 = "aa", count2 = 210),
              array2 = Seq(Nested2(key2 = "ab", count2 = 220)),
              exploded_array2 = Seq(Nested2(key2 = "ac", count2 = 230))
            ),
            Record2(
              key2 = "different",
              count2 = 300,
              nested2 = Nested2(key2 = "BA", count2 = 310),
              array2 = Seq(Nested2(key2 = "BB", count2 = 320)),
              exploded_array2 = Seq(Nested2(key2 = "bc2", count2 = 330))
            )
          )
        )
        .expectRaw(
          DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
            .kindOfDifferent(content = 1)
            .record(
              identical = 1,
              diffExamples = Seq(
                (
                  Key("different"),
                  Map(
                    "key" -> "different",
                    "nested" -> Map("key" -> "ba"),
                    "array" -> Seq(Map("key" -> "bb")),
                    "exploded_array" -> Seq(Map("key" -> "bc"))
                  ),
                  Map(
                    "key" -> "different",
                    "nested" -> Map("key" -> "BA"),
                    "array" -> Seq(Map("key" -> "BB")),
                    "exploded_array" -> Seq(Map("key" -> "bc2"))
                  )
                )
              )
            )
            .common("key", identical = 2)
            .common(
              "nested",
              identical = 1,
              diffExamples = Seq((Key("different"), Map("key" -> "ba"), Map("key" -> "BA")))
            )
            .common("nested.key", identical = 1, diffExamples = Seq((Key("different"), "ba", "BA")))
            .common(
              "array",
              identical = 1,
              diffExamples = Seq((Key("different"), Seq(Map("key" -> "bb")), Seq(Map("key" -> "BB"))))
            )
            .common(
              "array (struct)",
              identical = 1,
              diffExamples = Seq((Key("different") >> Key(0), Map("key" -> "bb"), Map("key" -> "BB")))
            )
            .common("array.key", identical = 1, diffExamples = Seq((Key("different") >> Key(0), "bb", "BB")))
            .common(
              "exploded_array",
              identical = 1,
              diffExamples = Seq((Key("different"), Seq(Map("key" -> "bc")), Seq(Map("key" -> "bc2"))))
            )
            .explodedArray(
              "exploded_array",
              DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
                .record(identical = 1,
                        leftExamples = Seq((Key("different") >> Key("bc"), Map("key" -> "bc"))),
                        rightExamples = Seq((Key("different") >> Key("bc2"), Map("key" -> "bc2"))))
                .common("key", identical = 1)
            )
        )
    }
  }
}
