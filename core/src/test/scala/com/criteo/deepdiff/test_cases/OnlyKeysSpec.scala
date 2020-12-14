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

object OnlyKeysSpec {
  final case class Nested(key: String, count: java.lang.Integer = null)
  final case class Record(key: String, nested: Nested, exploded_array: Seq[Nested], count: java.lang.Integer = null)
}

final class OnlyKeysSpec extends DeepDiffSpec {
  import DeepDiffSpec._
  import OnlyKeysSpec._
  import com.criteo.deepdiff.test_utils.EasyKeyExample._

  "Deep Diff" should {
    "still work as expected, even when all fields are keys" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .testPruned(
          struct(
            "key" -> StringType,
            "nested" -> struct("key" -> StringType),
            "exploded_array" -> ArrayType(struct("key" -> StringType))
          ))
        .conf( //language=HOCON
          """keys = ["key", "nested.key"]
            |ignore.fields =["count", "nested.count", "exploded_array.count"]
            |exploded-arrays {
            |  exploded_array = { keys = ["key"] }
            |}
            |""".stripMargin)
        .compare(
          left = Seq(
            Record("identical",
                   nested = Nested("nested-identical"),
                   exploded_array = Seq(Nested("exploded-identical"))),
            Record("different", nested = Nested("nested-different"), exploded_array = Seq(Nested("exploded-left"))),
            Record("left", nested = Nested("nested-left"), exploded_array = Seq.empty),
            Record("identical-2",
                   nested = Nested("nested-identical"),
                   exploded_array = Seq(Nested("exploded-identical"))),
            Record("different-2", nested = Nested("nested-different"), exploded_array = Seq(Nested("exploded-left"))),
            Record("left-2", nested = Nested("nested-left"), exploded_array = Seq.empty)
          ),
          right = Seq(
            Record("identical",
                   nested = Nested("nested-identical"),
                   exploded_array = Seq(Nested("exploded-identical"))),
            Record("different", nested = Nested("nested-different"), exploded_array = Seq(Nested("exploded-right"))),
            Record("right", nested = Nested("nested-right"), exploded_array = Seq.empty),
            // Adding count = 37 do avoid any UnsafeRow comparisons in the non-pruned case.
            Record("identical-2",
                   nested = Nested("nested-identical", count = 37),
                   exploded_array = Seq(Nested("exploded-identical", count = 37))),
            Record("different-2",
                   nested = Nested("nested-different", count = 37),
                   exploded_array = Seq(Nested("exploded-right", count = 37))),
            Record("right-2", nested = Nested("nested-right", count = 37), exploded_array = Seq.empty)
          )
        )
        .expectRaw(
          DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
            .kindOfDifferent(leftAndRightOnly = 2)
            .record(
              identical = 2,
              diffExamples = Seq(
                (Key("key" -> "different", "nested.key" -> "nested-different"),
                 Map("key" -> "different",
                     "nested" -> Map("key" -> "nested-different"),
                     "exploded_array" -> Seq(Map("key" -> "exploded-left"))),
                 Map("key" -> "different",
                     "nested" -> Map("key" -> "nested-different"),
                     "exploded_array" -> Seq(Map("key" -> "exploded-right")))),
                (Key("key" -> "different-2", "nested.key" -> "nested-different"),
                 Map("key" -> "different-2",
                     "nested" -> Map("key" -> "nested-different"),
                     "exploded_array" -> Seq(Map("key" -> "exploded-left"))),
                 Map("key" -> "different-2",
                     "nested" -> Map("key" -> "nested-different"),
                     "exploded_array" -> Seq(Map("key" -> "exploded-right"))))
              ),
              leftExamples = Seq(
                (Key("key" -> "left", "nested.key" -> "nested-left"),
                 Map("key" -> "left", "nested" -> Map("key" -> "nested-left"), "exploded_array" -> Seq.empty)),
                (Key("key" -> "left-2", "nested.key" -> "nested-left"),
                 Map("key" -> "left-2", "nested" -> Map("key" -> "nested-left"), "exploded_array" -> Seq.empty))
              ),
              rightExamples = Seq(
                (Key("key" -> "right", "nested.key" -> "nested-right"),
                 Map("key" -> "right", "nested" -> Map("key" -> "nested-right"), "exploded_array" -> Seq.empty)),
                (Key("key" -> "right-2", "nested.key" -> "nested-right"),
                 Map("key" -> "right-2", "nested" -> Map("key" -> "nested-right"), "exploded_array" -> Seq.empty))
              )
            )
            .common("key", identical = 4)
            .common("nested", identical = 4)
            .common("nested.key", identical = 4)
            .common(
              "exploded_array",
              identical = 2,
              diffExamples = Seq(
                (
                  Key("key" -> "different", "nested.key" -> "nested-different"),
                  Seq(Map("key" -> "exploded-left")),
                  Seq(Map("key" -> "exploded-right"))
                ),
                (
                  Key("key" -> "different-2", "nested.key" -> "nested-different"),
                  Seq(Map("key" -> "exploded-left")),
                  Seq(Map("key" -> "exploded-right"))
                )
              )
            )
            .explodedArray(
              "exploded_array",
              DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
                .record(
                  identical = 2,
                  leftExamples = Seq(
                    (
                      Key("key" -> "different", "nested.key" -> "nested-different") >> Key("exploded-left"),
                      Map("key" -> "exploded-left")
                    ),
                    (
                      Key("key" -> "different-2", "nested.key" -> "nested-different") >> Key("exploded-left"),
                      Map("key" -> "exploded-left")
                    )
                  ),
                  rightExamples = Seq(
                    (
                      Key("key" -> "different", "nested.key" -> "nested-different") >> Key("exploded-right"),
                      Map("key" -> "exploded-right")
                    ),
                    (
                      Key("key" -> "different-2", "nested.key" -> "nested-different") >> Key("exploded-right"),
                      Map("key" -> "exploded-right")
                    )
                  )
                )
                .common("key", identical = 2)
            )
        )
    }
  }
}
