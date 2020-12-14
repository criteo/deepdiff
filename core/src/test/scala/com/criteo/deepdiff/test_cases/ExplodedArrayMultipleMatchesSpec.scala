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
import com.criteo.deepdiff.test_utils.DeepDiffSpec.DeepDiffTestCase
import com.criteo.deepdiff.test_utils.{DatasetDiffsBuilder, DeepDiffSpec}

object ExplodedArrayMultipleMatchesSpec {
  final case class Nested(key: String, map: Map[String, Int] = null)
  final case class Record(key: String, records: Seq[Nested])
  final case class NestedB(key: String, map: Map[String, Int] = null, value: java.lang.Float = null)
  final case class RecordB(key: String, records: Seq[NestedB])
}

final class ExplodedArrayMultipleMatchesSpec extends DeepDiffSpec {
  import ExplodedArrayMultipleMatchesSpec._
  import com.criteo.deepdiff.test_utils.EasyKeyExample._

  "Multiple matches in exploded arrays" should {
    "be detected with root-level multiple matches" which {
      val left = Seq(
        Record("identical",
               Seq(
                 Nested("A", Map("a" -> 1))
               )),
        Record(
          "multiple",
          Seq(
            Nested("left", Map("b" -> 2)),
            Nested("left", Map("b" -> 3)),
            Nested("right", Map("c" -> 4)),
            Nested("left-identical", Map("d" -> 6)),
            Nested("left-identical", Map("d" -> 6)),
            Nested("right-identical", Map("e" -> 7)),
            Nested("left-only", Map("f" -> 8)),
            Nested("left-only", Map("f" -> 9)),
            Nested("left-only-identical", Map("f" -> 10)),
            Nested("left-only-identical", Map("f" -> 10))
          )
        )
      )
      val right = Seq(
        RecordB("identical",
                Seq(
                  NestedB("A", Map("a" -> 1))
                )),
        RecordB(
          "multiple",
          Seq(
            NestedB("left", Map("b" -> 2)),
            NestedB("right", Map("c" -> 4)),
            NestedB("right", Map("c" -> 5)),
            NestedB("left-identical", Map("d" -> 6)),
            NestedB("right-identical", Map("e" -> 7)),
            NestedB("right-identical", Map("e" -> 7)),
            NestedB("right-only", Map("f" -> 11)),
            NestedB("right-only", Map("f" -> 12)),
            NestedB("right-only-identical", Map("f" -> 13)),
            NestedB("right-only-identical", Map("f" -> 13))
          )
        )
      )
      val testCase = DeepDiffTestCase
        .testReversedLeftAndRight()
        .testPruned()
        .compare(left, right)
        .expect(
          DatasetDiffsBuilder[KeyExample, Product]()
            .kindOfDifferent()
            .record(
              identical = 1,
              diffExamples = Seq(
                (Key("multiple"), left(1), right(1))
              )
            )
            .common("key", identical = 2)
            .common(
              "records",
              identical = 1,
              diffExamples = Seq(
                (Key("multiple"), left(1).records, right(1).records)
              )
            )
            .explodedArray(
              "records",
              DatasetDiffsBuilder[KeyExample, Product]()
                .multipleMatches(
                  examples = Seq(
                    (Key("multiple") >> Key("left"),
                     Seq(Nested("left", Map("b" -> 3)), Nested("left", Map("b" -> 2))),
                     Seq(NestedB("left", Map("b" -> 2)))),
                    (Key("multiple") >> Key("right"),
                     Seq(Nested("right", Map("c" -> 4))),
                     Seq(NestedB("right", Map("c" -> 5)), NestedB("right", Map("c" -> 4)))),
                    (Key("multiple") >> Key("left-identical"),
                     Seq(Nested("left-identical", Map("d" -> 6)), Nested("left-identical", Map("d" -> 6))),
                     Seq(NestedB("left-identical", Map("d" -> 6)))),
                    (Key("multiple") >> Key("right-identical"),
                     Seq(Nested("right-identical", Map("e" -> 7))),
                     Seq(NestedB("right-identical", Map("e" -> 7)), NestedB("right-identical", Map("e" -> 7)))),
                    (Key("multiple") >> Key("left-only"),
                     Seq(Nested("left-only", Map("f" -> 9)), Nested("left-only", Map("f" -> 8))),
                     Nil),
                    (Key("multiple") >> Key("left-only-identical"),
                     Seq(Nested("left-only-identical", Map("f" -> 10)), Nested("left-only-identical", Map("f" -> 10))),
                     Nil),
                    (Key("multiple") >> Key("right-only"),
                     Nil,
                     Seq(NestedB("right-only", Map("f" -> 12)), NestedB("right-only", Map("f" -> 11)))),
                    (Key("multiple") >> Key("right-only-identical"),
                     Nil,
                     Seq(NestedB("right-only-identical", Map("f" -> 13)),
                         NestedB("right-only-identical", Map("f" -> 13))))
                  )
                )
                .record(identical = 1)
                .common("key", identical = 1)
                .common("map", identical = 1)
                .rightOnly("value", identical = 1)
            ))

      "are NOT omitted" when runningDeepDiffTestCase {
        testCase
          .conf( // language=HOCON
            """keys = ["key"]
              |exploded-arrays { records = { keys = ["key"] } }
              |""".stripMargin)
      }

      "are omitted" when runningDeepDiffTestCase {
        testCase
          .conf( // language=HOCON
            """keys = ["key"]
              |multiple-matches.omit-if-all-identical = true
              |exploded-arrays { records = { keys = ["key"] } }
              |""".stripMargin)
      }
    }

    "support ignored multiple identical matches" when runningDeepDiffTestCase {
      val left = Seq(
        Record(
          "identical-multiple",
          Seq(
            Nested("left-null"),
            Nested("left-null"),
            Nested("right-null"),
            Nested("left", Map("d" -> 1, "dd" -> 2, "ddd" -> 3, "dddd" -> 4)),
            Nested("left", Map("dd" -> 2, "dddd" -> 4, "d" -> 1, "ddd" -> 3)),
            Nested("right", Map("e" -> 1, "ee" -> 2, "eee" -> 3, "eeee" -> 4)),
            Nested("left-only", Map("f" -> 1, "ff" -> 2, "fff" -> 3, "ffff" -> 4)),
            Nested("left-only", Map("ff" -> 2, "ffff" -> 4, "f" -> 1, "fff" -> 3)),
            Nested("different"),
            Nested("different")
          )
        ),
        Record("different-multiple",
               Seq(
                 Nested("left", Map("a" -> 1)),
                 Nested("left", Map("a" -> 2))
               ))
      )
      val right = Seq(
        RecordB(
          "identical-multiple",
          Seq(
            NestedB("left-null"),
            NestedB("right-null"),
            NestedB("right-null"),
            NestedB("left", Map("d" -> 1, "dd" -> 2, "ddd" -> 3, "dddd" -> 4)),
            NestedB("right", Map("e" -> 1, "ee" -> 2, "eee" -> 3, "eeee" -> 4)),
            NestedB("right", Map("ee" -> 2, "eeee" -> 4, "e" -> 1, "eee" -> 3)),
            NestedB("right-only", Map("g" -> 1, "gg" -> 2, "ggg" -> 3, "gggg" -> 4)),
            NestedB("right-only", Map("gg" -> 2, "gggg" -> 4, "g" -> 1, "ggg" -> 3)),
            NestedB("different", value = 10f),
            NestedB("different", value = 10f)
          )
        ),
        RecordB("different-multiple",
                Seq(
                  NestedB("left", Map("a" -> 1))
                ))
      )
      DeepDiffTestCase
        .testReversedLeftAndRight()
        .testPruned()
        .conf( // language=HOCON
          """keys = ["key"]
            |exploded-arrays {
            |  "records": { 
            |     keys = ["key"]
            |     multiple-matches.omit-if-all-identical = true
            |  } 
            |}
            |""".stripMargin)
        .compare(left, right)
        .expect(DatasetDiffsBuilder[KeyExample, Product]()
          .kindOfDifferent(leftAndRightOnly = 1)
          .record(diffExamples = left.zip(right).map({ case (l, r) => (Key(l.key), l, r) }))
          .common("key", identical = 2)
          .common("records", diffExamples = left.zip(right).map({ case (l, r) => (Key(l.key), l.records, r.records) }))
          .explodedArray(
            "records",
            DatasetDiffsBuilder[KeyExample, Product]()
              .kindOfDifferent(rightOnly = 1)
              .multipleMatches(
                examples = Seq(
                  (Key("different-multiple") >> Key("left"),
                   Seq(
                     Nested("left", Map("a" -> 2)),
                     Nested("left", Map("a" -> 1))
                   ),
                   Seq(
                     NestedB("left", Map("a" -> 1))
                   ))
                ))
              .record(
                identical = 4,
                diffExamples = Seq(
                  (Key("identical-multiple") >> Key("different"),
                   Nested("different"),
                   NestedB("different", value = 10f))
                ),
                leftExamples = Seq(
                  (Key("identical-multiple") >> Key("left-only"),
                   Nested("left-only", Map("f" -> 1, "ff" -> 2, "fff" -> 3, "ffff" -> 4)))
                ),
                rightExamples = Seq(
                  (Key("identical-multiple") >> Key("right-only"),
                   NestedB("right-only", Map("g" -> 1, "gg" -> 2, "ggg" -> 3, "gggg" -> 4)))
                )
              )
              .common("key", identical = 5)
              .rightOnly("value",
                         identical = 4,
                         rightExamples = Seq((Key("identical-multiple") >> Key("different"), 10f)))
              .common("map", identical = 5)
          ))
    }
  }
}
