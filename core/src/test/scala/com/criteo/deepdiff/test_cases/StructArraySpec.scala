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
import com.criteo.deepdiff.test_utils.{DatasetDiffsBuilder, DeepDiffSpec}

import scala.language.reflectiveCalls

object StructArraySpec {
  final case class KeyOnly(key: String)
  final case class Record(key: String, array: Seq[Nested])
  final case class Nested(key: String, value: Int)
  final case class RecordB(key: String, array: Seq[NestedB], second: java.lang.Long = null)
  final case class NestedB(key: String, value: Int, second: java.lang.Long = null)
}

/** Common test between struct arrays and exploded arrays */
final class StructArraySpec extends DeepDiffSpec {
  import DeepDiffSpec._
  import StructArraySpec._
  import com.criteo.deepdiff.test_utils.EasyKeyExample._

  val explodedConf = // language=HOCON
    """keys = ["key"]
      |exploded-arrays {
      |  array = { keys = ["key"] }
      |}
      |""".stripMargin

  "Deep Diff" should {
    "should treat absent arrays as null" when {
      val left = Seq(
        KeyOnly("identical"),
        KeyOnly("empty"),
        KeyOnly("nulls"),
        KeyOnly("struct"),
        KeyOnly("multi-struct")
      )
      val right = Seq(
        Record("identical", array = null),
        Record("empty", array = Seq.empty),
        Record("nulls", array = Seq(null)),
        Record("struct", array = Seq(Nested("a", 1))),
        Record("multi-struct", array = Seq(Nested("a", 2), Nested("b", 3)))
      )
      val builder = DatasetDiffsBuilder[KeyExample, Product]()
        .kindOfDifferent(rightOnly = 4)
        .record(
          identical = 1,
          diffExamples = left.tail
            .zip(right.tail)
            .map({
              case (l, r) => (Key(l.key), l, r)
            })
        )
        .common("key", identical = 5)
        .rightOnly(
          "array",
          identical = 1,
          rightExamples = Seq(
            (Key("empty"), Seq.empty),
            (Key("nulls"), Seq(null)),
            (Key("struct"), Seq(Nested("a", 1))),
            (Key("multi-struct"), Seq(Nested("a", 2), Nested("b", 3)))
          )
        )

      "NOT exploded" when runningDeepDiffTestCase {
        DeepDiffTestCase
          .testReversedLeftAndRight()
          .defaultConf()
          .compare(left, right)
          .expect(
            builder
              .rightOnly("array.key")
              .rightOnly("array.value")
              .rightOnly(
                "array (struct)",
                rightExamples = Seq(
                  (Key("struct") >> Key(0), Nested("a", 1)),
                  (Key("multi-struct") >> Key(0), Nested("a", 2)),
                  (Key("multi-struct") >> Key(1), Nested("b", 3))
                )
              ))
      }
      "exploded" when runningDeepDiffTestCase {
        DeepDiffTestCase
          .testReversedLeftAndRight()
          .conf(explodedConf)
          .compare(left, right)
          .expect(builder
            .explodedArray(
              "array",
              DatasetDiffsBuilder.rightOnlyRecordDiffs(
                fields = Set("key", "value"),
                rightExamples = Seq(
                  (Key("struct") >> Key("a"), Nested("a", 1)),
                  (Key("multi-struct") >> Key("a"), Nested("a", 2)),
                  (Key("multi-struct") >> Key("b"), Nested("b", 3))
                )
              )
            ))

      }
    }

    "be able to compare arrays" which {
      val left = Seq(
        Record("identical-null", array = null),
        Record("identical", array = Seq(Nested("a", value = 1), Nested("b", value = 2), null)),
        Record("different", array = Seq(Nested("a", value = 3), Nested("b", value = 4))),
        Record("more-left", array = Seq(Nested("a", value = 5), Nested("b", value = 6))),
        Record("more-right", array = Seq(Nested("a", value = 7))),
        Record("left-only", array = Seq(Nested("a", value = 9))),
        Record("right-only", array = null)
      )

      def leftBaseBuilder(right: Seq[Product { def key: String; def array: Seq[Product] }]) = {
        val l = left.map(x => x.key -> x).toMap
        val r = right.map(x => x.key -> x).toMap
        DatasetDiffsBuilder[KeyExample, Product]()
          .kindOfDifferent(content = 1, leftOnly = 2, rightOnly = 2)
          .record(
            identical = 2,
            diffExamples = Seq(
              (Key("different"), l("different"), r("different")),
              (Key("more-left"), l("more-left"), r("more-left")),
              (Key("more-right"), l("more-right"), r("more-right")),
              (Key("left-only"), l("left-only"), r("left-only")),
              (Key("right-only"), l("right-only"), r("right-only"))
            )
          )
          .common("key", identical = 7)
          .common(
            "array",
            identical = 2,
            diffExamples = Seq(
              (Key("different"), l("different").array, r("different").array),
              (Key("more-left"), l("more-left").array, r("more-left").array),
              (Key("more-right"), l("more-right").array, r("more-right").array)
            ),
            leftExamples = Seq((Key("left-only"), l("left-only").array)),
            rightExamples = Seq((Key("right-only"), r("right-only").array))
          )
      }

      "have the same schema" when {
        val right = Seq(
          Record("identical-null", array = null),
          Record("identical", array = Seq(Nested("a", value = 1), Nested("b", value = 2), null)),
          Record(
            "different",
            array = Seq(
              Nested("a", value = 1024), // <- different
              Nested("b", value = 4)
            )
          ),
          Record(
            "more-left",
            array = Seq(
              Nested("a", value = 5)
              // <- missing 6
            )
          ),
          Record(
            "more-right",
            array = Seq(
              Nested("a", value = 7),
              Nested("b", value = 8) // <- added 8
            )
          ),
          Record("left-only", array = null),
          Record("right-only", array = Seq(Nested("a", value = 10)))
        )
        val builder = leftBaseBuilder(right)

        "NOT exploded" when runningDeepDiffTestCase {
          DeepDiffTestCase
            .defaultConf()
            .compare(left, right)
            .expect(
              builder
                .common(
                  "array (struct)",
                  identical = 6,
                  diffExamples = Seq((Key("different") >> Key(0), Nested("a", 3), Nested("a", 1024))),
                  leftExamples = Seq((Key("more-left") >> Key(1), Nested("b", 6))),
                  rightExamples = Seq((Key("more-right") >> Key(1), Nested("b", 8)))
                )
                .common("array.key", identical = 6)
                .common("array.value", identical = 5, diffExamples = Seq((Key("different") >> Key(0), 3, 1024)))
            )
        }

        "exploded" when runningDeepDiffTestCase {
          DeepDiffTestCase
            .conf(explodedConf)
            .compare(left, right)
            .expect(
              builder
                .explodedArray(
                  "array",
                  DatasetDiffsBuilder[KeyExample, Product]()
                    .record(
                      identical = 5, // null struct is not counted
                      diffExamples = Seq((Key("different") >> Key("a"), Nested("a", 3), Nested("a", 1024))),
                      leftExamples = Seq((Key("more-left") >> Key("b"), Nested("b", 6))),
                      rightExamples = Seq((Key("more-right") >> Key("b"), Nested("b", 8)))
                    )
                    .kindOfDifferent(content = 1)
                    .common("key", identical = 6)
                    .common("value", identical = 5, diffExamples = Seq((Key("different") >> Key("a"), 3, 1024)))
                )
            )
        }
      }

      "have different schemas" when {
        val right = Seq(
          RecordB("identical-null", array = null),
          RecordB("identical", array = Seq(NestedB("a", value = 1), NestedB("b", value = 2), null)),
          RecordB(
            "different",
            array = Seq(
              NestedB("a", value = 1024), // <- different
              NestedB("b", value = 4)
            )
          ),
          RecordB(
            "more-left",
            array = Seq(
              NestedB("a", value = 5)
              // <- missing 6
            )
          ),
          RecordB(
            "more-right",
            array = Seq(
              NestedB("a", value = 7),
              NestedB("b", value = 8) // <- added 8
            )
          ),
          RecordB("left-only", array = null),
          RecordB("right-only", array = Seq(NestedB("a", value = 10)))
        )
        val builder = leftBaseBuilder(right)
          .rightOnly("second", identical = 7)

        "NOT exploded" when runningDeepDiffTestCase {
          DeepDiffTestCase
            .testReversedLeftAndRight()
            .defaultConf()
            .compare(left, right)
            .expect(builder
              .common(
                "array (struct)",
                identical = 6,
                diffExamples = Seq((Key("different") >> Key(0), Nested("a", 3), NestedB("a", 1024))),
                leftExamples = Seq((Key("more-left") >> Key(1), Nested("b", 6))),
                rightExamples = Seq((Key("more-right") >> Key(1), NestedB("b", 8)))
              )
              .rightOnly("array.second", identical = 6)
              .common("array.key", identical = 6)
              .common("array.value", identical = 5, diffExamples = Seq((Key("different") >> Key(0), 3, 1024))))
        }
        "exploded" when runningDeepDiffTestCase {
          DeepDiffTestCase
            .testReversedLeftAndRight()
            .conf(explodedConf)
            .compare(left, right)
            .expect(builder
              .explodedArray(
                "array",
                DatasetDiffsBuilder[KeyExample, Product]()
                  .record(
                    identical = 5, // null struct is not counted
                    diffExamples = Seq((Key("different") >> Key("a"), Nested("a", 3), NestedB("a", 1024))),
                    leftExamples = Seq((Key("more-left") >> Key("b"), Nested("b", 6))),
                    rightExamples = Seq((Key("more-right") >> Key("b"), NestedB("b", 8)))
                  )
                  .kindOfDifferent(content = 1)
                  .rightOnly("second", identical = 6)
                  .common("key", identical = 6)
                  .common("value", identical = 5, diffExamples = Seq((Key("different") >> Key("a"), 3, 1024)))
              ))
        }
      }
    }
  }
}
