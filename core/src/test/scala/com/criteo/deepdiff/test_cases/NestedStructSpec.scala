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

object NestedStructSpec {
  final case class KeyOnly(key: String)
  final case class Record(key: String, nested: Nested)
  final case class Nested(name: String, value: java.lang.Integer)
  final case class RecordB(key: String, nested: NestedB)
  final case class NestedB(name: String, value: Int, second: java.lang.Long = null)
}

final class NestedStructSpec extends DeepDiffSpec {
  import DeepDiffSpec._
  import NestedStructSpec._
  import com.criteo.deepdiff.test_utils.EasyKeyExample._

  "Deep Diff" should {
    "be able to compare nested structs" which {
      "have the same schema" when runningDeepDiffTestCase {
        DeepDiffTestCase
          .defaultConf()
          .compare(
            left = Seq(
              Record("identical-null", null),
              Record("identical", Nested("1", 1)),
              Record("different", Nested("2", 2)),
              Record("left-only", Nested("3", 3)),
              Record("right-only", null)
            ),
            right = Seq(
              Record("identical-null", null),
              Record("identical", Nested("1", 1)),
              Record("different", Nested("2", 1024)),
              Record("left-only", null),
              Record("right-only", Nested("4", 4))
            )
          )
          .expect(
            DatasetDiffsBuilder[KeyExample, Product]()
              .kindOfDifferent(content = 1, leftOnly = 1, rightOnly = 1)
              .record(
                identical = 2,
                diffExamples = Seq(
                  (Key("different"), Record("different", Nested("2", 2)), Record("different", Nested("2", 1024))),
                  (Key("left-only"), Record("left-only", Nested("3", 3)), Record("left-only", null)),
                  (Key("right-only"), Record("right-only", null), Record("right-only", Nested("4", 4)))
                )
              )
              .common("key", identical = 5)
              .common(
                "nested",
                identical = 2,
                diffExamples = Seq((Key("different"), Nested("2", 2), Nested("2", 1024))),
                leftExamples = Seq((Key("left-only"), Nested("3", 3))),
                rightExamples = Seq((Key("right-only"), Nested("4", 4)))
              )
              .common("nested.name", identical = 2)
              .common("nested.value", identical = 1, diffExamples = Seq((Key("different"), 2, 1024)))
          )
      }
      "have different schemas" when runningDeepDiffTestCase {
        DeepDiffTestCase
          .testReversedLeftAndRight()
          .defaultConf()
          .compare(
            left = Seq(
              Record("identical-null", null),
              Record("identical", Nested("1", 1)),
              Record("different", Nested("2", 2)),
              Record("left-only", Nested("3", 3)),
              Record("right-only", null),
              Record("with-second", Nested("5", 5))
            ),
            right = Seq(
              RecordB("identical-null", null),
              RecordB("identical", NestedB("1", 1)),
              RecordB("different", NestedB("2", 1024)),
              RecordB("left-only", null),
              RecordB("right-only", NestedB("4", 4)),
              RecordB("with-second", NestedB("5", 5, second = 5L))
            )
          )
          .expect(
            DatasetDiffsBuilder[KeyExample, Product]()
              .kindOfDifferent(content = 1, leftOnly = 1, rightOnly = 2)
              .record(
                identical = 2,
                diffExamples = Seq(
                  (Key("different"), Record("different", Nested("2", 2)), RecordB("different", NestedB("2", 1024))),
                  (Key("left-only"), Record("left-only", Nested("3", 3)), RecordB("left-only", null)),
                  (Key("right-only"), Record("right-only", null), RecordB("right-only", NestedB("4", 4))),
                  (
                    Key("with-second"),
                    Record("with-second", Nested("5", 5)),
                    RecordB("with-second", NestedB("5", 5, second = 5L))
                  )
                )
              )
              .common("key", identical = 6)
              .common(
                "nested",
                identical = 2,
                diffExamples = Seq(
                  (Key("different"), Nested("2", 2), NestedB("2", 1024)),
                  (Key("with-second"), Nested("5", 5), NestedB("5", 5, second = 5L))
                ),
                leftExamples = Seq((Key("left-only"), Nested("3", 3))),
                rightExamples = Seq((Key("right-only"), NestedB("4", 4)))
              )
              .common("nested.name", identical = 3)
              .common("nested.value", identical = 2, diffExamples = Seq((Key("different"), 2, 1024)))
              .rightOnly("nested.second", identical = 2, rightExamples = Seq((Key("with-second"), 5L))))

      }
    }

    "treat absent struct as null, but not as struct of nulls" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .testReversedLeftAndRight()
        .defaultConf()
        .compare(
          left = Seq(
            KeyOnly("identical"),
            KeyOnly("right-only"),
            KeyOnly("right-only-2")
          ),
          right = Seq(
            Record("identical", null),
            Record("right-only", Nested("1", 1)),
            Record("right-only-2", Nested(null, null))
          )
        )
        .expect(
          DatasetDiffsBuilder[KeyExample, Product]()
            .kindOfDifferent(rightOnly = 2)
            .record(
              identical = 1,
              diffExamples = Seq(
                (Key("right-only"), KeyOnly("right-only"), Record("right-only", Nested("1", 1))),
                (Key("right-only-2"), KeyOnly("right-only-2"), Record("right-only-2", Nested(null, null)))
              )
            )
            .common("key", identical = 3)
            .rightOnly(
              "nested",
              identical = 1,
              rightExamples = Seq(
                (Key("right-only"), Nested("1", 1)),
                (Key("right-only-2"), Nested(null, null))
              )
            )
            .rightOnly("nested.name")
            .rightOnly("nested.value"))
    }
  }
}
