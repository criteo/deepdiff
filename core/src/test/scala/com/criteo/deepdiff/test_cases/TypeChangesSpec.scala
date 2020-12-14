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

object TypeChangesSpec {
  final case class Record(key: String, count: java.lang.Integer = null)
  final case class DiffRecord(key: String, count: String = null)
}
final class TypeChangesSpec extends DeepDiffSpec {
  import DeepDiffSpec._
  import TypeChangesSpec._
  import com.criteo.deepdiff.test_utils.EasyKeyExample._

  "Deep Diff" should {
    "detect type changes of columns" when runningDeepDiffTestCase {
      val left = Seq(
        Record("null"),
        Record("different", 1),
        Record("left", 2),
        Record("right")
      )
      val right = Seq(
        DiffRecord("null"),
        DiffRecord("different", "1"),
        DiffRecord("left"),
        DiffRecord("right", "3")
      )
      DeepDiffTestCase
        .defaultConf()
        .compare(left, right)
        .expect(
          DatasetDiffsBuilder[KeyExample, Product]()
            .kindOfDifferent(leftOnly = 1, rightOnly = 1, leftAndRightOnly = 1)
            .record(
              identical = 1,
              diffExamples = left.drop(1).zip(right.drop(1)).map({ case (l, r) => (Key(l.key), l, r) })
            )
            .common("key", identical = 4)
            .leftOnly(
              "count (int)",
              identical = 2,
              leftExamples = Seq(
                (Key("different"), 1),
                (Key("left"), 2)
              )
            )
            .rightOnly(
              "count (string)",
              identical = 2,
              rightExamples = Seq(
                (Key("different"), "1"),
                (Key("right"), "3")
              )
            )
        )
    }
  }
}
