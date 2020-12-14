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

object MultipleMatchesSpec {
  final case class Record(key: String, count: Int)
  final case class RecordB(key: String, count: Int, size: Long)
}

final class MultipleMatchesSpec extends DeepDiffSpec {
  import DeepDiffSpec._
  import MultipleMatchesSpec._
  import com.criteo.deepdiff.test_utils.EasyKeyExample._
  "Deep Diff" should {
    "detect multiple matches" which {
      val left = Seq(
        Record("A", 1),
        Record("A", 2),
        Record("B", 3),
        Record("D", 10),
        Record("D", 10),
        Record("E", 11),
        Record("G", 20),
        Record("G", 20)
      )
      "have the same schemas" when runningDeepDiffTestCase {
        DeepDiffTestCase
          .defaultConf()
          .compare(
            left,
            right = Seq(
              Record("B", 4),
              Record("B", 5),
              Record("C", 6),
              Record("C", 7),
              Record("E", 11),
              Record("E", 11),
              Record("F", 12),
              Record("F", 12),
              Record("G", 200)
            )
          )
          .expect(
            DatasetDiffsBuilder[KeyExample, Product]()
              .multipleMatches(
                examples = Seq(
                  (Key("A"), Seq(Record("A", 1), Record("A", 2)), Nil),
                  (Key("D"), Seq(Record("D", 10), Record("D", 10)), Nil),
                  (Key("C"), Nil, Seq(Record("C", 6), Record("C", 7))),
                  (Key("F"), Nil, Seq(Record("F", 12), Record("F", 12))),
                  (Key("B"), Seq(Record("B", 3)), Seq(Record("B", 4), Record("B", 5))),
                  (Key("E"), Seq(Record("E", 11)), Seq(Record("E", 11), Record("E", 11))),
                  (Key("G"), Seq(Record("G", 20), Record("G", 20)), Seq(Record("G", 200)))
                )
              )
              .record()
              .common("key")
              .common("count")
          )
      }

      "have different schemas" when runningDeepDiffTestCase {
        DeepDiffTestCase
          .testReversedLeftAndRight()
          .defaultConf()
          .compare(
            left,
            right = Seq(
              RecordB("B", 4, 4L),
              RecordB("B", 5, 5L),
              RecordB("C", 6, 6L),
              RecordB("C", 7, 7L),
              RecordB("E", 11, 11L),
              RecordB("E", 11, 11L),
              RecordB("F", 12, 12L),
              RecordB("F", 12, 12L),
              RecordB("G", 200, 200L)
            )
          )
          .expect(
            DatasetDiffsBuilder[KeyExample, Product]()
              .multipleMatches(
                examples = Seq(
                  (Key("A"), Seq(Record("A", 1), Record("A", 2)), Nil),
                  (Key("D"), Seq(Record("D", 10), Record("D", 10)), Nil),
                  (Key("C"), Nil, Seq(RecordB("C", 6, 6L), RecordB("C", 7, 7L))),
                  (Key("F"), Nil, Seq(RecordB("F", 12, 12L), RecordB("F", 12, 12L))),
                  (Key("B"), Seq(Record("B", 3)), Seq(RecordB("B", 4, 4L), RecordB("B", 5, 5L))),
                  (Key("E"), Seq(Record("E", 11)), Seq(RecordB("E", 11, 11L), RecordB("E", 11, 11L))),
                  (Key("G"), Seq(Record("G", 20), Record("G", 20)), Seq(RecordB("G", 200, 200L)))
                )
              )
              .record()
              .common("key")
              .common("count")
              .rightOnly("size")
          )
      }
    }
  }
}
