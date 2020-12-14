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

object ArraySpec {
  final case class Record(
      key: String,
      user: Seq[String] = null,
      shorty: Seq[java.lang.Short] = null,
      count: Seq[java.lang.Integer] = null,
      size: Seq[java.lang.Long] = null,
      value: Seq[java.lang.Float] = null,
      total: Seq[java.lang.Double] = null,
      little: Seq[java.lang.Byte] = null,
      big: Seq[Array[Byte]] = null,
      done: Seq[java.lang.Boolean] = null
  )

  object Record {
    def dummy(key: String): Record = Record(
      key,
      user = Seq("1"),
      shorty = Seq(2.toShort),
      count = Seq(3),
      size = Seq(4L),
      value = Seq(5f),
      total = Seq(6d),
      little = Seq(7.toByte),
      big = Seq(Array[Byte](8.toByte)),
      done = Seq(true)
    )
  }
}

final class ArraySpec extends DeepDiffSpec {
  import ArraySpec._
  import DeepDiffSpec._
  import com.criteo.deepdiff.test_utils.EasyKeyExample._

  "Deep Diff" should {
    "support null arrays" when runningDeepDiffTestCase {
      val left = Seq(
        Record("all-null"),
        Record.dummy("null-user"),
        Record.dummy("null-shorty"),
        Record.dummy("null-count"),
        Record.dummy("null-size"),
        Record.dummy("null-value"),
        Record.dummy("null-total"),
        Record.dummy("null-little"),
        Record.dummy("null-big"),
        Record.dummy("null-done")
      )
      val right = Seq(
        Record("all-null"),
        Record.dummy("null-user").copy(user = null),
        Record.dummy("null-shorty").copy(shorty = null),
        Record.dummy("null-count").copy(count = null),
        Record.dummy("null-size").copy(size = null),
        Record.dummy("null-value").copy(value = null),
        Record.dummy("null-total").copy(total = null),
        Record.dummy("null-little").copy(little = null),
        Record.dummy("null-big").copy(big = null),
        Record.dummy("null-done").copy(done = null)
      )
      DeepDiffTestCase
        .testReversedLeftAndRight()
        .defaultConf()
        .compare(left, right)
        .expect(
          DatasetDiffsBuilder[KeyExample, Product]()
            .kindOfDifferent(leftOnly = 9)
            .record(
              identical = 1,
              diffExamples = left.drop(1).zip(right.drop(1)).map({ case (l, r) => (Key(l.key), l, r) })
            )
            .common("key", identical = 10)
            .common("user", identical = 9, leftExamples = Seq((Key("null-user"), Seq("1"))))
            .common("shorty", identical = 9, leftExamples = Seq((Key("null-shorty"), Seq(2.toShort))))
            .common("count", identical = 9, leftExamples = Seq((Key("null-count"), Seq(3))))
            .common("size", identical = 9, leftExamples = Seq((Key("null-size"), Seq(4L))))
            .common("value", identical = 9, leftExamples = Seq((Key("null-value"), Seq(5f))))
            .common("total", identical = 9, leftExamples = Seq((Key("null-total"), Seq(6d))))
            .common("little", identical = 9, leftExamples = Seq((Key("null-little"), Seq(7.toByte))))
            .common("big", identical = 9, leftExamples = Seq((Key("null-big"), Seq(Array[Byte](8.toByte)))))
            .common("done", identical = 9, leftExamples = Seq((Key("null-done"), Seq(true)))))
    }
    "support any element type in arrays" when runningDeepDiffTestCase {
      val left = Seq(
        Record.dummy(key = "identical"),
        Record.dummy("different-user"),
        Record.dummy("different-shorty"),
        Record.dummy("different-count"),
        Record.dummy("different-size"),
        Record.dummy("different-value"),
        Record.dummy("different-total"),
        Record.dummy("different-little"),
        Record.dummy("different-big"),
        Record.dummy("different-done"),
        Record.dummy("null-user"),
        Record.dummy("null-shorty"),
        Record.dummy("null-count"),
        Record.dummy("null-size"),
        Record.dummy("null-value"),
        Record.dummy("null-total"),
        Record.dummy("null-little"),
        Record.dummy("null-big"),
        Record.dummy("null-done")
      )
      val right = Seq(
        Record.dummy(key = "identical"),
        Record.dummy("different-user").copy(user = Seq("11")),
        Record.dummy("different-shorty").copy(shorty = Seq(22.toShort)),
        Record.dummy("different-count").copy(count = Seq(33)),
        Record.dummy("different-size").copy(size = Seq(44L)),
        Record.dummy("different-value").copy(value = Seq(55f)),
        Record.dummy("different-total").copy(total = Seq(66d)),
        Record.dummy("different-little").copy(little = Seq(77.toByte)),
        Record.dummy("different-big").copy(big = Seq(Array[Byte](88.toByte))),
        Record.dummy("different-done").copy(done = Seq(false)),
        Record.dummy("null-user").copy(user = Seq(null)),
        Record.dummy("null-shorty").copy(shorty = Seq(null)),
        Record.dummy("null-count").copy(count = Seq(null)),
        Record.dummy("null-size").copy(size = Seq(null)),
        Record.dummy("null-value").copy(value = Seq(null)),
        Record.dummy("null-total").copy(total = Seq(null)),
        Record.dummy("null-little").copy(little = Seq(null)),
        Record.dummy("null-big").copy(big = Seq(null)),
        Record.dummy("null-done").copy(done = Seq(null))
      )

      DeepDiffTestCase
        .defaultConf()
        .compare(left, right)
        .expect(
          DatasetDiffsBuilder[KeyExample, Product]()
            .kindOfDifferent(content = 18)
            .record(
              identical = 1,
              diffExamples = left.drop(1).zip(right.drop(1)).map({ case (l, r) => (Key(l.key), l, r) })
            )
            .common("key", identical = 19)
            .common(
              "user",
              identical = 17,
              diffExamples = Seq((Key("different-user"), Seq("1"), Seq("11")), (Key("null-user"), Seq("1"), Seq(null)))
            )
            .common(
              "shorty",
              identical = 17,
              diffExamples = Seq(
                (Key("different-shorty"), Seq(2.toShort), Seq(22.toShort)),
                (Key("null-shorty"), Seq(2.toShort), Seq(null))
              )
            )
            .common(
              "count",
              identical = 17,
              diffExamples = Seq((Key("different-count"), Seq(3), Seq(33)), (Key("null-count"), Seq(3), Seq(null)))
            )
            .common(
              "size",
              identical = 17,
              diffExamples = Seq((Key("different-size"), Seq(4L), Seq(44L)), (Key("null-size"), Seq(4L), Seq(null)))
            )
            .common(
              "value",
              identical = 17,
              diffExamples = Seq((Key("different-value"), Seq(5f), Seq(55f)), (Key("null-value"), Seq(5f), Seq(null)))
            )
            .common(
              "total",
              identical = 17,
              diffExamples = Seq((Key("different-total"), Seq(6d), Seq(66d)), (Key("null-total"), Seq(6d), Seq(null)))
            )
            .common(
              "little",
              identical = 17,
              diffExamples = Seq(
                (Key("different-little"), Seq(7.toByte), Seq(77.toByte)),
                (Key("null-little"), Seq(7.toByte), Seq(null))
              )
            )
            .common(
              "big",
              identical = 17,
              diffExamples = Seq(
                (Key("different-big"), Seq(Array[Byte](8.toByte)), Seq(Array[Byte](88.toByte))),
                (Key("null-big"), Seq(Array[Byte](8.toByte)), Seq(null))
              )
            )
            .common(
              "done",
              identical = 17,
              diffExamples =
                Seq((Key("different-done"), Seq(true), Seq(false)), (Key("null-done"), Seq(true), Seq(null)))
            )
        )
    }

    "detect size changes in arrays" when runningDeepDiffTestCase {
      val identical = Record(
        key = "identical",
        user = Seq("1", "1"),
        shorty = Seq(2.toShort, 2.toShort),
        count = Seq(3, 3),
        size = Seq(4L, 4L),
        value = Seq(5f, 5f),
        total = Seq(6d, 6d),
        little = Seq(7.toByte, 7.toByte),
        big = Seq(Array[Byte](8.toByte), Array[Byte](8.toByte)),
        done = Seq(true, true)
      )

      val left = Seq(
        identical,
        Record("less-elements", user = Seq("1"), count = Seq(3, 3, 3), total = Seq(6d, 6d), big = Seq(null)),
        Record("more-elements", shorty = Seq(2.toShort, 2.toShort), little = Seq.empty, big = Seq(null))
      )
      val right = Seq(
        identical,
        Record("less-elements", user = Seq.empty, count = Seq(3, 3), total = Seq(6d), big = Seq.empty),
        Record(
          "more-elements",
          shorty = Seq(2.toShort, 2.toShort, 2.toShort),
          little = Seq(7.toByte),
          big = Seq(null, null)
        )
      )
      DeepDiffTestCase
        .defaultConf()
        .compare(left, right)
        .expect(
          DatasetDiffsBuilder[KeyExample, Product]()
            .kindOfDifferent(content = 2)
            .record(
              identical = 1,
              diffExamples = left.drop(1).zip(right.drop(1)).map({ case (l, r) => (Key(l.key), l, r) })
            )
            .common("key", identical = 3)
            .common("user", identical = 2, diffExamples = Seq((Key("less-elements"), Seq("1"), Seq.empty)))
            .common(
              "shorty",
              identical = 2,
              diffExamples =
                Seq((Key("more-elements"), Seq(2.toShort, 2.toShort), Seq(2.toShort, 2.toShort, 2.toShort)))
            )
            .common("count", identical = 2, diffExamples = Seq((Key("less-elements"), Seq(3, 3, 3), Seq(3, 3))))
            .common("size", identical = 3)
            .common("value", identical = 3)
            .common("total", identical = 2, diffExamples = Seq((Key("less-elements"), Seq(6d, 6d), Seq(6d))))
            .common("little", identical = 2, diffExamples = Seq((Key("more-elements"), Seq.empty, Seq(7.toByte))))
            .common(
              "big",
              identical = 1,
              diffExamples = Seq(
                (Key("less-elements"), Seq(null), Seq.empty),
                (Key("more-elements"), Seq(null), Seq(null, null))
              )
            )
            .common("done", identical = 3)
        )
    }

    "be order sensitive for arrays" when runningDeepDiffTestCase {
      val identical = Record(
        key = "identical",
        user = Seq("1", null, "11"),
        shorty = Seq(2.toShort, null, 22.toShort),
        count = Seq(3, null, 33),
        size = Seq(4L, null, 44L),
        value = Seq(5f, null, 55f),
        total = Seq(6d, null, 66d),
        little = Seq(7.toByte, null, 77.toByte),
        big = Seq(Array[Byte](8.toByte), null, Array[Byte](88.toByte)),
        done = Seq(true, null, false)
      )

      val left = Seq(
        identical,
        identical.copy(key = "different")
      )
      val right = Seq(
        identical,
        Record(
          key = "different",
          user = Seq("1", "11", null),
          shorty = Seq(2.toShort, 22.toShort, null),
          count = Seq(3, 33, null),
          size = Seq(4L, 44L, null),
          value = Seq(5f, 55f, null),
          total = Seq(6d, 66d, null),
          little = Seq(7.toByte, 77.toByte, null),
          big = Seq(Array[Byte](8.toByte), Array[Byte](88.toByte), null),
          done = Seq(true, false, null)
        )
      )
      DeepDiffTestCase
        .defaultConf()
        .compare(left, right)
        .expect(
          DatasetDiffsBuilder[KeyExample, Product]()
            .kindOfDifferent(content = 1)
            .record(
              identical = 1,
              diffExamples = left.drop(1).zip(right.drop(1)).map({ case (l, r) => (Key(l.key), l, r) })
            )
            .common("key", identical = 2)
            .common(
              "user",
              identical = 1,
              diffExamples = Seq((Key("different"), Seq("1", null, "11"), Seq("1", "11", null)))
            )
            .common(
              "shorty",
              identical = 1,
              diffExamples = Seq((Key("different"), Seq(2.toShort, null, 22.toShort), Seq(2.toShort, 22.toShort, null)))
            )
            .common(
              "count",
              identical = 1,
              diffExamples = Seq((Key("different"), Seq(3, null, 33), Seq(3, 33, null)))
            )
            .common(
              "size",
              identical = 1,
              diffExamples = Seq((Key("different"), Seq(4L, null, 44L), Seq(4L, 44L, null)))
            )
            .common(
              "value",
              identical = 1,
              diffExamples = Seq((Key("different"), Seq(5f, null, 55f), Seq(5f, 55f, null)))
            )
            .common(
              "total",
              identical = 1,
              diffExamples = Seq((Key("different"), Seq(6d, null, 66d), Seq(6d, 66d, null)))
            )
            .common(
              "little",
              identical = 1,
              diffExamples = Seq((Key("different"), Seq(7.toByte, null, 77.toByte), Seq(7.toByte, 77.toByte, null)))
            )
            .common(
              "big",
              identical = 1,
              diffExamples = Seq(
                (
                  Key("different"),
                  Seq(Array[Byte](8.toByte), null, Array[Byte](88.toByte)),
                  Seq(Array[Byte](8.toByte), Array[Byte](88.toByte), null)
                )
              )
            )
            .common(
              "done",
              identical = 1,
              diffExamples = Seq((Key("different"), Seq(true, null, false), Seq(true, false, null)))
            )
        )
    }
  }
}
