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

object BaseSpec {
  final case class KeyOnly(key: String)
  object Record {
    def dummy(key: String): Record =
      Record(
        key,
        user = "1",
        shorty = 2.toShort,
        count = 3,
        size = 4L,
        value = 5f,
        total = 6d,
        little = 7.toByte,
        big = Array[Byte](8.toByte),
        done = true
      )
  }
  final case class Record(
      key: String,
      user: String = null,
      shorty: java.lang.Short = null,
      count: java.lang.Integer = null,
      size: java.lang.Long = null,
      value: java.lang.Float = null,
      total: java.lang.Double = null,
      little: java.lang.Byte = null,
      big: Array[Byte] = null,
      done: java.lang.Boolean = null
  )
}

final class BaseSpec extends DeepDiffSpec {
  import BaseSpec._
  import DeepDiffSpec._
  import com.criteo.deepdiff.test_utils.EasyKeyExample._

  "Deep Diff" should {

    "detect differences on each individual field" when runningDeepDiffTestCase {
      val left = Seq(
        Record("identical-null"),
        Record.dummy("identical"),
        Record.dummy("different-user"),
        Record.dummy("different-shorty"),
        Record.dummy("different-count"),
        Record.dummy("different-size"),
        Record.dummy("different-value"),
        Record.dummy("different-total"),
        Record.dummy("different-little"),
        Record.dummy("different-big"),
        Record.dummy("different-done"),
        Record("not-null-user"),
        Record("not-null-shorty"),
        Record("not-null-count"),
        Record("not-null-size"),
        Record("not-null-value"),
        Record("not-null-total"),
        Record("not-null-little"),
        Record("not-null-big"),
        Record("not-null-done"),
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
        Record("identical-null"),
        Record.dummy("identical"),
        Record.dummy("different-user").copy(user = "10"),
        Record.dummy("different-shorty").copy(shorty = 11.toShort),
        Record.dummy("different-count").copy(count = 12),
        Record.dummy("different-size").copy(size = 13L),
        Record.dummy("different-value").copy(value = 14f),
        Record.dummy("different-total").copy(total = 15d),
        Record.dummy("different-little").copy(little = 16.toByte),
        Record.dummy("different-big").copy(big = Array[Byte](17.toByte)),
        Record.dummy("different-done").copy(done = false),
        Record("not-null-user", user = "11"),
        Record("not-null-shorty", shorty = 22.toShort),
        Record("not-null-count", count = 33),
        Record("not-null-size", size = 44L),
        Record("not-null-value", value = 55f),
        Record("not-null-total", total = 66d),
        Record("not-null-little", little = 77.toByte),
        Record("not-null-big", big = Array[Byte](88.toByte)),
        Record("not-null-done", done = false),
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
            .kindOfDifferent(content = 9, leftOnly = 9, rightOnly = 9)
            .record(
              identical = 2,
              diffExamples = left.drop(2).zip(right.drop(2)).map({ case (l, r) => (Key(l.key), l, r) })
            )
            .common("key", identical = 29)
            .common(
              "user",
              identical = 26,
              diffExamples = Seq((Key("different-user"), "1", "10")),
              rightExamples = Seq((Key("not-null-user"), "11")),
              leftExamples = Seq((Key("null-user"), "1"))
            )
            .common(
              "shorty",
              identical = 26,
              diffExamples = Seq((Key("different-shorty"), 2.toShort, 11.toShort)),
              rightExamples = Seq((Key("not-null-shorty"), 22.toShort)),
              leftExamples = Seq((Key("null-shorty"), 2.toShort))
            )
            .common(
              "count",
              identical = 26,
              diffExamples = Seq((Key("different-count"), 3, 12)),
              rightExamples = Seq((Key("not-null-count"), 33)),
              leftExamples = Seq((Key("null-count"), 3))
            )
            .common(
              "size",
              identical = 26,
              diffExamples = Seq((Key("different-size"), 4L, 13L)),
              rightExamples = Seq((Key("not-null-size"), 44L)),
              leftExamples = Seq((Key("null-size"), 4L))
            )
            .common(
              "value",
              identical = 26,
              diffExamples = Seq((Key("different-value"), 5f, 14f)),
              rightExamples = Seq((Key("not-null-value"), 55f)),
              leftExamples = Seq((Key("null-value"), 5f))
            )
            .common(
              "total",
              identical = 26,
              diffExamples = Seq((Key("different-total"), 6d, 15d)),
              rightExamples = Seq((Key("not-null-total"), 66d)),
              leftExamples = Seq((Key("null-total"), 6d))
            )
            .common(
              "little",
              identical = 26,
              diffExamples = Seq((Key("different-little"), 7.toByte, 16.toByte)),
              rightExamples = Seq((Key("not-null-little"), 77.toByte)),
              leftExamples = Seq((Key("null-little"), 7.toByte))
            )
            .common(
              "big",
              identical = 26,
              diffExamples = Seq((Key("different-big"), Array[Byte](8.toByte), Array[Byte](17.toByte))),
              rightExamples = Seq((Key("not-null-big"), Array[Byte](88.toByte))),
              leftExamples = Seq((Key("null-big"), Array[Byte](8.toByte)))
            )
            .common(
              "done",
              identical = 26,
              diffExamples = Seq((Key("different-done"), true, false)),
              rightExamples = Seq((Key("not-null-done"), false)),
              leftExamples = Seq((Key("null-done"), true))
            )
        )
    }

    "record the type of differences" when runningDeepDiffTestCase {
      val left = Seq(
        Record("left-and-right-only", user = "1"),
        Record("left-only", count = 2),
        Record("left-only-2", value = 3f),
        Record("right-only"),
        Record("right-only-2"),
        Record("right-only-3"),
        Record("content", size = 7L),
        Record("content-2", value = 8f),
        Record("content-3", total = 9d),
        Record("content-4", user = "10")
      )
      val right = Seq(
        Record("left-and-right-only", count = 1),
        Record("left-only"),
        Record("left-only-2"),
        Record("right-only", total = 4d),
        Record("right-only-2", shorty = 5.toShort),
        Record("right-only-3", done = false),
        Record("content", size = 77L),
        Record("content-2", value = 88f),
        Record("content-3", total = 99d),
        Record("content-4", user = "1010")
      )
      DeepDiffTestCase
        .testReversedLeftAndRight()
        .defaultConf()
        .compare(left, right)
        .expect(
          DatasetDiffsBuilder[KeyExample, Product]()
            .kindOfDifferent(content = 4, rightOnly = 3, leftOnly = 2, leftAndRightOnly = 1)
            .record(diffExamples = left.zip(right).map({ case (l, r) => (Key(l.key), l, r) }))
            .common("key", identical = 10)
            .common(
              "user",
              identical = 8,
              diffExamples = Seq((Key("content-4"), "10", "1010")),
              leftExamples = Seq((Key("left-and-right-only"), "1"))
            )
            .common("shorty", identical = 9, rightExamples = Seq((Key("right-only-2"), 5.toShort)))
            .common(
              "count",
              identical = 8,
              leftExamples = Seq((Key("left-only"), 2)),
              rightExamples = Seq((Key("left-and-right-only"), 1))
            )
            .common("size", identical = 9, diffExamples = Seq((Key("content"), 7L, 77L)))
            .common(
              "value",
              identical = 8,
              diffExamples = Seq((Key("content-2"), 8f, 88f)),
              leftExamples = Seq((Key("left-only-2"), 3f))
            )
            .common(
              "total",
              identical = 8,
              diffExamples = Seq((Key("content-3"), 9d, 99d)),
              rightExamples = Seq((Key("right-only"), 4d))
            )
            .common("little", identical = 10)
            .common("big", identical = 10)
            .common("done", identical = 9, rightExamples = Seq((Key("right-only-3"), false)))
        )
    }

    "detect multiple differences on different fields" when runningDeepDiffTestCase {
      val left = Seq(
        Record("A"),
        Record.dummy("B")
      )
      val right = Seq(
        Record.dummy("A"),
        Record(
          key = "B",
          user = "11",
          shorty = 22.toShort,
          count = 33,
          size = 44L,
          value = 55f,
          total = 66d,
          little = 77.toByte,
          big = Array[Byte](88.toByte),
          done = false
        )
      )
      DeepDiffTestCase
        .testReversedLeftAndRight()
        .defaultConf()
        .compare(left, right)
        .expect(
          DatasetDiffsBuilder[KeyExample, Product]()
            .kindOfDifferent(content = 1, rightOnly = 1)
            .record(diffExamples = left.zip(right).map({ case (l, r) => (Key(l.key), l, r) }))
            .common("key", identical = 2)
            .common("user", diffExamples = Seq((Key("B"), "1", "11")), rightExamples = Seq((Key("A"), "1")))
            .common(
              "shorty",
              diffExamples = Seq((Key("B"), 2.toShort, 22.toShort)),
              rightExamples = Seq((Key("A"), 2.toShort))
            )
            .common("count", diffExamples = Seq((Key("B"), 3, 33)), rightExamples = Seq((Key("A"), 3)))
            .common("size", diffExamples = Seq((Key("B"), 4L, 44L)), rightExamples = Seq((Key("A"), 4L)))
            .common("value", diffExamples = Seq((Key("B"), 5f, 55f)), rightExamples = Seq((Key("A"), 5f)))
            .common("total", diffExamples = Seq((Key("B"), 6d, 66d)), rightExamples = Seq((Key("A"), 6d)))
            .common(
              "little",
              diffExamples = Seq((Key("B"), 7.toByte, 77.toByte)),
              rightExamples = Seq((Key("A"), 7.toByte))
            )
            .common(
              "big",
              diffExamples = Seq((Key("B"), Array[Byte](8.toByte), Array[Byte](88.toByte))),
              rightExamples = Seq((Key("A"), Array[Byte](8.toByte)))
            )
            .common("done", diffExamples = Seq((Key("B"), true, false)), rightExamples = Seq((Key("A"), true)))
        )
    }

    "should detect left and right only records" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .defaultConf()
        .compare(left = Seq(Record("A")), right = Seq(Record("B")))
        .expect(
          DatasetDiffsBuilder[KeyExample, Product]()
            .record(leftExamples = Seq((Key("A"), Record("A"))), rightExamples = Seq((Key("B"), Record("B"))))
            .common("key")
            .common("user")
            .common("shorty")
            .common("count")
            .common("size")
            .common("value")
            .common("total")
            .common("little")
            .common("big")
            .common("done")
        )
    }

    "treat absent fields as nulls" when runningDeepDiffTestCase {
      val left = Seq(
        KeyOnly("identical"),
        KeyOnly("all-missing"),
        KeyOnly("missing-user"),
        KeyOnly("missing-shorty"),
        KeyOnly("missing-count"),
        KeyOnly("missing-size"),
        KeyOnly("missing-value"),
        KeyOnly("missing-total"),
        KeyOnly("missing-little"),
        KeyOnly("missing-big"),
        KeyOnly("missing-done")
      )
      val right = Seq(
        Record("identical"),
        Record.dummy("all-missing"),
        Record("missing-user", user = "11"),
        Record("missing-shorty", shorty = 22.toShort),
        Record("missing-count", count = 33),
        Record("missing-size", size = 44L),
        Record("missing-value", value = 55f),
        Record("missing-total", total = 66d),
        Record("missing-little", little = 77.toByte),
        Record("missing-big", big = Array[Byte](88.toByte)),
        Record("missing-done", done = false)
      )

      DeepDiffTestCase
        .testReversedLeftAndRight()
        .defaultConf()
        .compare(left, right)
        .expect(
          DatasetDiffsBuilder[KeyExample, Product]()
            .kindOfDifferent(rightOnly = 10)
            .record(
              identical = 1,
              diffExamples = left.drop(1).zip(right.drop(1)).map({ case (l, r) => (Key(l.key), l, r) })
            )
            .common("key", identical = 11)
            .rightOnly(
              "user",
              identical = 9,
              rightExamples = Seq((Key("all-missing"), "1"), (Key("missing-user"), "11"))
            )
            .rightOnly(
              "shorty",
              identical = 9,
              rightExamples = Seq((Key("all-missing"), 2.toShort), (Key("missing-shorty"), 22.toShort))
            )
            .rightOnly(
              "count",
              identical = 9,
              rightExamples = Seq((Key("all-missing"), 3), (Key("missing-count"), 33))
            )
            .rightOnly(
              "size",
              identical = 9,
              rightExamples = Seq((Key("all-missing"), 4L), (Key("missing-size"), 44L))
            )
            .rightOnly(
              "value",
              identical = 9,
              rightExamples = Seq((Key("all-missing"), 5f), (Key("missing-value"), 55f))
            )
            .rightOnly(
              "total",
              identical = 9,
              rightExamples = Seq((Key("all-missing"), 6d), (Key("missing-total"), 66d))
            )
            .rightOnly(
              "little",
              identical = 9,
              rightExamples = Seq((Key("all-missing"), 7.toByte), (Key("missing-little"), 77.toByte))
            )
            .rightOnly(
              "big",
              identical = 9,
              rightExamples =
                Seq((Key("all-missing"), Array[Byte](8.toByte)), (Key("missing-big"), Array[Byte](88.toByte)))
            )
            .rightOnly(
              "done",
              identical = 9,
              rightExamples = Seq((Key("all-missing"), true), (Key("missing-done"), false))
            )
        )
    }
  }
}
