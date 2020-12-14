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

import scala.collection.immutable.ListMap

object MapSpec {
  object RecordValue {
    def dummy(key: String): RecordValue =
      RecordValue(
        key = key,
        user = Map("user" -> "1"),
        shorty = Map("shorty" -> 2.toShort),
        count = Map("count" -> 3),
        size = Map("size" -> 4L),
        value = Map("value" -> 5f),
        total = Map("total" -> 6d),
        little = Map("little" -> 7.toByte),
        big = Map("big" -> Array[Byte](8)),
        done = Map("done" -> true)
      )
  }
  final case class RecordValue(
      key: String,
      user: Map[String, String] = null,
      shorty: Map[String, java.lang.Short] = null,
      count: Map[String, java.lang.Integer] = null,
      size: Map[String, java.lang.Long] = null,
      value: Map[String, java.lang.Float] = null,
      total: Map[String, java.lang.Double] = null,
      little: Map[String, java.lang.Byte] = null,
      big: Map[String, Array[Byte]] = null,
      done: Map[String, java.lang.Boolean] = null
  )

  object RecordKey {
    def dummy(key: String): RecordKey =
      RecordKey(
        key = key,
        user = Map("1" -> "user"),
        shorty = Map(2.toShort -> "shorty"),
        count = Map(3 -> "count"),
        size = Map(4L -> "size"),
        little = Map(7.toByte -> "little"),
        big = Map(Array[Byte](8) -> "big")
      )
  }
  final case class RecordKey(
      key: String,
      user: Map[String, String] = null,
      shorty: Map[Short, String] = null,
      count: Map[Int, String] = null,
      size: Map[Long, String] = null,
      little: Map[Byte, String] = null,
      big: Map[Array[Byte], String] = null
  )

  final case class RecordStruct(key: String, nested: Map[String, Nested] = null)
  final case class Nested(user: String = null, count: java.lang.Integer = null)
  final case class Simple(key: String, mapping: Map[String, Int] = null)
}

final class MapSpec extends DeepDiffSpec {
  import DeepDiffSpec._
  import MapSpec._
  import com.criteo.deepdiff.test_utils.EasyKeyExample._

  "Deep Diff" should {
    "compare Map of structs" when runningDeepDiffTestCase {
      val left = Seq(
        RecordStruct("identical", nested = Map("1" -> Nested("a", 1))),
        RecordStruct("different", nested = Map("2" -> Nested("b", 2))),
        RecordStruct("different-2", nested = Map("3" -> Nested("c", 3))),
        RecordStruct("left-only", nested = Map("4" -> Nested("d", 4))),
        RecordStruct("right-only", nested = Map("5" -> null))
      )
      val right = Seq(
        RecordStruct("identical", nested = Map("1" -> Nested("a", 1))),
        RecordStruct("different", nested = Map("2" -> Nested("b", 22))),
        RecordStruct("different-2", nested = Map("3" -> Nested("cc", 3))),
        RecordStruct("left-only", nested = Map("4" -> null)),
        RecordStruct("right-only", nested = Map("5" -> Nested("e", 5)))
      )

      DeepDiffTestCase
        .defaultConf()
        .compare(left, right)
        .expect(
          DatasetDiffsBuilder[KeyExample, Product]()
            .kindOfDifferent(content = 4)
            .record(
              identical = 1,
              diffExamples = left.drop(1).zip(right.drop(1)).map({ case (l, r) => (Key(l.key), l, r) })
            )
            .common("key", identical = 5)
            .common(
              "nested",
              identical = 1,
              diffExamples = Seq(
                (Key("different"), Map("2" -> Nested("b", 2)), Map("2" -> Nested("b", 22))),
                (Key("different-2"), Map("3" -> Nested("c", 3)), Map("3" -> Nested("cc", 3))),
                (Key("left-only"), Map("4" -> Nested("d", 4)), Map("4" -> null)),
                (Key("right-only"), Map("5" -> null), Map("5" -> Nested("e", 5)))
              )
            )
        )
    }

    "support comparing Maps to null" when runningDeepDiffTestCase {
      val left = Seq(
        RecordValue("all-null"),
        RecordValue.dummy("null-user"),
        RecordValue.dummy("null-shorty"),
        RecordValue.dummy("null-count"),
        RecordValue.dummy("null-size"),
        RecordValue.dummy("null-value"),
        RecordValue.dummy("null-total"),
        RecordValue.dummy("null-little"),
        RecordValue.dummy("null-big"),
        RecordValue.dummy("null-done")
      )
      val right = Seq(
        RecordValue("all-null"),
        RecordValue.dummy("null-user").copy(user = null),
        RecordValue.dummy("null-shorty").copy(shorty = null),
        RecordValue.dummy("null-count").copy(count = null),
        RecordValue.dummy("null-size").copy(size = null),
        RecordValue.dummy("null-value").copy(value = null),
        RecordValue.dummy("null-total").copy(total = null),
        RecordValue.dummy("null-little").copy(little = null),
        RecordValue.dummy("null-big").copy(big = null),
        RecordValue.dummy("null-done").copy(done = null)
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
            .common("user", identical = 9, leftExamples = Seq((Key("null-user"), Map("user" -> "1"))))
            .common("shorty", identical = 9, leftExamples = Seq((Key("null-shorty"), Map("shorty" -> 2.toShort))))
            .common("count", identical = 9, leftExamples = Seq((Key("null-count"), Map("count" -> 3))))
            .common("size", identical = 9, leftExamples = Seq((Key("null-size"), Map("size" -> 4L))))
            .common("value", identical = 9, leftExamples = Seq((Key("null-value"), Map("value" -> 5f))))
            .common("total", identical = 9, leftExamples = Seq((Key("null-total"), Map("total" -> 6d))))
            .common("little", identical = 9, leftExamples = Seq((Key("null-little"), Map("little" -> 7.toByte))))
            .common(
              "big",
              identical = 9,
              leftExamples = Seq((Key("null-big"), Map("big" -> Array[Byte](8.toByte))))
            )
            .common("done", identical = 9, leftExamples = Seq((Key("null-done"), Map("done" -> true))))
        )
    }

    // Using protobuf here to have precise control of the order of the map's elements.
    "not be order sensitive for Maps" when {
      "comparing multiple matches" which {
        val conf = // language=HOCON
          """keys = ["key"]
            |multiple-matches.omit-if-all-identical = true
            |""".stripMargin
        // adding one element to avoid a empty parquet file.
        val identical = Simple(key = "identical")
        val data = Seq(
          Simple("A", ListMap("1" -> 1, "2" -> 2, "3" -> 3, "4" -> 4)),
          Simple("A", ListMap("2" -> 2, "3" -> 3, "4" -> 4, "1" -> 1)),
          Simple("A", ListMap("3" -> 3, "4" -> 4, "1" -> 1, "2" -> 2)),
          Simple("A", ListMap("4" -> 4, "1" -> 1, "2" -> 2, "3" -> 3))
        )

        "are on on side only" when runningDeepDiffTestCase {
          DeepDiffTestCase
            .testReversedLeftAndRight()
            .conf(conf)
            .compare(data :+ identical, Seq(identical))
            .expect(
              DatasetDiffsBuilder[KeyExample, Simple]()
                .record(
                  identical = 1,
                  leftExamples = Seq((Key("A"), Simple("A", Map("1" -> 1, "2" -> 2, "3" -> 3, "4" -> 4))))
                )
                .common("key", identical = 1)
                .common("mapping", identical = 1)
            )
        }

        "are on both sides" when runningDeepDiffTestCase {
          DeepDiffTestCase
            .conf(conf)
            .compare(data, data.reverse)
            .expect(
              DatasetDiffsBuilder[KeyExample, Simple]()
                .record(identical = 1)
                .common("key", identical = 1)
                .common("mapping", identical = 1)
            )
        }
      }
      "comparing left and right records" when runningDeepDiffTestCase {
        DeepDiffTestCase
          .defaultConf()
          .compare(
            left = Seq(
              Simple("identical", ListMap("1" -> 1, "2" -> 2, "3" -> 3, "4" -> 4)),
              Simple("identical-2", ListMap("1" -> 1, "2" -> 2, "3" -> 3, "4" -> 4)),
              Simple("identical-3", ListMap("1" -> 1, "2" -> 2, "3" -> 3, "4" -> 4)),
              Simple("identical-4", ListMap("1" -> 1, "2" -> 2, "3" -> 3, "4" -> 4))
            ),
            right = Seq(
              Simple("identical", ListMap("1" -> 1, "2" -> 2, "3" -> 3, "4" -> 4)),
              Simple("identical-2", ListMap("2" -> 2, "3" -> 3, "4" -> 4, "1" -> 1)),
              Simple("identical-3", ListMap("3" -> 3, "4" -> 4, "1" -> 1, "2" -> 2)),
              Simple("identical-4", ListMap("4" -> 4, "1" -> 1, "2" -> 2, "3" -> 3))
            )
          )
          .expect(
            DatasetDiffsBuilder[KeyExample, Simple]()
              .record(identical = 4)
              .common("key", identical = 4)
              .common("mapping", identical = 4)
          )
      }
    }

    "support any value type in Maps" when runningDeepDiffTestCase {
      val left = Seq(
        RecordValue.dummy("identical"),
        RecordValue.dummy("different-user"),
        RecordValue.dummy("different-shorty"),
        RecordValue.dummy("different-count"),
        RecordValue.dummy("different-size"),
        RecordValue.dummy("different-value"),
        RecordValue.dummy("different-total"),
        RecordValue.dummy("different-little"),
        RecordValue.dummy("different-big"),
        RecordValue.dummy("different-done"),
        RecordValue.dummy("null-user"),
        RecordValue.dummy("null-shorty"),
        RecordValue.dummy("null-count"),
        RecordValue.dummy("null-size"),
        RecordValue.dummy("null-value"),
        RecordValue.dummy("null-total"),
        RecordValue.dummy("null-little"),
        RecordValue.dummy("null-big"),
        RecordValue.dummy("null-done")
      )
      val right = Seq(
        RecordValue.dummy("identical"),
        RecordValue.dummy("different-user").copy(user = Map("user" -> "11")),
        RecordValue.dummy("different-shorty").copy(shorty = Map("shorty" -> 22.toShort)),
        RecordValue.dummy("different-count").copy(count = Map("count" -> 33)),
        RecordValue.dummy("different-size").copy(size = Map("size" -> 44L)),
        RecordValue.dummy("different-value").copy(value = Map("value" -> 55f)),
        RecordValue.dummy("different-total").copy(total = Map("total" -> 66d)),
        RecordValue.dummy("different-little").copy(little = Map("little" -> 77.toByte)),
        RecordValue.dummy("different-big").copy(big = Map("big" -> Array[Byte](88.toByte))),
        RecordValue.dummy("different-done").copy(done = Map("done" -> false)),
        RecordValue.dummy("null-user").copy(user = Map("user" -> null)),
        RecordValue.dummy("null-shorty").copy(shorty = Map("shorty" -> null)),
        RecordValue.dummy("null-count").copy(count = Map("count" -> null)),
        RecordValue.dummy("null-size").copy(size = Map("size" -> null)),
        RecordValue.dummy("null-value").copy(value = Map("value" -> null)),
        RecordValue.dummy("null-total").copy(total = Map("total" -> null)),
        RecordValue.dummy("null-little").copy(little = Map("little" -> null)),
        RecordValue.dummy("null-big").copy(big = Map("big" -> null)),
        RecordValue.dummy("null-done").copy(done = Map("done" -> null))
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
              diffExamples = Seq(
                (Key("different-user"), Map("user" -> "1"), Map("user" -> "11")),
                (Key("null-user"), Map("user" -> "1"), Map("user" -> null))
              )
            )
            .common(
              "shorty",
              identical = 17,
              diffExamples = Seq(
                (Key("different-shorty"), Map("shorty" -> 2.toShort), Map("shorty" -> 22.toShort)),
                (Key("null-shorty"), Map("shorty" -> 2.toShort), Map("shorty" -> null))
              )
            )
            .common(
              "count",
              identical = 17,
              diffExamples = Seq(
                (Key("different-count"), Map("count" -> 3), Map("count" -> 33)),
                (Key("null-count"), Map("count" -> 3), Map("count" -> null))
              )
            )
            .common(
              "size",
              identical = 17,
              diffExamples = Seq(
                (Key("different-size"), Map("size" -> 4L), Map("size" -> 44L)),
                (Key("null-size"), Map("size" -> 4L), Map("size" -> null))
              )
            )
            .common(
              "value",
              identical = 17,
              diffExamples = Seq(
                (Key("different-value"), Map("value" -> 5f), Map("value" -> 55f)),
                (Key("null-value"), Map("value" -> 5f), Map("value" -> null))
              )
            )
            .common(
              "total",
              identical = 17,
              diffExamples = Seq(
                (Key("different-total"), Map("total" -> 6d), Map("total" -> 66d)),
                (Key("null-total"), Map("total" -> 6d), Map("total" -> null))
              )
            )
            .common(
              "little",
              identical = 17,
              diffExamples = Seq(
                (Key("different-little"), Map("little" -> 7.toByte), Map("little" -> 77.toByte)),
                (Key("null-little"), Map("little" -> 7.toByte), Map("little" -> null))
              )
            )
            .common(
              "big",
              identical = 17,
              diffExamples = Seq(
                (Key("different-big"), Map("big" -> Array[Byte](8.toByte)), Map("big" -> Array[Byte](88.toByte))),
                (Key("null-big"), Map("big" -> Array[Byte](8.toByte)), Map("big" -> null))
              )
            )
            .common(
              "done",
              identical = 17,
              diffExamples = Seq(
                (Key("different-done"), Map("done" -> true), Map("done" -> false)),
                (Key("null-done"), Map("done" -> true), Map("done" -> null))
              )
            )
        )
    }

    "support any key types in Maps" when runningDeepDiffTestCase {
      val left = Seq(
        RecordKey.dummy("identical"),
        RecordKey.dummy("different-user"),
        RecordKey.dummy("different-shorty"),
        RecordKey.dummy("different-count"),
        RecordKey.dummy("different-size"),
        RecordKey.dummy("different-little"),
        RecordKey.dummy("different-big")
      )
      val right = Seq(
        RecordKey.dummy("identical"),
        RecordKey.dummy("different-user").copy(user = Map("11" -> "user")),
        RecordKey.dummy("different-shorty").copy(shorty = Map(22.toShort -> "shorty")),
        RecordKey.dummy("different-count").copy(count = Map(33 -> "count")),
        RecordKey.dummy("different-size").copy(size = Map(44L -> "size")),
        RecordKey.dummy("different-little").copy(little = Map(77.toByte -> "little")),
        RecordKey.dummy("different-big").copy(big = Map(Array[Byte](88.toByte) -> "big"))
      )

      DeepDiffTestCase
        .defaultConf()
        .compare(left, right)
        .expect(
          DatasetDiffsBuilder[KeyExample, Product]()
            .kindOfDifferent(content = 6)
            .record(
              identical = 1,
              diffExamples = left.drop(1).zip(right.drop(1)).map({ case (l, r) => (Key(l.key), l, r) })
            )
            .common("key", identical = 7)
            .common(
              "user",
              identical = 6,
              diffExamples = Seq((Key("different-user"), Map("1" -> "user"), Map("11" -> "user")))
            )
            .common(
              "shorty",
              identical = 6,
              diffExamples = Seq(
                (Key("different-shorty"), Map(2.toShort -> "shorty"), Map(22.toShort -> "shorty"))
              )
            )
            .common(
              "count",
              identical = 6,
              diffExamples = Seq((Key("different-count"), Map(3 -> "count"), Map(33 -> "count")))
            )
            .common(
              "size",
              identical = 6,
              diffExamples = Seq((Key("different-size"), Map(4L -> "size"), Map(44L -> "size")))
            )
            .common(
              "little",
              identical = 6,
              diffExamples = Seq(
                (Key("different-little"), Map(7.toByte -> "little"), Map(77.toByte -> "little"))
              )
            )
            .common(
              "big",
              identical = 6,
              diffExamples = Seq(
                (Key("different-big"), Map(Array[Byte](8.toByte) -> "big"), Map(Array[Byte](88.toByte) -> "big"))
              )
            )
        )
    }
  }
}
