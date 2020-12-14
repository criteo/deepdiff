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

object NumericEqualitySpec {
  final case class Record(
      key: String,
      intF: java.lang.Integer,
      longF: java.lang.Long,
      floatF: java.lang.Float,
      doubleF: java.lang.Double
  )
  object Record {
    def fromConstant(key: String, value: Int): Record = Record(
      key = key,
      intF = value,
      longF = value.toLong,
      floatF = value.toFloat,
      doubleF = value.toDouble
    )
  }
  final case class BigRecord(
      key: String,
      intF: java.lang.Integer,
      longF: java.lang.Long,
      floatF: java.lang.Float,
      doubleF: java.lang.Double,
      array_int: Seq[java.lang.Integer],
      array_long: Seq[java.lang.Long],
      array_float: Seq[java.lang.Float],
      array_double: Seq[java.lang.Double],
      map_int: Map[String, java.lang.Integer],
      map_long: Map[String, java.lang.Long],
      map_float: Map[String, java.lang.Float],
      map_double: Map[String, java.lang.Double]
  )

  object BigRecord {
    def fromConstant(key: String, value: Int): BigRecord = BigRecord(
      key = key,
      intF = value,
      longF = value.toLong,
      floatF = value.toFloat,
      doubleF = value.toDouble,
      array_int = Seq(value),
      array_long = Seq(value.toLong),
      array_float = Seq(value.toFloat),
      array_double = Seq(value.toDouble),
      map_int = Map("a" -> value),
      map_long = Map("a" -> value.toLong),
      map_float = Map("a" -> value.toFloat),
      map_double = Map("a" -> value.toDouble)
    )
  }

  final case class BigNestedRecord(
      key: String,
      nested: BigRecord,
      array: Seq[BigRecord],
      exploded_array: Seq[BigRecord],
      map: Map[String, BigRecord]
  )

  final case class NestedRecord(
      key: String,
      nested: Record,
      array: Seq[Record],
      exploded_array: Seq[Record]
  )

  object NestedRecord {
    def apply(key: String, intF: Int, longF: Long, floatF: Float, doubleF: Double): NestedRecord = {
      val record = Record(
        key = s"$key-a",
        intF = intF,
        longF = longF,
        floatF = floatF,
        doubleF = doubleF
      )
      NestedRecord(
        key = key,
        nested = record,
        array = Seq(record.copy(key = s"$key-b")),
        exploded_array = Seq(record.copy(key = s"$key-c"))
      )
    }
  }
}

final class NumericEqualitySpec extends DeepDiffSpec {
  import DeepDiffSpec._
  import NumericEqualitySpec._
  import com.criteo.deepdiff.test_utils.EasyKeyExample._

  "Deep Diff" should {
    "be able to compare any numeric type" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .defaultConf()
        .compare(
          left = Seq(
            BigRecord.fromConstant("identical", 100),
            BigRecord.fromConstant("identical-2", 0),
            BigRecord.fromConstant("different", 100),
            BigRecord.fromConstant("different-2", 0)
          ),
          right = Seq(
            BigRecord.fromConstant("identical", 100),
            BigRecord.fromConstant("identical-2", 0),
            BigRecord.fromConstant("different", 101),
            BigRecord.fromConstant("different-2", 1)
          )
        )
        .expect(
          bigRecordChecker(
            identical = 2,
            diffExamples = Seq(
              (Key("different"), BigRecord.fromConstant("different", 100), BigRecord.fromConstant("different", 101)),
              (Key("different-2"), BigRecord.fromConstant("different-2", 0), BigRecord.fromConstant("different-2", 1))
            )
          )
        )
    }

    "support NaN comparisons" when runningDeepDiffTestCase {
      val nanBigRecord = BigRecord(
        key = "NaN",
        intF = 1,
        longF = 1L,
        floatF = java.lang.Float.NaN,
        doubleF = java.lang.Double.NaN,
        array_int = Seq(1),
        array_long = Seq(1L),
        array_float = Seq(java.lang.Float.NaN),
        array_double = Seq(java.lang.Double.NaN),
        map_int = Map("a" -> 1),
        map_long = Map("a" -> 1L),
        map_float = Map("a" -> java.lang.Float.NaN),
        map_double = Map("a" -> java.lang.Double.NaN)
      )
      DeepDiffTestCase
        .testReversedLeftAndRight()
        // no examples as scalatest compares NaN to NaN in the examples, which obviously does not work well
        .conf( // language=HOCON
          """keys = ["key"]
            |max-examples = 0
            |""".stripMargin)
        .compare(
          left = Seq(
            nanBigRecord,
            nanBigRecord.copy(key = "different-NaN")
          ),
          right = Seq(
            nanBigRecord,
            nanBigRecord.copy(
              key = "different-NaN",
              floatF = 1f,
              doubleF = 1d,
              array_float = Seq(1f),
              array_double = Seq(1d),
              map_float = Map("a" -> 1f),
              map_double = Map("a" -> 1d)
            )
          )
        )
        .expect(
          DatasetDiffsBuilder[KeyExample, BigRecord]()
            .kindOfDifferent(content = 1)
            .record(identical = 1, different = 1)
            .common("key", identical = 2)
            .common("intF", identical = 2)
            .common("longF", identical = 2)
            .common("array_int", identical = 2)
            .common("array_long", identical = 2)
            .common("map_int", identical = 2)
            .common("map_long", identical = 2)
            .common("floatF", identical = 1, different = 1)
            .common("doubleF", identical = 1, different = 1)
            .common("array_float", identical = 1, different = 1)
            .common("array_double", identical = 1, different = 1)
            .common("map_float", identical = 1, different = 1)
            .common("map_double", identical = 1, different = 1)
        )
    }

    "support default absolute tolerances" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .conf( // language=HOCON
          """keys = ["key"]
            |default-tolerance {
            |  absolute = 1
            |}
            |""".stripMargin)
        .compare(
          left = Seq(
            BigRecord.fromConstant("A", 100),
            BigRecord.fromConstant("B", 100),
            BigRecord.fromConstant("C", 100),
            BigRecord.fromConstant("D", 100),
            BigRecord.fromConstant("E", 100)
          ),
          right = Seq(
            BigRecord.fromConstant("A", 100),
            BigRecord.fromConstant("B", 101),
            BigRecord.fromConstant("C", 99),
            BigRecord.fromConstant("D", 102),
            BigRecord.fromConstant("E", 98)
          )
        )
        .expect(
          bigRecordChecker(
            identical = 3,
            diffExamples = Seq(
              (Key("D"), BigRecord.fromConstant("D", 100), BigRecord.fromConstant("D", 102)),
              (Key("E"), BigRecord.fromConstant("E", 100), BigRecord.fromConstant("E", 98))
            )
          )
        )
    }

    "support absolute tolerances" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .conf( // language=HOCON
          """keys = ["key"]
            |tolerances = {
            |  array_double = { absolute = 1.0 }
            |  array_float = { absolute = 1.0 }
            |  array_int = { absolute = 1.0 }
            |  array_long = { absolute = 1.0 }
            |  doubleF = { absolute = 1.0 }
            |  floatF = { absolute = 1.0 }
            |  intF = { absolute = 1.0 }
            |  longF = { absolute = 1.0 }
            |  map_double = { absolute = 1.0 }
            |  map_float = { absolute = 1.0 }
            |  map_int = { absolute = 1.0 }
            |  map_long = { absolute = 1.0 }
            |}
            |""".stripMargin)
        .compare(
          left = Seq(
            BigRecord.fromConstant("A", 100),
            BigRecord.fromConstant("B", 100),
            BigRecord.fromConstant("C", 100),
            BigRecord.fromConstant("D", 100),
            BigRecord.fromConstant("E", 100)
          ),
          right = Seq(
            BigRecord.fromConstant("A", 100),
            BigRecord.fromConstant("B", 101),
            BigRecord.fromConstant("C", 99),
            BigRecord.fromConstant("D", 102),
            BigRecord.fromConstant("E", 98)
          )
        )
        .expect(
          bigRecordChecker(
            identical = 3,
            diffExamples = Seq(
              (Key("D"), BigRecord.fromConstant("D", 100), BigRecord.fromConstant("D", 102)),
              (Key("E"), BigRecord.fromConstant("E", 100), BigRecord.fromConstant("E", 98))
            )
          )
        )
    }

    "support relative tolerances" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .conf( // language=HOCON
          """keys = ["key"]
            |default-tolerance = {
            |  absolute = 0
            |  relative = 0.1
            |}
            |""".stripMargin)
        .compare(
          left = Seq(
            BigRecord.fromConstant("A", 100),
            BigRecord.fromConstant("B", 100),
            BigRecord.fromConstant("C", 100),
            BigRecord.fromConstant("D", 100),
            BigRecord.fromConstant("E", 100),
            BigRecord.fromConstant("F", 100),
            BigRecord.fromConstant("G", 100)
          ),
          right = Seq(
            BigRecord.fromConstant("A", 100),
            BigRecord.fromConstant("B", 95),
            BigRecord.fromConstant("C", 105),
            BigRecord.fromConstant("D", 91),
            BigRecord.fromConstant("E", 109),
            BigRecord.fromConstant("F", 89),
            BigRecord.fromConstant("G", 111)
          )
        )
        .expect(
          bigRecordChecker(
            identical = 5,
            diffExamples = Seq(
              (Key("F"), BigRecord.fromConstant("F", 100), BigRecord.fromConstant("F", 89)),
              (Key("G"), BigRecord.fromConstant("G", 100), BigRecord.fromConstant("G", 111))
            )
          )
        )
    }

    "support absolute and relative tolerances" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .conf( // language=HOCON
          """keys = ["key"]
            |multiple-matches.ignore-if-identical = true
            |tolerances = {
            |  array_double = { absolute = 10.0, relative = 0.1 }
            |  array_float = { absolute = 10.0, relative = 0.1 }
            |  array_int = { absolute = 10.0, relative = 0.1 }
            |  array_long = { absolute = 10.0, relative = 0.1 }
            |  doubleF = { absolute = 10.0, relative = 0.1 }
            |  floatF = { absolute = 10.0, relative = 0.1 }
            |  intF = { absolute = 10.0, relative = 0.1 }
            |  longF = { absolute = 10.0, relative = 0.1 }
            |  map_double = { absolute = 10.0, relative = 0.1 }
            |  map_float = { absolute = 10.0, relative = 0.1 }
            |  map_int = { absolute = 10.0, relative = 0.1 }
            |  map_long = { absolute = 10.0, relative = 0.1 }
            |}
            |""".stripMargin)
        .compare(
          left = Seq(
            BigRecord.fromConstant("A", 100),
            BigRecord.fromConstant("B", 100),
            BigRecord.fromConstant("C", 100),
            BigRecord.fromConstant("D", 1000),
            BigRecord.fromConstant("E", 1000),
            BigRecord.fromConstant("F", 10),
            BigRecord.fromConstant("G", 10),
            BigRecord.fromConstant("H", 100),
            BigRecord.fromConstant("I", 100)
          ),
          right = Seq(
            BigRecord.fromConstant("A", 100),
            BigRecord.fromConstant("B", 95),
            BigRecord.fromConstant("C", 105),
            BigRecord.fromConstant("D", 1011), // abs not respected
            BigRecord.fromConstant("E", 989), // abs not respected
            BigRecord.fromConstant("F", 5), // rel not respected
            BigRecord.fromConstant("G", 15), // rel not respected
            BigRecord.fromConstant("H", 120), // neither respected
            BigRecord.fromConstant("I", 80) // neither respected
          )
        )
        .expect(
          bigRecordChecker(
            identical = 3,
            diffExamples = Seq(
              (Key("D"), BigRecord.fromConstant("D", 1000), BigRecord.fromConstant("D", 1011)),
              (Key("E"), BigRecord.fromConstant("E", 1000), BigRecord.fromConstant("E", 989)),
              (Key("F"), BigRecord.fromConstant("F", 10), BigRecord.fromConstant("F", 5)),
              (Key("G"), BigRecord.fromConstant("G", 10), BigRecord.fromConstant("G", 15)),
              (Key("H"), BigRecord.fromConstant("H", 100), BigRecord.fromConstant("H", 120)),
              (Key("I"), BigRecord.fromConstant("I", 100), BigRecord.fromConstant("I", 80))
            )
          )
        )
    }

    "support absolute or relative tolerances" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .conf( // language=HOCON
          """keys = ["key"]
            |multiple-matches.ignore-if-identical = true
            |default-tolerance {
            |  satisfies = "any"
            |}
            |tolerances = {
            |  array_double = { absolute = 10.0, relative = 0.1 }
            |  array_float = { absolute = 10.0, relative = 0.1 }
            |  array_int = { absolute = 10.0, relative = 0.1 }
            |  array_long = { absolute = 10.0, relative = 0.1 }
            |  doubleF = { absolute = 10.0, relative = 0.1 }
            |  floatF = { absolute = 10.0, relative = 0.1 }
            |  intF = { absolute = 10.0, relative = 0.1 }
            |  longF = { absolute = 10.0, relative = 0.1 }
            |  map_double = { absolute = 10.0, relative = 0.1 }
            |  map_float = { absolute = 10.0, relative = 0.1 }
            |  map_int = { absolute = 10.0, relative = 0.1 }
            |  map_long = { absolute = 10.0, relative = 0.1 }
            |}
            |""".stripMargin)
        .compare(
          left = Seq(
            BigRecord.fromConstant("A", 100),
            BigRecord.fromConstant("B", 100),
            BigRecord.fromConstant("C", 100),
            BigRecord.fromConstant("D", 1000),
            BigRecord.fromConstant("E", 1000),
            BigRecord.fromConstant("F", 10),
            BigRecord.fromConstant("G", 10),
            BigRecord.fromConstant("H", 100),
            BigRecord.fromConstant("I", 100)
          ),
          right = Seq(
            BigRecord.fromConstant("A", 100),
            BigRecord.fromConstant("B", 95),
            BigRecord.fromConstant("C", 105),
            BigRecord.fromConstant("D", 1011), // abs not respected
            BigRecord.fromConstant("E", 989), // abs not respected
            BigRecord.fromConstant("F", 5), // rel not respected
            BigRecord.fromConstant("G", 15), // rel not respected
            BigRecord.fromConstant("H", 120), // neither respected
            BigRecord.fromConstant("I", 80) // neither respected
          )
        )
        .expect(
          bigRecordChecker(
            identical = 7,
            diffExamples = Seq(
              (Key("H"), BigRecord.fromConstant("H", 100), BigRecord.fromConstant("H", 120)),
              (Key("I"), BigRecord.fromConstant("I", 100), BigRecord.fromConstant("I", 80))
            )
          )
        )
    }

    "also propagate tolerances in nested fields" when runningDeepDiffTestCase {
      val baseRecord = BigNestedRecord(
        key = "A",
        nested = BigRecord.fromConstant("a", 100),
        array = Seq(
          BigRecord.fromConstant("b", 20),
          BigRecord.fromConstant("c", 300)
        ),
        exploded_array = Seq(
          BigRecord.fromConstant("b", 20),
          BigRecord.fromConstant("c", 300)
        ),
        map = Map(
          "1" -> BigRecord.fromConstant("d", 1000),
          "2" -> BigRecord.fromConstant("e", 500)
        )
      )
      val left = Seq(
        baseRecord,
        baseRecord.copy(key = "B"),
        baseRecord.copy(key = "C")
      )
      val right = Seq(
        baseRecord,
        BigNestedRecord(
          key = "B",
          nested = BigRecord.fromConstant("a", 95),
          array = Seq(
            BigRecord.fromConstant("b", 20),
            BigRecord.fromConstant("c", 293)
          ),
          exploded_array = Seq(
            BigRecord.fromConstant("b", 20),
            BigRecord.fromConstant("c", 293)
          ),
          map = Map(
            "1" -> BigRecord.fromConstant("d", 997),
            "2" -> BigRecord.fromConstant("e", 502)
          )
        ),
        BigNestedRecord(
          key = "C",
          nested = BigRecord.fromConstant("a", 111),
          array = Seq(
            BigRecord.fromConstant("b", 15),
            BigRecord.fromConstant("c", 299)
          ),
          exploded_array = Seq(
            BigRecord.fromConstant("b", 15),
            BigRecord.fromConstant("c", 299)
          ),
          map = Map(
            "1" -> BigRecord.fromConstant("d", 1030),
            "2" -> BigRecord.fromConstant("e", 502)
          )
        )
      )
      DeepDiffTestCase
        .conf( // language=HOCON
          """keys = ["key"]
            |exploded-arrays {
            |  exploded_array = { keys = ["key"] }
            |}
            |tolerances = {
            |  array = { absolute = 10.0, relative = 0.1 }
            |  exploded_array = { absolute = 10.0, relative = 0.1 }
            |  map = { absolute = 10.0, relative = 0.1 }
            |  nested = { absolute = 10.0, relative = 0.1 }
            |}
            |""".stripMargin)
        .compare(left, right)
        .expect(
          DatasetDiffsBuilder[KeyExample, Product]()
            .kindOfDifferent(content = 1)
            .record(identical = 2, diffExamples = Seq((Key("C"), left.last, right.last)))
            .common("key", identical = 3)
            .common(
              "nested",
              identical = 2,
              diffExamples = Seq(
                (Key("C"), BigRecord.fromConstant(key = "a", 100), BigRecord.fromConstant(key = "a", value = 111))
              )
            )
            .common("nested.key", identical = 3)
            .common("nested.intF", identical = 2, diffExamples = Seq((Key("C"), 100, 111)))
            .common("nested.longF", identical = 2, diffExamples = Seq((Key("C"), 100L, 111L)))
            .common(
              "nested.floatF",
              identical = 2,
              diffExamples = Seq((Key("C"), 100f, 111f))
            )
            .common(
              "nested.doubleF",
              identical = 2,
              diffExamples = Seq((Key("C"), 100d, 111d))
            )
            .common(
              "nested.array_int",
              identical = 2,
              diffExamples = Seq((Key("C"), Seq(100), Seq(111)))
            )
            .common(
              "nested.array_long",
              identical = 2,
              diffExamples = Seq((Key("C"), Seq(100L), Seq(111L)))
            )
            .common(
              "nested.array_float",
              identical = 2,
              diffExamples = Seq((Key("C"), Seq(100f), Seq(111f)))
            )
            .common(
              "nested.array_double",
              identical = 2,
              diffExamples = Seq((Key("C"), Seq(100d), Seq(111d)))
            )
            .common(
              "nested.map_int",
              identical = 2,
              diffExamples = Seq((Key("C"), Map("a" -> 100), Map("a" -> 111)))
            )
            .common(
              "nested.map_long",
              identical = 2,
              diffExamples = Seq((Key("C"), Map("a" -> 100L), Map("a" -> 111L)))
            )
            .common(
              "nested.map_float",
              identical = 2,
              diffExamples = Seq((Key("C"), Map("a" -> 100f), Map("a" -> 111f)))
            )
            .common(
              "nested.map_double",
              identical = 2,
              diffExamples = Seq((Key("C"), Map("a" -> 100d), Map("a" -> 111d)))
            )
            .common(
              "array",
              identical = 2,
              diffExamples = Seq(
                (
                  Key("C"),
                  Seq(BigRecord.fromConstant(key = "b", value = 20), BigRecord.fromConstant(key = "c", value = 300)),
                  Seq(BigRecord.fromConstant(key = "b", value = 15), BigRecord.fromConstant(key = "c", value = 299))
                )
              )
            )
            .common(
              "array (struct)",
              identical = 5,
              diffExamples = Seq(
                (
                  Key("C") >> Key(0),
                  BigRecord.fromConstant(key = "b", value = 20),
                  BigRecord.fromConstant(key = "b", value = 15)
                )
              )
            )
            .common("array.key", identical = 6)
            .common(
              "array.intF",
              identical = 5,
              diffExamples = Seq((Key("C") >> Key(0), 20, 15))
            )
            .common(
              "array.longF",
              identical = 5,
              diffExamples = Seq((Key("C") >> Key(0), 20L, 15L))
            )
            .common(
              "array.floatF",
              identical = 5,
              diffExamples = Seq((Key("C") >> Key(0), 20f, 15f))
            )
            .common(
              "array.doubleF",
              identical = 5,
              diffExamples = Seq((Key("C") >> Key(0), 20d, 15L))
            )
            .common(
              "array.array_int",
              identical = 5,
              diffExamples = Seq((Key("C") >> Key(0), Seq(20), Seq(15)))
            )
            .common(
              "array.array_long",
              identical = 5,
              diffExamples = Seq((Key("C") >> Key(0), Seq(20L), Seq(15L)))
            )
            .common(
              "array.array_float",
              identical = 5,
              diffExamples = Seq((Key("C") >> Key(0), Seq(20f), Seq(15f)))
            )
            .common(
              "array.array_double",
              identical = 5,
              diffExamples = Seq((Key("C") >> Key(0), Seq(20d), Seq(15L)))
            )
            .common(
              "array.map_int",
              identical = 5,
              diffExamples = Seq((Key("C") >> Key(0), Map("a" -> 20), Map("a" -> 15)))
            )
            .common(
              "array.map_long",
              identical = 5,
              diffExamples = Seq((Key("C") >> Key(0), Map("a" -> 20L), Map("a" -> 15L)))
            )
            .common(
              "array.map_float",
              identical = 5,
              diffExamples = Seq((Key("C") >> Key(0), Map("a" -> 20f), Map("a" -> 15f)))
            )
            .common(
              "array.map_double",
              identical = 5,
              diffExamples = Seq((Key("C") >> Key(0), Map("a" -> 20d), Map("a" -> 15d)))
            )
            .common(
              "exploded_array",
              identical = 2,
              diffExamples = Seq(
                (
                  Key("C"),
                  Seq(BigRecord.fromConstant(key = "b", value = 20), BigRecord.fromConstant(key = "c", value = 300)),
                  Seq(BigRecord.fromConstant(key = "b", value = 15), BigRecord.fromConstant(key = "c", value = 299))
                )
              )
            )
            .explodedArray(
              "exploded_array",
              DatasetDiffsBuilder[KeyExample, Product]()
                .record(
                  identical = 5,
                  diffExamples = Seq(
                    (
                      Key("C") >> Key("b"),
                      BigRecord.fromConstant(key = "b", value = 20),
                      BigRecord.fromConstant(key = "b", value = 15)
                    )
                  )
                )
                .kindOfDifferent(content = 1)
                .common("key", identical = 6)
                .common(
                  "intF",
                  identical = 5,
                  diffExamples = Seq((Key("C") >> Key("b"), 20, 15))
                )
                .common(
                  "longF",
                  identical = 5,
                  diffExamples = Seq((Key("C") >> Key("b"), 20L, 15L))
                )
                .common(
                  "floatF",
                  identical = 5,
                  diffExamples = Seq((Key("C") >> Key("b"), 20f, 15f))
                )
                .common(
                  "doubleF",
                  identical = 5,
                  diffExamples = Seq((Key("C") >> Key("b"), 20d, 15L))
                )
                .common(
                  "array_int",
                  identical = 5,
                  diffExamples = Seq((Key("C") >> Key("b"), Seq(20), Seq(15)))
                )
                .common(
                  "array_long",
                  identical = 5,
                  diffExamples = Seq((Key("C") >> Key("b"), Seq(20L), Seq(15L)))
                )
                .common(
                  "array_float",
                  identical = 5,
                  diffExamples = Seq((Key("C") >> Key("b"), Seq(20f), Seq(15f)))
                )
                .common(
                  "array_double",
                  identical = 5,
                  diffExamples = Seq((Key("C") >> Key("b"), Seq(20d), Seq(15L)))
                )
                .common(
                  "map_int",
                  identical = 5,
                  diffExamples = Seq((Key("C") >> Key("b"), Map("a" -> 20), Map("a" -> 15)))
                )
                .common(
                  "map_long",
                  identical = 5,
                  diffExamples = Seq((Key("C") >> Key("b"), Map("a" -> 20L), Map("a" -> 15L)))
                )
                .common(
                  "map_float",
                  identical = 5,
                  diffExamples = Seq((Key("C") >> Key("b"), Map("a" -> 20f), Map("a" -> 15f)))
                )
                .common(
                  "map_double",
                  identical = 5,
                  diffExamples = Seq((Key("C") >> Key("b"), Map("a" -> 20d), Map("a" -> 15d)))
                )
            )
            .common(
              "map",
              identical = 2,
              diffExamples = Seq(
                (
                  Key("C"),
                  Map(
                    "1" -> BigRecord.fromConstant(key = "d", value = 1000),
                    "2" -> BigRecord.fromConstant(key = "e", value = 500)
                  ),
                  Map(
                    "1" -> BigRecord.fromConstant(key = "d", value = 1030),
                    "2" -> BigRecord.fromConstant(key = "e", value = 502)
                  )
                )
              )
            )
        )
    }

    "support complex relative and absolute tolerance definitions" when runningDeepDiffTestCase {
      val left = Seq(
        NestedRecord(
          key = "identical",
          intF = 10000,
          longF = 200,
          floatF = 5,
          doubleF = 100
        ),
        NestedRecord(
          key = "identical-tolerance",
          intF = 10000,
          longF = 200,
          floatF = 5,
          doubleF = 100
        ),
        NestedRecord(
          key = "C",
          intF = 10000,
          longF = 200,
          floatF = 5,
          doubleF = 100
        ),
        NestedRecord(
          key = "D",
          intF = 100,
          longF = 1000000,
          floatF = 100,
          doubleF = 1
        )
      )
      val right = Seq(
        NestedRecord(
          key = "identical",
          intF = 10000,
          longF = 200,
          floatF = 5,
          doubleF = 100
        ),
        NestedRecord(
          key = "identical-tolerance",
          intF = 9901,
          longF = 199,
          floatF = 10,
          doubleF = 101
        ),
        // overridden tolerances are taken into account
        NestedRecord(
          key = "C",
          intF = 9899,
          longF = 197,
          floatF = 11,
          doubleF = 102
        ),
        // old ones also
        NestedRecord(
          key = "D",
          intF = 87,
          longF = 1000011,
          floatF = 89,
          doubleF = 1.12
        )
      )
      DeepDiffTestCase
        .conf( // language=HOCON
          """keys = ["key"]
            |exploded-arrays {
            |  "exploded_array": { keys = ["key"] }
            |}
            |tolerances = {
            |  "array": { absolute = 10.0, relative = 0.1 }
            |  "array.doubleF": { absolute = 1.0 }
            |  "array.floatF": { relative = 1.0 }
            |  "array.intF": { absolute = 100.0 }
            |  "array.longF": { relative = 0.01 }
            |  "exploded_array": { absolute = 10.0, relative = 0.1 }
            |  "exploded_array.doubleF": { absolute = 1.0 }
            |  "exploded_array.floatF": { relative = 1.0 }
            |  "exploded_array.intF": { absolute = 100.0 }
            |  "exploded_array.longF": { relative = 0.01 }
            |  "nested" { absolute = 10.0, relative = 0.1 }
            |  "nested.doubleF": { absolute = 1.0 }
            |  "nested.floatF": { relative = 1.0 }
            |  "nested.intF": { absolute = 100.0 }
            |  "nested.longF": { relative = 0.01 }
            |}
            |""".stripMargin)
        .compare(left, right)
        .expect(
          DatasetDiffsBuilder[KeyExample, Product]()
            .kindOfDifferent(content = 2)
            .record(
              identical = 2,
              diffExamples = left
                .drop(2)
                .zip(right.drop(2))
                .map({
                  case (l, r) => (Key(l.key), l, r)
                })
            )
            .common("key", identical = 4)
            .common(
              "nested",
              identical = 2,
              diffExamples = Seq(
                (
                  Key("C"),
                  Record(key = "C-a", intF = 10000, longF = 200, floatF = 5, doubleF = 100),
                  Record(key = "C-a", intF = 9899, longF = 197, floatF = 11, doubleF = 102)
                ),
                (
                  Key("D"),
                  Record(key = "D-a", intF = 100, longF = 1000000, floatF = 100, doubleF = 1),
                  Record(key = "D-a", intF = 87, longF = 1000011, floatF = 89, doubleF = 1.12)
                )
              )
            )
            .common("nested.key", identical = 4)
            .common("nested.intF", identical = 2, diffExamples = Seq((Key("C"), 10000, 9899), (Key("D"), 100, 87)))
            .common(
              "nested.longF",
              identical = 2,
              diffExamples = Seq((Key("C"), 200L, 197L), (Key("D"), 1000000L, 1000011L))
            )
            .common(
              "nested.floatF",
              identical = 2,
              diffExamples = Seq((Key("C"), 5f, 11f), (Key("D"), 100f, 89f))
            )
            .common(
              "nested.doubleF",
              identical = 2,
              diffExamples = Seq((Key("C"), 100d, 102d), (Key("D"), 1d, 1.12d))
            )
            .common(
              "array",
              identical = 2,
              diffExamples = Seq(
                (
                  Key("C"),
                  Seq(Record(key = "C-b", intF = 10000, longF = 200, floatF = 5, doubleF = 100)),
                  Seq(Record(key = "C-b", intF = 9899, longF = 197, floatF = 11, doubleF = 102))
                ),
                (
                  Key("D"),
                  Seq(Record(key = "D-b", intF = 100, longF = 1000000, floatF = 100, doubleF = 1)),
                  Seq(Record(key = "D-b", intF = 87, longF = 1000011, floatF = 89, doubleF = 1.12))
                )
              )
            )
            .common(
              "array (struct)",
              identical = 2,
              diffExamples = Seq(
                (
                  Key("C") >> Key(0),
                  Record(key = "C-b", intF = 10000, longF = 200, floatF = 5, doubleF = 100),
                  Record(key = "C-b", intF = 9899, longF = 197, floatF = 11, doubleF = 102)
                ),
                (
                  Key("D") >> Key(0),
                  Record(key = "D-b", intF = 100, longF = 1000000, floatF = 100, doubleF = 1),
                  Record(key = "D-b", intF = 87, longF = 1000011, floatF = 89, doubleF = 1.12)
                )
              )
            )
            .common("array.key", identical = 4)
            .common(
              "array.intF",
              identical = 2,
              diffExamples = Seq((Key("C") >> Key(0), 10000, 9899), (Key("D") >> Key(0), 100, 87))
            )
            .common(
              "array.longF",
              identical = 2,
              diffExamples = Seq((Key("C") >> Key(0), 200L, 197L), (Key("D") >> Key(0), 1000000L, 1000011L))
            )
            .common(
              "array.floatF",
              identical = 2,
              diffExamples = Seq((Key("C") >> Key(0), 5f, 11f), (Key("D") >> Key(0), 100f, 89f))
            )
            .common(
              "array.doubleF",
              identical = 2,
              diffExamples = Seq((Key("C") >> Key(0), 100d, 102d), (Key("D") >> Key(0), 1d, 1.12d))
            )
            .common(
              "exploded_array",
              identical = 2,
              diffExamples = Seq(
                (
                  Key("C"),
                  Seq(Record(key = "C-c", intF = 10000, longF = 200, floatF = 5, doubleF = 100)),
                  Seq(Record(key = "C-c", intF = 9899, longF = 197, floatF = 11, doubleF = 102))
                ),
                (
                  Key("D"),
                  Seq(Record(key = "D-c", intF = 100, longF = 1000000, floatF = 100, doubleF = 1)),
                  Seq(Record(key = "D-c", intF = 87, longF = 1000011, floatF = 89, doubleF = 1.12))
                )
              )
            )
            .explodedArray(
              "exploded_array",
              DatasetDiffsBuilder[KeyExample, Product]()
                .record(
                  identical = 2,
                  diffExamples = Seq(
                    (
                      Key("C") >> Key("C-c"),
                      Record(key = "C-c", intF = 10000, longF = 200, floatF = 5, doubleF = 100),
                      Record(key = "C-c", intF = 9899, longF = 197, floatF = 11, doubleF = 102)
                    ),
                    (
                      Key("D") >> Key("D-c"),
                      Record(key = "D-c", intF = 100, longF = 1000000, floatF = 100, doubleF = 1),
                      Record(key = "D-c", intF = 87, longF = 1000011, floatF = 89, doubleF = 1.12)
                    )
                  )
                )
                .kindOfDifferent(content = 2)
                .common("key", identical = 4)
                .common(
                  "intF",
                  identical = 2,
                  diffExamples = Seq((Key("C") >> Key("C-c"), 10000, 9899), (Key("D") >> Key("D-c"), 100, 87))
                )
                .common(
                  "longF",
                  identical = 2,
                  diffExamples = Seq((Key("C") >> Key("C-c"), 200L, 197L), (Key("D") >> Key("D-c"), 1000000L, 1000011L))
                )
                .common(
                  "floatF",
                  identical = 2,
                  diffExamples = Seq((Key("C") >> Key("C-c"), 5f, 11f), (Key("D") >> Key("D-c"), 100f, 89f))
                )
                .common(
                  "doubleF",
                  identical = 2,
                  diffExamples = Seq((Key("C") >> Key("C-c"), 100d, 102d), (Key("D") >> Key("D-c"), 1d, 1.12d))
                )
            )
        )
    }
  }

  private def bigRecordChecker(
      identical: Int,
      diffExamples: Seq[(KeyExample, BigRecord, BigRecord)]
  ): DatasetDiffsBuilder[KeyExample, BigRecord] = {
    val checker = DatasetDiffsBuilder[KeyExample, BigRecord]()
      .kindOfDifferent(content = diffExamples.length)
      .record(identical = identical, diffExamples = diffExamples)
      .common("key", identical + diffExamples.length)

    def addFieldExamples[T](name: String, getter: BigRecord => T)(
        checker: DatasetDiffsBuilder[KeyExample, BigRecord]
    ): DatasetDiffsBuilder[KeyExample, BigRecord] = {
      checker.common(name, identical = identical, diffExamples = diffExamples.map({
        case (key, left, right) => (key, getter(left), getter(right))
      }))
    }

    Seq(
      addFieldExamples("intF", _.intF)(_),
      addFieldExamples("longF", _.longF)(_),
      addFieldExamples("floatF", _.floatF)(_),
      addFieldExamples("doubleF", _.doubleF)(_),
      addFieldExamples("array_int", _.array_int)(_),
      addFieldExamples("array_long", _.array_long)(_),
      addFieldExamples("array_float", _.array_float)(_),
      addFieldExamples("array_double", _.array_double)(_),
      addFieldExamples("map_int", _.map_int)(_),
      addFieldExamples("map_long", _.map_long)(_),
      addFieldExamples("map_float", _.map_float)(_),
      addFieldExamples("map_double", _.map_double)(_)
    ).foldLeft(checker)({
      case (checker, func) => func(checker)
    })
  }
}
