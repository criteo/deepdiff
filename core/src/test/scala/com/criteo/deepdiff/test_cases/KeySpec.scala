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

object KeySpec {
  final case class Simple(key: String, value: java.lang.Integer)
  final case class Nested(value: Long, simple: Simple)
  final case class Composite(
      string: String,
      shortF: java.lang.Short,
      intF: java.lang.Integer,
      longF: java.lang.Long,
      floatF: java.lang.Float,
      doubleF: java.lang.Double,
      booleanF: java.lang.Boolean,
      byteF: java.lang.Byte,
      binary: Array[Byte],
      value: Int
  )
}

final class KeySpec extends DeepDiffSpec {
  import DeepDiffSpec._
  import KeySpec._

  "Deep Diff" should {
    "differentiate emtpy string and null keys" when runningDeepDiffTestCase {
      val data: Seq[Simple] = Seq(
        Simple(key = "A", value = 1),
        Simple(key = "B", value = 2),
        Simple(key = "", value = 3),
        Simple(key = null, value = 4)
      )
      DeepDiffTestCase
        .defaultConf()
        .compare(left = data, right = data.reverse)
        .expectRaw(
          DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
            .record(identical = 4)
            .common("key", identical = 4)
            .common("value", identical = 4)
        )
    }

    "support composite keys" when runningDeepDiffTestCase {
      val data: Seq[Composite] = (for {
        string <- Seq[String]("1", "2", null)
        short <- Seq[java.lang.Short](1.toShort, 2.toShort, null)
        int <- Seq[java.lang.Integer](1, 2, null)
        long <- Seq[java.lang.Long](1L, 2L, null)
        float <- Seq[java.lang.Float](1f, 2f, null)
        double <- Seq[java.lang.Double](1d, 2d, null)
        boolean <- Seq[java.lang.Boolean](true, false, null)
        byte <- Seq[java.lang.Byte](1.toByte, 2.toByte, null)
        binary <- Seq[Array[Byte]](Array[Byte](1.toByte), Array[Byte](2.toByte), null)
      } yield Composite(string, short, int, long, float, double, boolean, byte, binary, 0)).zipWithIndex.map({
        case (row, index) => row.copy(value = index)
      })
      DeepDiffTestCase
        .conf( // language=HOCON
          """keys = ["string", "shortF", "intF", "longF", "floatF", "doubleF", "booleanF", "byteF", "binary"]
            |""".stripMargin)
        .compare(left = data, right = data.reverse)
        .expectRaw(
          DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
            .record(identical = data.length)
            .common("string", identical = data.length)
            .common("shortF", identical = data.length)
            .common("intF", identical = data.length)
            .common("longF", identical = data.length)
            .common("floatF", identical = data.length)
            .common("doubleF", identical = data.length)
            .common("booleanF", identical = data.length)
            .common("byteF", identical = data.length)
            .common("binary", identical = data.length)
            .common("value", identical = data.length)
        )
    }

    "support nested keys" when runningDeepDiffTestCase {
      val data: Seq[Nested] = Seq(
        Nested(simple = Simple(key = "1", value = 0), value = 1),
        Nested(simple = Simple(key = "2", value = 0), value = 2),
        Nested(simple = Simple(key = "", value = 0), value = 3),
        Nested(simple = Simple(key = null, value = 0), value = 4),
        Nested(simple = null, value = 5)
      )
      DeepDiffTestCase
        .conf( // language=HOCON
          """keys = ["simple.key"]
            |""".stripMargin)
        .compare(left = data, right = data.reverse)
        .expectRaw(
          DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
            .record(identical = 5)
            .common("value", identical = 5)
            .common("simple", identical = 5)
            .common("simple.key", identical = 4)
            .common("simple.value", identical = 4)
        )
    }

    "support multiple nested keys" when runningDeepDiffTestCase {
      val data: Seq[Nested] = Seq(
        Nested(simple = Simple(key = "0", value = 0), value = 1),
        Nested(simple = Simple(key = "1", value = 0), value = 2),
        Nested(simple = Simple(key = "0", value = 1), value = 3),
        Nested(simple = Simple(key = "1", value = 1), value = 4),
        Nested(simple = Simple(key = null, value = null), value = 5),
        Nested(simple = null, value = 6)
      )
      DeepDiffTestCase
        .conf( // language=HOCON
          """keys = ["simple.key", "simple.value"]
            |""".stripMargin)
        .compare(left = data, right = data.reverse)
        .expectRaw(
          DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
            .record(identical = 6)
            .common("value", identical = 6)
            .common("simple", identical = 6)
            .common("simple.key", identical = 5)
            .common("simple.value", identical = 5)
        )
    }
  }
}
