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

package com.criteo.deepdiff.type_support.comparator

import com.criteo.deepdiff.test_utils.SparkUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object MapComparatorSpec {
  final case class TestCase[+K, +V](keyType: DataType,
                                    valueType: DataType,
                                    left: Seq[(K, V)],
                                    right: Seq[(K, V)],
                                    equal: Boolean)
}

final class MapComparatorSpec extends AnyFlatSpec with Matchers {
  import MapComparatorSpec._
  import SparkUtils._

  val testCases = Seq(
    TestCase[String, Int](StringType, IntegerType, Seq.empty, Seq.empty, equal = true),
    TestCase[String, Int](StringType, IntegerType, Seq("a" -> 1, "b" -> 2), Seq("a" -> 1, "b" -> 2), equal = true),
    TestCase[String, Int](StringType, IntegerType, Seq("a" -> 1, "b" -> 2), Seq("b" -> 2, "a" -> 1), equal = true),
    TestCase[String, java.lang.Integer](StringType,
                                        IntegerType,
                                        Seq("a" -> null, "b" -> 2),
                                        Seq("b" -> 2, "a" -> null),
                                        equal = true),
    TestCase[Int, String](IntegerType, StringType, Seq(1 -> "a"), Seq(1 -> "a"), equal = true),
    TestCase[Int, String](IntegerType, StringType, Seq(1 -> "a"), Seq(2 -> "a"), equal = false),
    TestCase[Int, String](IntegerType, StringType, Seq(1 -> "a"), Seq(1 -> "b"), equal = false),
    TestCase[String, Int](StringType, IntegerType, Seq("a" -> 1, "b" -> 2), Seq("a" -> 1, "b" -> 3), equal = false),
    TestCase[String, Int](StringType, IntegerType, Seq("a" -> 1, "b" -> 2), Seq("A" -> 1, "b" -> 2), equal = false),
    TestCase[String, Int](StringType, IntegerType, Seq("a" -> 1, "b" -> 2), Seq("a" -> 1), equal = false)
  )

  it should "be able to compare two maps" in {
    for (test <- testCases) {
      withClue(test.toString) {
        val comparator = {
          val mapType = MapType(test.keyType, test.valueType)
          Comparator(mapType, defaultEqualityParams)
        }
        comparator.isFieldEqual(
          asSparkRow(("placeholder", asSparkMap(test.left))),
          1,
          asSparkRow((asSparkMap(test.right), "placeholder")),
          0
        ) should be(test.equal)
      }
    }
  }

  it should "handle null values correctly" in {
    val comparator = {
      val mapType = MapType(StringType, ArrayType(IntegerType))
      Comparator(mapType, defaultEqualityParams)
    }

    val A = 2
    val B = 1
    def makeA(keys: Seq[_], values: Seq[_]): InternalRow =
      asSparkRow((1, "2", asSparkMap(keys, values), 3d))

    def makeB(keys: Seq[_], values: Seq[_]): InternalRow =
      asSparkRow((1, asSparkMap(keys, values), "2", 3d))

    val ref = makeA(Seq("a", "b"), Seq(Seq(1, 2), Seq(1, 2)))
    val aNull = makeA(Seq("a", "b"), Seq(null, Seq(1, 2)))
    val bNull = makeB(Seq("a", "b"), Seq(Seq(1, 2), null))

    comparator.isFieldEqual(ref, A, ref, A) should be(true)
    comparator.isFieldEqual(aNull, A, aNull, A) should be(true)
    comparator.isFieldEqual(ref, A, aNull, A) should be(false)
    comparator.isFieldEqual(aNull, A, ref, A) should be(false)
    comparator.isFieldEqual(aNull, A, bNull, B) should be(false)
  }
}
