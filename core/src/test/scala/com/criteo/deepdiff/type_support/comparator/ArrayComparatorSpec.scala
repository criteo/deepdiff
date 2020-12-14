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

object ArrayComparatorSpec {
  final case class TestCase[+T](elementType: DataType, left: Seq[T], right: Seq[T], equal: Boolean)
}

final class ArrayComparatorSpec extends AnyFlatSpec with Matchers {
  import ArrayComparatorSpec._
  import SparkUtils._

  val testCases = Seq(
    TestCase[Int](IntegerType, Seq.empty, Seq.empty, equal = true),
    TestCase[Int](IntegerType, Seq(1, 2, 3), Seq(1, 2, 3), equal = true),
    TestCase[Int](IntegerType, Seq(1, 2, 3), Seq(1, 2, 3, 4), equal = false),
    TestCase[Int](IntegerType, Seq(1, 2, 3), Seq(1, 2), equal = false),
    TestCase[Int](IntegerType, Seq(1, 2, 3), Seq(1, 3, 2), equal = false),
    TestCase[String](StringType, Seq("a", "b"), Seq("a", "b"), equal = true)
  )

  it should "be able to compare two arrays" in {
    for (test <- testCases) {
      withClue(test.toString) {
        val comparator = {
          val arrayType = ArrayType(test.elementType)
          EqualityParams
          Comparator(arrayType, defaultEqualityParams)
        }
        comparator.isFieldEqual(
          asSparkRow(("placeholder", test.left)),
          1,
          asSparkRow((test.right, "placeholder")),
          0
        ) should be(test.equal)
      }
    }
  }

  it should "handle null values correctly" in {
    val comparator = {
      val arrayType = ArrayType(ArrayType(IntegerType))
      Comparator(arrayType, defaultEqualityParams)
    }

    val ordinal = 2
    def makeDummy[T](a: Seq[T]): InternalRow = asSparkRow((1, "2", a, 3d))

    val ref = makeDummy(Seq(Seq(1), Seq(2)))
    val aNull = makeDummy(Seq(Seq(1), null))
    val bNull = makeDummy(Seq(null, Seq(2)))

    comparator.isFieldEqual(ref, ordinal, ref, ordinal) should be(true)
    comparator.isFieldEqual(aNull, ordinal, aNull, ordinal) should be(true)
    comparator.isFieldEqual(ref, ordinal, aNull, ordinal) should be(false)
    comparator.isFieldEqual(aNull, ordinal, ref, ordinal) should be(false)
    comparator.isFieldEqual(aNull, ordinal, bNull, ordinal) should be(false)
  }
}
