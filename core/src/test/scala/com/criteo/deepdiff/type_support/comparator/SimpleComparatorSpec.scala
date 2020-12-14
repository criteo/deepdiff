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
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object SimpleComparatorSpec {
  final case class TestCase[+T](dataType: DataType, a: T, b: T, equal: Boolean)
}

final class SimpleComparatorSpec extends AnyFlatSpec with Matchers {
  import SimpleComparatorSpec._

  val testCases = Seq(
    TestCase[Boolean](BooleanType, true, true, equal = true),
    TestCase[Boolean](BooleanType, false, true, equal = false),
    TestCase[Byte](ByteType, 1, 1, equal = true),
    TestCase[Byte](ByteType, 1, 110, equal = false),
    TestCase[String](StringType, "test", "test", equal = true),
    TestCase[String](StringType, "test", "not", equal = false),
    TestCase[Array[Byte]](BinaryType, "test".getBytes(), "test".getBytes(), equal = true),
    TestCase[Array[Byte]](BinaryType, "test".getBytes(), "not".getBytes(), equal = false),
    TestCase[Int](DateType, 1234, 1234, equal = true),
    TestCase[Int](DateType, 1234, 2345, equal = false),
    TestCase[Long](TimestampType, 1234L, 1234L, equal = true),
    TestCase[Long](TimestampType, 1234L, 2345L, equal = false)
  )

  it should "work" in {
    for (test <- testCases) {
      withClue(test.toString) {
        val left = SparkUtils.asSparkRow((Seq.empty[Int], test.a))
        val right = SparkUtils.asSparkRow((test.b, Seq.empty[Int]))
        val comparator = Comparator(test.dataType, defaultEqualityParams)
        comparator.isFieldEqual(left, 1, right, 0) should be(test.equal)
        comparator.isFieldEqual(right, 0, left, 1) should be(test.equal)
      }
    }
  }

}
