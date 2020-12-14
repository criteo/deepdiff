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

package com.criteo.deepdiff.type_support.example

import java.util.TimeZone

import com.criteo.deepdiff.test_utils.SparkUtils
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object ExampleFactorySpec {
  final case class TestCase[+T, +E](dataType: DataType, value: T, example: E)
  object TestCase {
    def apply[T](dataType: DataType, value: T): TestCase[T, T] = TestCase(dataType, value, value)
  }
}

final class ExampleFactorySpec extends AnyFlatSpec with Matchers {
  import ExampleFactorySpec._
  import SparkUtils._

  val structTestCase = TestCase(StructType(Seq(StructField("a", IntegerType), StructField("b", StringType))),
                                (1, "str"),
                                Map("a" -> 1, "b" -> "str"))
  val testCases = Seq(
    TestCase[Boolean](BooleanType, true),
    TestCase[Double](DoubleType, 1d),
    TestCase[Long](LongType, 1L),
    TestCase[Float](FloatType, 1f),
    TestCase[Short](ShortType, 1),
    TestCase[Int](IntegerType, 1),
    TestCase[String](StringType, "test"),
    TestCase[Array[Byte]](BinaryType, "test".getBytes()),
    TestCase[Byte](ByteType, 1),
    TestCase[Array[Int]](ArrayType(IntegerType), Array(1, 2, 3)),
    TestCase[Map[String, Int]](MapType(StringType, IntegerType), Map("a" -> 1)),
    TestCase[Int, String](DateType, 1, "1970-01-02"),
    TestCase[Int, String](DateType, 30, "1970-01-31"),
    TestCase[Long, String](TimestampType, 1393387038L * 1000000L, "2014-02-26 03:57:18"),
    TestCase[Long, String](TimestampType, 1596349422L * 1000000L, "2020-08-02 06:23:42"),
    structTestCase
  )

  "Examples" should "be generated for any kind of types" in {
    val defaultTz = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    for (test <- testCases) {
      withClue(test.toString) {
        val row = asSparkRow((Seq.empty[Int], test.value))
        val dtExample = ExampleFactory(test.dataType)
        dtExample.getFriendly(row, 1) should be(test.example)
      }
    }
    TimeZone.setDefault(defaultTz)
  }

  "structExample" should "return a valid ExampleFactory" in {
    val schema = struct("a" -> IntegerType, "b" -> StringType)
    val row = asSparkRow((Seq.empty[Int], (1, "str")))
    val expectedExample = Map("a" -> 1, "b" -> "str")
    for (
      dtExample <- Seq(
        ExampleFactory.structExampleFactory(
          schema,
          Seq(
            StructExampleFactory.FieldExampleFactory("a", 0, ExampleFactory(IntegerType)),
            StructExampleFactory.FieldExampleFactory("b", 1, ExampleFactory(StringType))
          )
        ),
        ExampleFactory(schema)
      )
    ) {
      dtExample.getFriendly(row, 1) should be(expectedExample)
    }
  }

  "structArrayExample" should "return a valid ExampleFactory" in {
    val row = asSparkRow((Seq.empty[Int], Seq((1, "str"), (2, "ing"))))
    val dtExample = ExampleFactory.structArrayExampleFactory(
      ExampleFactory(struct("a" -> IntegerType, "b" -> StringType)).asInstanceOf[StructExampleFactory]
    )
    val expectedExample = Seq(Map("a" -> 1, "b" -> "str"), Map("a" -> 2, "b" -> "ing"))
    dtExample.getFriendly(row, 1) should be(expectedExample)
  }
}
