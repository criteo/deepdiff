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

package com.criteo.deepdiff.test_utils

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Magic land to generate InternalRow easily.
  */
class SparkUtilsSpec extends AnyFlatSpec with Matchers {
  import SparkUtils._

  implicit class UTF8StringHelper(val s: String) {
    def utf8: UTF8String = UTF8String.fromString(s)
  }

  "struct" should "generate StructType" in {
    struct("a" -> IntegerType, "b" -> DoubleType) should be(
      StructType(
        Seq(
          StructField("a", IntegerType),
          StructField("b", DoubleType)
        )))
  }

  "Dummy left and right schemas" should "have the same data" in {
    for (
      (l, r) <- Seq[(StructType, StructType)](
        (dummyLeftSchema, dummyRightSchema), {
          val l =
            dummyLeftSchema("exploded_array").dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
          val r =
            dummyRightSchema("exploded_array").dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
          (l, r)
        }, {
          val l = dummyLeftSchema("struct").dataType.asInstanceOf[StructType]
          val r = dummyRightSchema("struct").dataType.asInstanceOf[StructType]
          (l, r)
        }
      )
    ) {
      withClue(s"$l =?= $r") {
        l.length should be(r.length)
        l.fields.map(_.name) should contain theSameElementsAs r.fields.map(_.name)
      }
    }
  }

  "asSpark" should "support any kind of type" in {
    for (
      (value, expected) <- Seq[(Any, Any)](
        (null, null),
        (1, 1),
        (2f, 2f),
        (3d, 3d),
        (4L, 4L),
        ("5", "5".utf8),
        (6.toByte, 6.toByte),
        (7.toShort, 7.toShort),
        (true, true),
        (Array(10, 20), new GenericArrayData(Seq(10, 20))),
        (Seq(10d, 20d), new GenericArrayData(Seq(10d, 20d))),
        ("ab".getBytes, "ab".getBytes),
        ((1, 2f), new GenericInternalRow(Array[Any](1, 2f)))
      )
    ) {
      asSpark(value) should be(expected)
    }
  }

  "asSparkRow" should "support any kind of product/sequence" in {
    val expected = new GenericInternalRow(Array[Any](1, 2f, "3".utf8, true, new GenericArrayData(Array[Int](1, 2))))
    asSparkRow((1, 2f, "3", true, Array[Int](1, 2))) should be(expected)
    asSparkRow(Seq(1, 2f, "3", true, Array[Int](1, 2))) should be(expected)
  }

  "asSparkArray" should "support any kind of sequence" in {
//    asSparkArray(Seq(1, 2, 3)) should be(new GenericArrayData(Array[java.lang.Integer](1, 2, 3)))
    asSparkArray(Seq("x", "y", "z")) should be(new GenericArrayData(Array("x".utf8, "y".utf8, "z".utf8)))
    asSparkArray(Seq("4".getBytes(), "5".getBytes())) should be(
      new GenericArrayData(Seq("4".getBytes(), "5".getBytes())))
    asSparkArray(Seq(20L)) should be(new GenericArrayData(Seq(20L)))
    asSparkArray(Seq((10, "20"))) should be(
      new GenericArrayData(
        Seq(
          new GenericInternalRow(Array[Any](10, "20".utf8))
        )))
  }

  "Map" should "be supported" in {
    val expected = new ArrayBasedMapData(
      keyArray = new GenericArrayData(Seq("a".utf8, "b".utf8)),
      valueArray = new GenericArrayData(Seq(1, 2))
    )
    for (
      result <- Seq[MapData](
        asSparkMap(Seq(("a", 1), ("b", 2))),
        asSparkMap(Seq("a", "b"), Seq(1, 2)),
        asSpark(Map("a" -> 1, "b" -> 2)).asInstanceOf[MapData]
      )
    ) {
      result.keyArray() should be(expected.keyArray)
      result.valueArray() should be(expected.valueArray)
    }
  }

  "asExample" should "produce valid examples" in {
    asRecordExample(
      schema = struct(
        "a" -> IntegerType,
        "b" -> StringType,
        "c" -> MapType(StringType, DoubleType),
        "d" -> ArrayType(FloatType),
        "e" -> struct("x" -> BooleanType, "y" -> StringType)
      ),
      (1, "2", Map("X" -> 3d, "Y" -> 4d), Seq(5f, 6f), (false, "e"))
    ) should be(
      Map(
        "a" -> 1,
        "b" -> "2",
        "c" -> Map(
          "X" -> 3d,
          "Y" -> 4d
        ),
        "d" -> Seq(5f, 6f),
        "e" -> Map(
          "x" -> false,
          "y" -> "e"
        )
      ))
  }

  val l = LeftRow(1, 2, 3, 4, "a", true, Seq(6, null), null, Seq((8f, "b"), null), (9, "c", 10d))
  val r = RightRow(1, 2, 3, 4, "a", true, Seq(6, null), null, Seq((8f, "b"), null), (9, "c", 10d))
  val l2 = LeftRow(null, null, null, null, null, null, null, null, null, null)
  val r2 = RightRow(null, null, null, null, null, null, null, null, null, null)

  "DummyRows" should "generate valid spark rows and example" in {
    val expectedExample = Map(
      "int" -> 1,
      "long" -> 2L,
      "float" -> 3f,
      "double" -> 4d,
      "string" -> "a",
      "boolean" -> true,
      "array" -> Seq(6, null),
      "map" -> null,
      "exploded_array" -> Seq(
        Map(
          "a1" -> 8f,
          "a2" -> "b"
        ),
        null
      ),
      "struct" -> Map(
        "s1" -> 9,
        "s2" -> "c",
        "s3" -> 10d
      )
    )
    l.asExample should be(expectedExample)
    r.asExample should be(expectedExample)
    l.asSpark should be(
      new GenericInternalRow(Array[Any](
        1,
        2L,
        3f,
        4d,
        "a".utf8,
        true,
        new GenericArrayData(Seq(6, null)),
        null,
        new GenericArrayData(Seq(new GenericInternalRow(Array[Any](8f, "b".utf8)), null)),
        new GenericInternalRow(Array[Any](9, "c".utf8, 10d))
      )))
    r.asSpark should be(
      new GenericInternalRow(Array[Any](
        2L,
        null,
        "a".utf8,
        new GenericArrayData(Seq(6, null)),
        1,
        3f,
        new GenericArrayData(Seq(new GenericInternalRow(Array[Any]("b".utf8, 8f)), null)),
        4d,
        new GenericInternalRow(Array[Any]("c".utf8, 10d, 9)),
        true
      )))

    val expectedExample2 = Map(
      "int" -> null,
      "long" -> null,
      "float" -> null,
      "double" -> null,
      "string" -> null,
      "boolean" -> null,
      "array" -> null,
      "map" -> null,
      "exploded_array" -> null,
      "struct" -> null
    )
    l2.asExample should be(expectedExample2)
    r2.asExample should be(expectedExample2)
    l2.asSpark should be(new GenericInternalRow(Array[Any](null, null, null, null, null, null, null, null, null, null)))
    r2.asSpark should be(new GenericInternalRow(Array[Any](null, null, null, null, null, null, null, null, null, null)))
  }

  "asSparkRowAndExample" should "provide the same results as DummyRow" in {
    val (row, example) = asSparkRowAndExample(dummyLeftSchema, l.asInstanceOf[Product])
    row should be(l.asSpark)
    example should be(l.asExample)

    val (row2, example2) = asSparkRowAndExample(dummyLeftSchema, l2.asInstanceOf[Product])
    row2 should be(l2.asSpark)
    example2 should be(l2.asExample)
  }
}
