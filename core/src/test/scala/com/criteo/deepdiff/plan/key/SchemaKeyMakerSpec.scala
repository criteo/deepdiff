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

package com.criteo.deepdiff.plan.key

import com.criteo.deepdiff.FieldsKeyExample
import com.criteo.deepdiff.plan.field.FieldPath
import com.criteo.deepdiff.plan.{FieldUtils, KeyDiffField}
import com.criteo.deepdiff.utils.Common
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.types._
import org.scalatest.AppendedClues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.ByteBuffer
import java.util
import java.util.TimeZone
import scala.util.Random

trait SchemaKeyMakerSpec extends AnyFlatSpec with Matchers with AppendedClues {
  import FieldUtils.commonKey
  import com.criteo.deepdiff.test_utils.SparkUtils._

  private[key] abstract class SchemaKeyMaker {
    def buildLeft(input: SpecializedGetters): Array[Byte]
    def buildRight(input: SpecializedGetters): Array[Byte]
    def deserialize(buffer: ByteBuffer): FieldsKeyExample

    def checkLeftAndRight(row: InternalRow, expected: FieldsKeyExample): Unit = {
      val rawLeft = buildLeft(row)
      val rawRight = buildRight(row)
      shouldBeEqual(rawLeft, rawRight)
      deserialize(rawLeft) should be(expected)
    }

    def checkLeftAndRight(left: InternalRow, right: InternalRow, expected: FieldsKeyExample): Unit = {
      val rawLeft = buildLeft(left)
      val rawRight = buildRight(right)
      shouldBeEqual(rawLeft, rawRight)
      deserialize(rawLeft) should be(expected)
    }

    def deserialize(key: Array[Byte]): FieldsKeyExample = deserialize(ByteBuffer.wrap(key))
  }

  def reliableKeyExtractor(buildKeyMaker: (Common[StructType], Seq[KeyDiffField[Common]]) => SchemaKeyMaker) {
    def buildTwinKeyMaker(schema: StructType, keys: Seq[String]): SchemaKeyMaker =
      buildKeyMaker(Common.twin(schema), keys.map(commonKey))

    it should "throw errors on invalid keys" in {
      for (
        key <- Seq(
          "unknown", // field does not exist
          "int.field", // int is not a struct
          "struct.s99", // s99 does not exist
          // not supported key types
          "exploded_array",
          "exploded_array.a1",
          "map",
          "struct",
          "array"
        )
      ) {
        a[IllegalArgumentException] should be thrownBy {
          buildTwinKeyMaker(
            schema = struct(
              "int" -> IntegerType,
              "struct" -> struct("s1" -> StringType),
              "exploded_array" -> ArrayType(
                struct(
                  "a1" -> DoubleType
                )),
              "array" -> ArrayType(IntegerType),
              "map" -> MapType(StringType, IntegerType)
            ),
            key :: Nil
          )
        }
      }

      a[IllegalArgumentException] should be thrownBy {
        buildTwinKeyMaker(
          schema = struct("int" -> IntegerType),
          Seq("int", "int")
        )
      }
    }

    it should "support arbitrary long keys" in {
      val keyMaker = buildTwinKeyMaker(schema = struct("string" -> StringType), "string" :: Nil)
      val r = new Random()
      for (length <- 10 until 1000 by 10) {
        val str = r.nextString(length)
        keyMaker.checkLeftAndRight(sparkRow(str), FieldsKeyExample(Map("string" -> Some(str))))
      }
    }

    it should "not keep any traces of previous keys" in {
      val keyMaker = buildTwinKeyMaker(schema = struct("string" -> StringType), "string" :: Nil)
      for (value <- Seq[String]("test", null, "")) {
        val row = sparkRow(value)
        val leftKey = keyMaker.buildLeft(row)
        shouldBeEqual(keyMaker.buildRight(row), leftKey)
        for (previousValue <- Seq[String]("1", null, "test", "")) {
          withClue(s"$previousValue -> $value") {
            val previousRow = sparkRow(previousValue)
            keyMaker.buildLeft(previousRow)
            shouldBeEqual(keyMaker.buildLeft(row), leftKey)
            keyMaker.buildRight(previousRow)
            shouldBeEqual(keyMaker.buildRight(row), leftKey)
          }
        }
      }
    }

    it should "support at least 32 keys" in {
      for (n <- 1 until 32) {
        withClue(s"n = $n\n") {
          val keys = (0 until n).map(_.toString)
          val keyMaker = buildTwinKeyMaker(schema = struct(keys.map(_ -> StringType): _*), keys)
          val allValues = keys.map(_ * 2).toArray
          keyMaker.checkLeftAndRight(row = asSparkRow(allValues),
                                     expected = FieldsKeyExample(keys.zip(allValues.map(Option(_))).toMap))
          for (k <- 0 until n) {
            val values = allValues.clone()
            values(k) = null
            keyMaker.checkLeftAndRight(
              row = asSparkRow(values),
              expected = FieldsKeyExample(keys.zip(values.map(Option(_))).toMap)
            )
          }
        }
      }
    }

    it should "support nested keys" in {
      val schema = struct("struct" -> struct("key" -> StringType))
      buildTwinKeyMaker(schema, "struct.key" :: Nil).checkLeftAndRight(
        row = sparkRow(sparkRow("test")),
        expected = FieldsKeyExample(Map("struct.key" -> Some("test")))
      )
    }

    it should "support at least 32 nested keys" in {
      for (n <- 1 until 32) {
        withClue(s"n = $n\n") {
          val keys = (0 until n).map(i => s"struct.$i")
          val schema = struct("struct" -> struct((0 until n).map(_.toString -> StringType): _*))
          val keyMaker = buildTwinKeyMaker(schema, keys)
          val allValues = (0 until n).map(_.toString * 2).toArray
          keyMaker.checkLeftAndRight(row = sparkRow(asSparkRow(allValues)),
                                     expected = FieldsKeyExample(keys.zip(allValues.map(Option(_))).toMap))

          val allNull = Array.fill[String](n)(null)
          val nullKeyExample = FieldsKeyExample(keys.zip(allNull.map(Option(_))).toMap)
          keyMaker.checkLeftAndRight(row = sparkRow(asSparkRow(allNull)), expected = nullKeyExample)
          keyMaker.checkLeftAndRight(row = sparkRow(null), expected = nullKeyExample)

          for (k <- 0 until n) {
            val oneNullValue = allValues.clone()
            oneNullValue(k) = null
            keyMaker.checkLeftAndRight(row = sparkRow(asSparkRow(oneNullValue)),
                                       expected = FieldsKeyExample(keys.zip(oneNullValue.map(Option(_))).toMap))
          }
        }
      }
    }

    object multiTypesData {
      val fields: Seq[(String, DataType)] = Seq[(String, DataType)](
        "short" -> ShortType,
        "int" -> IntegerType,
        "long" -> LongType,
        "float" -> FloatType,
        "double" -> DoubleType,
        "string" -> StringType,
        "boolean" -> BooleanType,
        "byte" -> ByteType,
        "binary" -> BinaryType
      )
      val possibleValues = Map(
        "short" -> Seq[java.lang.Short](4.toShort, null, Short.MaxValue),
        "int" -> Seq[java.lang.Integer](1, null, Integer.MAX_VALUE),
        "long" -> Seq[java.lang.Long](-1L, null, Long.MinValue),
        "float" -> Seq[java.lang.Float](1.2f, Float.MinValue, null, Float.NegativeInfinity, Float.NaN),
        "double" -> Seq[java.lang.Double](-3.2d, Double.PositiveInfinity, Double.NaN, null, Double.MaxValue),
        "string" -> Seq[String]("test", null, "hello again", ""),
        "boolean" -> Seq[java.lang.Boolean](true, null, false),
        "byte" -> Seq[java.lang.Byte](1.toByte, null, 2.toByte),
        "binary" -> Seq[Array[Byte]](Array[Byte](1.toByte), null, Array[Byte](2.toByte))
      )
      val rowsAndValues: Seq[(InternalRow, Seq[Any])] = (for {
        short <- possibleValues("short")
        int <- possibleValues("int")
        long <- possibleValues("long")
        float <- possibleValues("float")
        double <- possibleValues("double")
        string <- possibleValues("string")
        boolean <- possibleValues("boolean")
        byte <- possibleValues("byte")
        binary <- possibleValues("binary")
      } yield Seq(short, int, long, float, double, string, boolean, byte, binary)).map({ values =>
        (asSparkRow(values), values)
      })
    }

    it should "generate unique keys" in {
      import multiTypesData._
      val schema = struct(fields: _*)
      for (
        n <- 1 until fields.length;
        keys <- schema.fieldNames.combinations(n)
      ) {
        val keyMaker = buildTwinKeyMaker(schema, keys)
        val builder = Set.newBuilder[ByteBuffer]
        for ((row, values) <- rowsAndValues) {
          try {
            val leftKey = keyMaker.buildLeft(row)
            val rightKey = keyMaker.buildRight(row)
            assert(util.Arrays.equals(leftKey, rightKey)) // should is too slow
            builder += ByteBuffer.wrap(leftKey)
          } catch {
            case t: Throwable =>
              println(s"ROW:\n${asRecordExample(schema, values)}\n")
              throw t
          }
        }
        val expectedNumberOfKeys = keys.map(possibleValues(_).length).product
        val generatedKeys = builder.result
        generatedKeys.size should be(expectedNumberOfKeys) withClue new Object {
          // lazy clue message
          override def toString: String = {
            s"\n$keys\n${generatedKeys.map(buffer => prettyHex(buffer.array)).toSeq.sorted.mkString("\n\n")}"
          }
        }
      }
    }

    it should "be able to deserialize any type of content" in {
      import multiTypesData._
      val schema = struct(fields: _*)
      val keyMaker = buildTwinKeyMaker(schema, schema.fieldNames)
      val NaN = new Object()
      def fixFields(x: Any): Any =
        x match {
          case b: Array[Byte]       => fastHex(b)
          case f: Float if f.isNaN  => NaN
          case d: Double if d.isNaN => NaN
          case x                    => x
        }
      for ((row, values) <- rowsAndValues) {
        val expected = fields.map(_._1).zip(values.map(fixFields).map(Option(_))).toMap
        val rawLeft = keyMaker.buildLeft(row)
        val rawRight = keyMaker.buildRight(row)
        shouldBeEqual(rawLeft, rawRight)
        // comparing string representation as Array[Byte] do not implement value equality.
        keyMaker.deserialize(rawLeft) match {
          case FieldsKeyExample(fields) =>
            fields.mapValues(_.map(fixFields)) should be(expected)
        }
      }
    }

    it should "not depend on the layout of the schema" in {
      import multiTypesData._
      var previousKey: Array[Byte] = null
      val keys = "string" :: Nil
      val expected = FieldsKeyExample(Map("string" -> possibleValues("string").headOption))
      for (schema <- fields.permutations.map(struct(_: _*))) {
        val keyMaker = buildTwinKeyMaker(schema, keys)
        val row = asSparkRow(schema.fields.map(f => possibleValues(f.name).head))
        keyMaker.checkLeftAndRight(row, expected)
        val leftKey = keyMaker.buildLeft(row)
        if (previousKey == null) previousKey = leftKey
        else {
          util.Arrays.equals(previousKey, leftKey) should be(true)
        }
      }
    }

    it should "support date & time types" in {
      val defaultTz = TimeZone.getDefault
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
      val schema = struct(
        "date" -> DateType,
        "timestamp" -> TimestampType
      )
      val keyMaker = buildKeyMaker(
        Common.twin(schema),
        Seq(
          commonKey("date"),
          commonKey("timestamp")
        )
      )
      keyMaker.checkLeftAndRight(
        row = sparkRow(1, 1000000L),
        expected = FieldsKeyExample(
          Map(
            "date" -> "1970-01-02",
            "timestamp" -> "1970-01-01 00:00:01"
          ).mapValues(Option(_)))
      )
      TimeZone.setDefault(defaultTz)
    }

    it should "support different layout for left and right" in {
      val keyMaker = buildKeyMaker(
        Common(
          left = struct(
            "intL" -> IntegerType,
            "string" -> StringType,
            "stringL" -> StringType,
            "long" -> LongType,
            "double" -> DoubleType
          ),
          right = struct(
            "double" -> DoubleType,
            "stringR" -> StringType,
            "long" -> LongType,
            "intR" -> IntegerType,
            "string" -> StringType
          )
        ),
        Seq(
          KeyDiffField(
            fullName = "int",
            relativeRawPath = Common(
              left = FieldPath("intL"),
              right = FieldPath("intR")
            )
          ),
          KeyDiffField(
            fullName = "string",
            relativeRawPath = Common.twin(FieldPath("string"))
          ),
          KeyDiffField(
            fullName = "string v2",
            relativeRawPath = Common(
              left = FieldPath("stringL"),
              right = FieldPath("stringR")
            )
          ),
          commonKey("long"),
          commonKey("double")
        )
      )
      keyMaker.checkLeftAndRight(
        left = sparkRow(1, "2", "3L", 4L, 5d),
        right = sparkRow(5d, "3L", 4L, 1, "2"),
        expected = FieldsKeyExample(
          Map(
            "int" -> 1,
            "string" -> "2",
            "string v2" -> "3L",
            "long" -> 4L,
            "double" -> 5d
          ).mapValues(Option(_)))
      )
    }
  }

  private def shouldBeEqual(result: Array[Byte], expected: Array[Byte]): Unit = {
    util.Arrays.equals(result, expected) should be(true) withClue new Object {
      override def toString: String =
        s"RESULT:\n${prettyHex(result)}\n\nEXPECTED:\n${prettyHex(expected)}\n"
    }
  }

  private def prettyHex(x: Array[Byte]): String =
    x.map("%02X".format(_))
      .grouped(8)
      .map(_.mkString(" "))
      .zipWithIndex
      .map({
        case (raw, index) => f"${index * 8}%03d | $raw%-23s"
      })
      .mkString("\n")

  private val HEX_ARRAY = "0123456789ABCDEF".toCharArray
  private def fastHex(bytes: Array[Byte]): String = {
    val hexChars = new Array[Char](bytes.length * 2)
    var j = 0
    while (j < bytes.length) {
      val v = bytes(j) & 0xff
      hexChars(j * 2) = HEX_ARRAY(v >>> 4)
      hexChars(j * 2 + 1) = HEX_ARRAY(v & 0x0f)
      j += 1
    }
    new String(hexChars)
  }
}
