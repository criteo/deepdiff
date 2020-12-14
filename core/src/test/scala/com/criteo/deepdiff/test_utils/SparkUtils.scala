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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
  * Magic land to generate InternalRow easily.
  */
object SparkUtils { self =>

  def struct(f: (String, DataType)*): StructType =
    StructType(f.map({ case (name, tpe) => StructField(name, tpe) }))

  val dummyLeftSchema: StructType = struct(
    "int" -> IntegerType, // 0
    "long" -> LongType, // 1
    "float" -> FloatType, // 2
    "double" -> DoubleType, // 3
    "string" -> StringType, // 4
    "boolean" -> BooleanType, // 5
    "array" -> ArrayType(IntegerType), // 6
    "map" -> MapType(StringType, IntegerType), // 7
    "exploded_array" -> ArrayType(struct("a1" -> FloatType, "a2" -> StringType)), // 8
    "struct" -> struct("s1" -> IntegerType, "s2" -> StringType, "s3" -> DoubleType) // 9
  )

  val dummyRightSchema: StructType = struct(
    "long" -> LongType, // 0
    "map" -> MapType(StringType, IntegerType), // 1
    "string" -> StringType, // 2
    "array" -> ArrayType(IntegerType), // 3
    "int" -> IntegerType, // 4
    "float" -> FloatType, // 5
    "exploded_array" -> ArrayType(struct("a2" -> StringType, "a1" -> FloatType)), // 6
    "double" -> DoubleType, // 7
    "struct" -> struct("s2" -> StringType, "s3" -> DoubleType, "s1" -> IntegerType), // 8
    "boolean" -> BooleanType // 9
  )

  sealed trait DummyRow {
    def int: java.lang.Integer
    def long: java.lang.Long
    def float: java.lang.Float
    def double: java.lang.Double
    def string: String
    def boolean: java.lang.Boolean
    def array: Seq[java.lang.Integer]
    def map: Map[String, java.lang.Integer]
    def exploded_array: Seq[(java.lang.Float, String)]
    def struct: (java.lang.Integer, String, java.lang.Double)

    protected def schema: StructType

    lazy val asSpark: InternalRow = {
      asSparkRow(schema.map(_.name match {
        case "exploded_array" =>
          if (exploded_array == null) null
          else {
            val nestedSchema =
              schema("exploded_array").dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
            val nestedExample = asExample("exploded_array").asInstanceOf[Seq[Map[String, Any]]]
            self.asSpark(nestedExample.map(e => if (e == null) null else asSparkRow(nestedSchema.map(f => e(f.name)))))
          }
        case "struct" =>
          if (struct == null) null
          else {
            val nestedSchema = schema("struct").dataType.asInstanceOf[StructType]
            val nestedExample = asExample("struct").asInstanceOf[Map[String, Any]]
            asSparkRow(nestedSchema.map(f => nestedExample(f.name)))
          }
        case name => self.asSpark(asExample(name))
      }))
    }

    lazy val asExample: Map[String, Any] = Map(
      "int" -> int,
      "long" -> long,
      "float" -> float,
      "double" -> double,
      "string" -> string,
      "boolean" -> boolean,
      "array" -> array,
      "map" -> map,
      "exploded_array" -> {
        if (exploded_array == null) null
        else exploded_array.map(x => if (x == null) null else Map("a1" -> x._1, "a2" -> x._2))
      },
      "struct" -> {
        if (struct == null) null
        else
          Map(
            "s1" -> struct._1,
            "s2" -> struct._2,
            "s3" -> struct._3
          )
      }
    )
  }

  object DummyRow {
    def leftAndRight(a: java.lang.Integer,
                     b: java.lang.Long,
                     c: java.lang.Float,
                     d: java.lang.Double,
                     e: String,
                     f: java.lang.Boolean,
                     g: Seq[java.lang.Integer],
                     h: Map[String, java.lang.Integer],
                     i: Seq[(java.lang.Float, String)],
                     j: (java.lang.Integer, String, java.lang.Double)): (LeftRow, RightRow) = {
      val left = LeftRow(a, b, c, d, e, f, g, h, i, j)
      val right = RightRow(a, b, c, d, e, f, g, h, i, j)
      (left, right)
    }
  }

  final case class LeftRow(int: java.lang.Integer,
                           long: java.lang.Long,
                           float: java.lang.Float,
                           double: java.lang.Double,
                           string: String,
                           boolean: java.lang.Boolean,
                           array: Seq[java.lang.Integer],
                           map: Map[String, java.lang.Integer],
                           exploded_array: Seq[(java.lang.Float, String)],
                           struct: (java.lang.Integer, String, java.lang.Double))
      extends DummyRow {
    protected val schema = dummyLeftSchema
  }

  final case class RightRow(int: java.lang.Integer,
                            long: java.lang.Long,
                            float: java.lang.Float,
                            double: java.lang.Double,
                            string: String,
                            boolean: java.lang.Boolean,
                            array: Seq[java.lang.Integer],
                            map: Map[String, java.lang.Integer],
                            exploded_array: Seq[(java.lang.Float, String)],
                            struct: (java.lang.Integer, String, java.lang.Double))
      extends DummyRow {
    protected val schema = dummyRightSchema
  }

  def asSparkRowAndExample(schema: StructType, e: Product): (InternalRow, Map[String, Any]) = {
    require(schema.length == e.productArity)
    (asSparkRow(e), asRecordExample(schema, e))
  }

  def asRecordExample(schema: StructType, p: Product): Map[String, Any] =
    asRecordExample(schema, p.productIterator.toSeq)

  def asRecordExample(schema: StructType, values: Seq[Any]): Map[String, Any] = {
    schema
      .zip(values)
      .map({
        case (field, element) =>
          field.name -> {
            if (element != null) {
              field.dataType match {
                case nestedSchema: StructType =>
                  asRecordExample(nestedSchema, element.asInstanceOf[Product])
                case at: ArrayType =>
                  val array = element.asInstanceOf[Seq[Any]]
                  at.elementType match {
                    case nestedSchema: StructType =>
                      array.map(x => if (x != null) asRecordExample(nestedSchema, x.asInstanceOf[Product]) else null)
                    case _ => array
                  }
                case mp: MapType =>
                  val mapData = element.asInstanceOf[Map[Any, Any]]
                  mp.valueType match {
                    case nestedSchema: StructType =>
                      mapData.map({
                        case (k, v) =>
                          k -> (if (v != null) asRecordExample(nestedSchema, v.asInstanceOf[Product]) else null)
                      })
                    case _ => mapData
                  }
                case _ => element
              }
            } else {
              null
            }
          }
      })
      .toMap
  }

  def asSpark(e: Any): Any =
    e match {
      case null           => null
      case v: DummyRow    => v.asSpark
      case v: Boolean     => boolean2Boolean(v)
      case v: Byte        => byte2Byte(v)
      case v: Int         => int2Integer(v)
      case v: Short       => short2Short(v)
      case v: Long        => long2Long(v)
      case v: Float       => float2Float(v)
      case v: Double      => double2Double(v)
      case v: String      => UTF8String.fromString(v)
      case v: Map[_, _]   => asSparkMap(v.keys.toSeq, v.values.toSeq)
      case v: Array[Byte] => v
      case v: Seq[_]      => asSparkArray(v)
      case v: Array[_]    => asSparkArray(v)
      case v: Product     => asSparkRow(v)
      case v              => v
    }

  def asSparkMap(keys: Seq[_], values: Seq[_]): MapData = {
    require(keys.length == values.length)
    new ArrayBasedMapData(keyArray = asSparkArray(keys), valueArray = asSparkArray(values))
  }

  def asSparkMap(pairs: Seq[(_, _)]): MapData =
    asSparkMap(pairs.map(_._1), pairs.map(_._2))

  def asSparkArray(values: Seq[_]): ArrayData =
    new GenericArrayData(values.map(asSpark))

  def sparkRow(values: Any*): InternalRow =
    new GenericInternalRow(values.map(asSpark).toArray[Any])

  def asSparkRow(row: Seq[Any]): InternalRow =
    new GenericInternalRow(row.map(asSpark).toArray[Any])

  def asSparkRow(e: Product): InternalRow =
    new GenericInternalRow(e.productIterator.map(asSpark).toArray[Any])
}
