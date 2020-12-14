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

import com.criteo.deepdiff.KeyExample
import com.criteo.deepdiff.diff.{K, Kx}
import com.criteo.deepdiff.plan.key.KeyMaker
import com.criteo.deepdiff.raw_part.{RawDiffExampleBuilder, RawToFriendlyDiffExample}
import com.criteo.deepdiff.utils.{LeftRight, LeftRightFunctor}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.nio.ByteBuffer
import scala.language.higherKinds

private[deepdiff] abstract class ExampleFactory extends Serializable {
  type RawExample
  type FriendlyExample

  protected def rawToFriendly(rawExample: RawExample): FriendlyExample
  protected def getNotNullRaw(input: SpecializedGetters, ordinal: Int): RawExample
  private[example] def getFriendly(input: SpecializedGetters, ordinal: Int): FriendlyExample =
    rawToFriendly(getNotNullRaw(input, ordinal))

  protected final def anyRawToFriendly(x: Any): FriendlyExample = rawToFriendly(x.asInstanceOf[RawExample])
}

/** Base class for types for which the serialized example is the data itself. */
private final class PrimaryExampleFactory[T](getter: (SpecializedGetters, Int) => T) extends ExampleFactory {
  type RawExample = T
  type FriendlyExample = T
  protected def rawToFriendly(rawExample: RawExample): FriendlyExample = rawExample
  protected def getNotNullRaw(input: SpecializedGetters, ordinal: Int): T = getter(input, ordinal)
}

private[deepdiff] object ExampleFactory {
  object diffExample {
    def rawExampleBuilder[LR[+T] <: LeftRight[T]](ordinal: LR[Int], exampleFactory: LR[ExampleFactory])(implicit
        lr: LeftRightFunctor[LR]): RawDiffExampleBuilder[K, InternalRow, Kx, Any] = {
      val fieldGetters: LR[SpecializedGetters => Any] = ordinal
        .zip(exampleFactory)
        .map({
          case (ordinal, exampleFactory: ExampleFactory) =>
            exampleFactory.getNotNullRaw(_, ordinal)
        })
      new RawDiffExampleBuilder[K, InternalRow, Kx, Any] {
        private val maybeLeftGetter = fieldGetters.maybeLeft
        private val maybeRightGetter = fieldGetters.maybeRight

        @inline def asRawKeyExample(key: K): Kx = KeyMaker.buildBinaryKey(key)
        @inline def asRawLeftExample(left: InternalRow): Any =
          maybeLeftGetter.get(left)
        @inline def asRawRightExample(right: InternalRow): Any =
          maybeRightGetter.get(right)
      }
    }

    def rawToFriendly(keyMaker: KeyMaker,
                      exampleFactory: LeftRight[ExampleFactory]): RawToFriendlyDiffExample[Kx, Any, KeyExample, Any] =
      new RawToFriendlyDiffExample[Kx, Any, KeyExample, Any] {
        def rawToFriendlyKey(rawKey: Kx): KeyExample =
          keyMaker.buildExampleFromBinaryKey(ByteBuffer.wrap(rawKey.getBytes))
        def rawToFriendlyLeft(rawLeft: Any): Any = exampleFactory.maybeLeft.get.anyRawToFriendly(rawLeft)
        def rawToFriendlyRight(rawRight: Any): Any = exampleFactory.maybeRight.get.anyRawToFriendly(rawRight)
      }
  }

  def structArrayExampleFactory(structExample: StructExampleFactory): ArrayExampleFactory[StructExampleFactory] =
    ArrayExampleFactory(structExample)

  def structExampleFactory(rawSchema: StructType,
                           fieldExamples: Seq[StructExampleFactory.FieldExampleFactory]): StructExampleFactory =
    StructExampleFactory(numFields = rawSchema.length, fieldExamples)

  def apply(dataType: DataType): ExampleFactory = {
    dataType match {
      case BooleanType =>
        new PrimaryExampleFactory((input: SpecializedGetters, ordinal: Int) => input.getBoolean(ordinal))
      case ByteType =>
        new PrimaryExampleFactory((input: SpecializedGetters, ordinal: Int) => input.getByte(ordinal))
      case BinaryType =>
        new PrimaryExampleFactory((input: SpecializedGetters, ordinal: Int) => input.getBinary(ordinal))
      case ShortType =>
        new PrimaryExampleFactory((input: SpecializedGetters, ordinal: Int) => input.getShort(ordinal))
      case IntegerType =>
        new PrimaryExampleFactory((input: SpecializedGetters, ordinal: Int) => input.getInt(ordinal))
      case LongType =>
        new PrimaryExampleFactory((input: SpecializedGetters, ordinal: Int) => input.getLong(ordinal))
      case FloatType =>
        new PrimaryExampleFactory((input: SpecializedGetters, ordinal: Int) => input.getFloat(ordinal))
      case DoubleType =>
        new PrimaryExampleFactory((input: SpecializedGetters, ordinal: Int) => input.getDouble(ordinal))
      case StringType =>
        new ExampleFactory {
          type RawExample = UTF8String
          type FriendlyExample = String

          protected def rawToFriendly(rawExample: UTF8String): String = rawExample.toString

          protected def getNotNullRaw(input: SpecializedGetters, ordinal: Int): UTF8String =
            input.getUTF8String(ordinal).copy()

          override private[example] def getFriendly(input: SpecializedGetters, ordinal: Int): String =
            input.getUTF8String(ordinal).toString
        }
      case DateType =>
        new ExampleFactory {
          type RawExample = Int
          type FriendlyExample = String

          protected def rawToFriendly(rawExample: Int): String =
            DateTimeExampleUtils.dateToString(rawExample)
          protected def getNotNullRaw(input: SpecializedGetters, ordinal: Int): Int = input.getInt(ordinal)
        }
      case TimestampType =>
        new ExampleFactory {
          type RawExample = Long
          type FriendlyExample = String

          protected def rawToFriendly(rawExample: Long): String =
            DateTimeExampleUtils.timestampToString(rawExample)
          protected def getNotNullRaw(input: SpecializedGetters, ordinal: Int): Long = input.getLong(ordinal)
        }
      case at: ArrayType => ArrayExampleFactory(ExampleFactory(at.elementType))
      case mt: MapType =>
        MapExampleFactory(
          keyExample = ExampleFactory(mt.keyType),
          valueExample = ExampleFactory(mt.valueType)
        )
      case struct: StructType =>
        StructExampleFactory(
          numFields = struct.length,
          fieldExampleFactories = struct.fields.map(field =>
            StructExampleFactory
              .FieldExampleFactory(field.name, struct.fieldIndex(field.name), ExampleFactory(field.dataType)))
        )
      case dt =>
        throw new IllegalArgumentException(s"Unsupported type $dt")
    }
  }
}
