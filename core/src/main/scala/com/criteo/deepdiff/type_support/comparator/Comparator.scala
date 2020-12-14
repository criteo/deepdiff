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

import com.criteo.deepdiff.diff.{K, R}
import com.criteo.deepdiff.raw_part.{CommonRawDiffsPartBuilder, KindOfDiff}
import com.criteo.deepdiff.type_support.unsafe.{BinaryEquivalence, UnsafeComparator}
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.nio.ByteBuffer
import java.util

/** Helper class to handle data for a specific [[DataType]]
  */
private[deepdiff] abstract class Comparator extends Serializable {
  @inline def addComparisonResult(key: K,
                                  left: R,
                                  leftAnyNull: Boolean,
                                  leftOrdinal: Int,
                                  right: R,
                                  rightAnyNull: Boolean,
                                  rightOrdinal: Int,
                                  fieldDiffs: CommonRawDiffsPartBuilder[K, R, _, _]): KindOfDiff

  // Using x and y from here on, as we're not only comparing left to right but also left to left and the opposite for
  // multiple matches.
  @inline def isFieldEqual(x: SpecializedGetters, xOrdinal: Int, y: SpecializedGetters, yOrdinal: Int): Boolean =
    isFieldEqual(x, xOrdinal, xAnyNull = true, y, yOrdinal, yAnyNull = true)

  @inline def isFieldEqual(x: SpecializedGetters,
                           xOrdinal: Int,
                           xAnyNull: Boolean,
                           y: SpecializedGetters,
                           yOrdinal: Int,
                           yAnyNull: Boolean): Boolean
}

private[comparator] sealed abstract class PrimaryComparator extends Comparator {
  @inline protected def isNotNullFieldEqual(x: SpecializedGetters,
                                            xOrdinal: Int,
                                            y: SpecializedGetters,
                                            yOrdinal: Int): Boolean

  @inline final def addComparisonResult(key: K,
                                        left: R,
                                        leftAnyNull: Boolean,
                                        leftOrdinal: Int,
                                        right: R,
                                        rightAnyNull: Boolean,
                                        rightOrdinal: Int,
                                        fieldDiffs: CommonRawDiffsPartBuilder[K, R, _, _]): KindOfDiff = {
    val isLeftNull = leftAnyNull && left.isNullAt(leftOrdinal)
    val isRightNull = rightAnyNull && right.isNullAt(rightOrdinal)
    if (isLeftNull != isRightNull) {
      if (isLeftNull) {
        fieldDiffs.rightOnly(key, right)
      } else {
        fieldDiffs.leftOnly(key, left)
      }
    } else if (isLeftNull || isNotNullFieldEqual(left, leftOrdinal, right, rightOrdinal)) {
      fieldDiffs.identical()
    } else {
      fieldDiffs.different(key, left, right)
    }
  }

  @inline def isFieldEqual(x: SpecializedGetters,
                           xOrdinal: Int,
                           xAnyNull: Boolean,
                           y: SpecializedGetters,
                           yOrdinal: Int,
                           yAnyNull: Boolean): Boolean = {
    val isXNull = xAnyNull && x.isNullAt(xOrdinal)
    val isYNull = yAnyNull && y.isNullAt(yOrdinal)
    if (isXNull != isYNull) false
    else if (isXNull) true
    else isNotNullFieldEqual(x, xOrdinal, y, yOrdinal)
  }
}

private[comparator] abstract class ObjectComparator[T] extends Comparator {
  @inline protected def get(input: SpecializedGetters, ordinal: Int): T
  @inline protected def isEqual(x: T, y: T): Boolean

  @inline final def addComparisonResult(key: K,
                                        left: R,
                                        leftAnyNull: Boolean,
                                        leftOrdinal: Int,
                                        right: R,
                                        rightAnyNull: Boolean,
                                        rightOrdinal: Int,
                                        fieldDiffs: CommonRawDiffsPartBuilder[K, R, _, _]): KindOfDiff = {
    val l = get(left, leftOrdinal)
    val r = get(right, rightOrdinal)
    if (l == null) {
      if (r == null) {
        fieldDiffs.identical()
      } else {
        fieldDiffs.rightOnly(key, right)
      }
    } else if (r == null) {
      fieldDiffs.leftOnly(key, left)
    } else {
      if (isEqual(l, r)) fieldDiffs.identical()
      else fieldDiffs.different(key, left, right)
    }
  }

  @inline def isFieldEqual(x: SpecializedGetters,
                           xOrdinal: Int,
                           xAnyNull: Boolean,
                           y: SpecializedGetters,
                           yOrdinal: Int,
                           yAnyNull: Boolean): Boolean = {
    val xx = get(x, xOrdinal)
    val yy = get(y, yOrdinal)
    (xx == null && yy == null) || (xx != null && yy != null && isEqual(xx, yy))
  }
}

/** Used in two context: AtomicDiffFields and comparison of multiple matches. */
private[deepdiff] object Comparator {
  def recordComparator(schema: StructType,
                       fieldComparators: Seq[StructComparator.FieldComparator],
                       binaryEquivalence: BinaryEquivalence): StructComparator = {
    StructComparator(
      numFields = schema.length,
      fieldComparators = fieldComparators.toArray,
      unsafeRowComparator = UnsafeComparator.struct(binaryEquivalence)
    )
  }

  def recordArrayComparator(structComparator: StructComparator,
                            binaryEquivalence: BinaryEquivalence): ArrayComparator = {
    ArrayComparator(
      structComparator,
      unsafeArrayComparator = UnsafeComparator.array(binaryEquivalence)
    )
  }

  def apply(dataType: DataType, params: EqualityParams): Comparator = {
    dataType match {
      case array: ArrayType =>
        ArrayComparator(Comparator(array.elementType, params),
                        unsafeArrayComparator = UnsafeComparator.array(BinaryEquivalence(array, params)))
      case map: MapType =>
        MapComparator(
          notNullKeyAccessor = buildHashMapNotNullKeyAccessor(map.keyType),
          keyComparator = Comparator(map.keyType, EqualityParams.strict),
          valueComparator = Comparator(map.valueType, params),
          unsafeMapComparator = UnsafeComparator.map(
            BinaryEquivalence(map.keyType, EqualityParams.strict),
            BinaryEquivalence(map.valueType, params)
          )
        )
      case schema: StructType =>
        StructComparator(
          numFields = schema.length,
          fieldComparators = schema
            .map(field => {
              StructComparator.FieldComparator(
                ordinal = schema.fieldIndex(field.name),
                Comparator(field.dataType, params)
              )
            })
            .toArray,
          unsafeRowComparator = UnsafeComparator.struct(BinaryEquivalence(schema, params))
        )
      case StringType =>
        new ObjectComparator[UTF8String] {
          @inline def get(input: SpecializedGetters, ordinal: Int): UTF8String =
            input.getUTF8String(ordinal)
          @inline def isEqual(x: UTF8String, y: UTF8String): Boolean = x == y
        }
      case BinaryType =>
        new ObjectComparator[Array[Byte]] {
          @inline def get(input: SpecializedGetters, ordinal: Int): Array[Byte] = input.getBinary(ordinal)
          @inline def isEqual(x: Array[Byte], y: Array[Byte]): Boolean =
            util.Arrays.equals(x, y)
        }
      case BooleanType =>
        new PrimaryComparator {
          @inline def isNotNullFieldEqual(x: SpecializedGetters,
                                          xOrdinal: Int,
                                          y: SpecializedGetters,
                                          yOrdinal: Int): Boolean =
            x.getBoolean(xOrdinal) == y.getBoolean(yOrdinal)
        }
      case ByteType =>
        new PrimaryComparator {
          @inline def isNotNullFieldEqual(x: SpecializedGetters,
                                          xOrdinal: Int,
                                          y: SpecializedGetters,
                                          yOrdinal: Int): Boolean =
            x.getByte(xOrdinal) == y.getByte(yOrdinal)
        }
      case DateType =>
        new PrimaryComparator {
          @inline def isNotNullFieldEqual(x: SpecializedGetters,
                                          xOrdinal: Int,
                                          y: SpecializedGetters,
                                          yOrdinal: Int): Boolean =
            x.getInt(xOrdinal) == y.getInt(yOrdinal)
        }
      case TimestampType =>
        new PrimaryComparator {
          @inline def isNotNullFieldEqual(x: SpecializedGetters,
                                          xOrdinal: Int,
                                          y: SpecializedGetters,
                                          yOrdinal: Int): Boolean =
            x.getLong(xOrdinal) == y.getLong(yOrdinal)
        }
      case ShortType =>
        new PrimaryComparator {
          private val numericEquality = params.numericEquality
          @inline def isNotNullFieldEqual(x: SpecializedGetters,
                                          xOrdinal: Int,
                                          y: SpecializedGetters,
                                          yOrdinal: Int): Boolean =
            numericEquality.isEqual(x.getShort(xOrdinal), y.getShort(yOrdinal))
        }
      case IntegerType =>
        new PrimaryComparator {
          private val numericEquality = params.numericEquality
          @inline def isNotNullFieldEqual(x: SpecializedGetters,
                                          xOrdinal: Int,
                                          y: SpecializedGetters,
                                          yOrdinal: Int): Boolean =
            numericEquality.isEqual(x.getInt(xOrdinal), y.getInt(yOrdinal))
        }
      case LongType =>
        new PrimaryComparator {
          private val numericEquality = params.numericEquality
          @inline def isNotNullFieldEqual(x: SpecializedGetters,
                                          xOrdinal: Int,
                                          y: SpecializedGetters,
                                          yOrdinal: Int): Boolean =
            numericEquality.isEqual(x.getLong(xOrdinal), y.getLong(yOrdinal))
        }
      case FloatType =>
        new PrimaryComparator {
          private val numericEquality = params.numericEquality
          @inline def isNotNullFieldEqual(x: SpecializedGetters,
                                          xOrdinal: Int,
                                          y: SpecializedGetters,
                                          yOrdinal: Int): Boolean =
            numericEquality.isEqual(x.getFloat(xOrdinal), y.getFloat(yOrdinal))
        }
      case DoubleType =>
        new PrimaryComparator {
          private val numericEquality = params.numericEquality
          @inline def isNotNullFieldEqual(x: SpecializedGetters,
                                          xOrdinal: Int,
                                          y: SpecializedGetters,
                                          yOrdinal: Int): Boolean =
            numericEquality.isEqual(x.getDouble(xOrdinal), y.getDouble(yOrdinal))
        }
      case dt =>
        throw new IllegalArgumentException(s"Unsupported type $dt")
    }
  }

  private def buildHashMapNotNullKeyAccessor(keyType: DataType): (SpecializedGetters, Int) => Any =
    keyType match {
      case BinaryType =>
        (input: SpecializedGetters, ordinal: Int) => {
          val b = input.getBinary(ordinal)
          assert(b != null)
          ByteBuffer.wrap(b)
        }
      case StringType =>
        (input: SpecializedGetters, ordinal: Int) => {
          val s = input.getUTF8String(ordinal)
          assert(s != null)
          s
        }
      case ByteType =>
        (input: SpecializedGetters, ordinal: Int) => input.getByte(ordinal)
      case ShortType =>
        (input: SpecializedGetters, ordinal: Int) => input.getShort(ordinal)
      case IntegerType =>
        (input: SpecializedGetters, ordinal: Int) => input.getInt(ordinal)
      case LongType =>
        (input: SpecializedGetters, ordinal: Int) => input.getLong(ordinal)
    }
}
