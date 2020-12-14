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

package com.criteo.deepdiff.type_support.unsafe

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeArrayData, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}

private[deepdiff] abstract class UnsafeComparator[T] extends Serializable {
  @inline def compare(x: T, y: T): UnsafeComparison
}

private[deepdiff] object UnsafeComparator {
  import BinaryEquivalence._
  import UnsafeComparison._

  def array(elementBinaryEquivalence: BinaryEquivalence): UnsafeComparator[ArrayData] = {
    elementBinaryEquivalence match {
      case Undetermined =>
        new UnsafeArrayComparator {
          @inline def compareUnsafe(x: UnsafeArrayData, y: UnsafeArrayData): UnsafeComparison = Unknown
        }
      case BinaryEqualImpliesEqual =>
        new UnsafeArrayComparator {
          @inline def compareUnsafe(x: UnsafeArrayData, y: UnsafeArrayData): UnsafeComparison = {
            if (x.getSizeInBytes != y.getSizeInBytes || x != y) Unknown else Equal
          }
        }
      case BinaryEqualImpliesEqualAndDifferentSizeImpliesNotEqual =>
        new UnsafeArrayComparator {
          @inline def compareUnsafe(x: UnsafeArrayData, y: UnsafeArrayData): UnsafeComparison = {
            if (x.getSizeInBytes != y.getSizeInBytes) NotEqual else { if (x == y) Equal else Unknown }
          }
        }
      case EqualEquivalentToBinaryEqual =>
        new UnsafeArrayComparator {
          @inline def compareUnsafe(x: UnsafeArrayData, y: UnsafeArrayData): UnsafeComparison = {
            if (x.getSizeInBytes != y.getSizeInBytes || x != y) NotEqual else Equal
          }
        }
    }
  }

  def struct(binaryEquivalence: BinaryEquivalence): UnsafeComparator[InternalRow] = {
    binaryEquivalence match {
      case Undetermined =>
        new UnsafeRowComparator {
          @inline def compareUnsafe(x: UnsafeRow, y: UnsafeRow): UnsafeComparison = Unknown
        }
      case BinaryEqualImpliesEqual =>
        new UnsafeRowComparator {
          @inline def compareUnsafe(x: UnsafeRow, y: UnsafeRow): UnsafeComparison = {
            if (x.getSizeInBytes != y.getSizeInBytes || x != y) Unknown else Equal
          }
        }
      case BinaryEqualImpliesEqualAndDifferentSizeImpliesNotEqual =>
        new UnsafeRowComparator {
          @inline def compareUnsafe(x: UnsafeRow, y: UnsafeRow): UnsafeComparison = {
            if (x.getSizeInBytes != y.getSizeInBytes) NotEqual else { if (x == y) Equal else Unknown }
          }
        }
      case EqualEquivalentToBinaryEqual =>
        new UnsafeRowComparator {
          @inline def compareUnsafe(x: UnsafeRow, y: UnsafeRow): UnsafeComparison = {
            if (x.getSizeInBytes != y.getSizeInBytes || x != y) NotEqual else Equal
          }
        }
    }
  }

  def map(keyBinaryEquivalence: BinaryEquivalence,
          valueBinaryEquivalence: BinaryEquivalence): UnsafeComparator[MapData] = {
    require(keyBinaryEquivalence == EqualEquivalentToBinaryEqual)
    valueBinaryEquivalence match {
      case Undetermined =>
        new UnsafeMapComparator {
          @inline def compareUnsafeValues(equalKeys: Boolean,
                                          x: UnsafeArrayData,
                                          y: UnsafeArrayData): UnsafeComparison =
            if (equalKeys) OnlyMapKeysAreEqual else Unknown
        }
      case BinaryEqualImpliesEqual =>
        new UnsafeMapComparator {
          @inline def compareUnsafeValues(equalKeys: Boolean,
                                          x: UnsafeArrayData,
                                          y: UnsafeArrayData): UnsafeComparison = {
            if (equalKeys) {
              if (x.getSizeInBytes != y.getSizeInBytes || x != y) OnlyMapKeysAreEqual else Equal
            } else Unknown
          }
        }
      case BinaryEqualImpliesEqualAndDifferentSizeImpliesNotEqual | EqualEquivalentToBinaryEqual =>
        new UnsafeMapComparator {
          @inline def compareUnsafeValues(equalKeys: Boolean,
                                          x: UnsafeArrayData,
                                          y: UnsafeArrayData): UnsafeComparison = {
            if (x.getSizeInBytes != y.getSizeInBytes) NotEqual
            else {
              if (equalKeys) {
                if (x == y) Equal else OnlyMapKeysAreEqual
              } else Unknown
            }
          }
        }
    }
  }
}
