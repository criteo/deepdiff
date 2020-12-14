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

package com.criteo.deepdiff.diff.accumulators

import com.criteo.deepdiff.diff.{K, Kx, LeftRightRawDiffsPartBuilders, R}
import com.criteo.deepdiff.plan.ArrayStructDiffField
import com.criteo.deepdiff.raw_part.{KindOfDiff, LeftRightRawDiffsPart}
import com.criteo.deepdiff.type_support.unsafe.UnsafeComparator
import com.criteo.deepdiff.type_support.unsafe.UnsafeComparison.Equal
import com.criteo.deepdiff.utils.Common
import org.apache.spark.sql.catalyst.util.ArrayData

/** Base class for comparing common arrays of structs. */
private[diff] abstract class ArrayStructDiffAccumulator(
    arrayDiffField: ArrayStructDiffField[Common],
    maxDiffExamples: Int
) extends FieldWithNestedRecordsDiffAccumulator {
  import KindOfDiff._
  private[accumulators] final val fullName: String = arrayDiffField.fullName
  private val unsafeArrayComparator: UnsafeComparator[ArrayData] =
    UnsafeComparator.array(arrayDiffField.binaryEquivalence)
  private val leftOrdinal = arrayDiffField.raw.left.ordinal
  private val rightOrdinal = arrayDiffField.raw.right.ordinal
  private val fieldDiffs = LeftRightRawDiffsPartBuilders.common(arrayDiffField, maxDiffExamples)
  protected final val leftNumFields: Int = arrayDiffField.nestedStruct.schema.rawNumFields.left
  protected final val rightNumFields: Int = arrayDiffField.nestedStruct.schema.rawNumFields.right

  /** Must implement the logic to compare the elements individually. */
  @inline protected def compareArrayElements(key: K, left: ArrayData, right: ArrayData): KindOfDiff
  @inline protected def nestedBinaryIdenticalRecord(count: Int, nestedNotNullRecords: Iterator[R]): Unit

  private[accumulators] final def compareRecords(key: K,
                                                 left: R,
                                                 leftAnyNull: Boolean,
                                                 right: R,
                                                 rightAnyNull: Boolean): KindOfDiff = {
    val leftArray = left.getArray(leftOrdinal)
    val rightArray = right.getArray(rightOrdinal)

    if (leftArray == null && rightArray == null) {
      fieldDiffs.identical()
    } else if (leftArray == null) {
      fieldDiffs.rightOnly(key, right)
    } else if (rightArray == null) {
      fieldDiffs.leftOnly(key, left)
    } else {
      unsafeArrayComparator.compare(leftArray, rightArray) match {
        case Equal =>
          binaryIdenticalArray(leftArray)
          fieldDiffs.identical()
        case _ =>
          val flags = compareArrayElements(key, leftArray, rightArray)
          if ((flags | IDENTICAL) == IDENTICAL) fieldDiffs.identical()
          else fieldDiffs.different(key, left, right)
          flags
      }
    }
  }

  private[accumulators] final def binaryIdenticalRecord(left: R): Unit = {
    fieldDiffs.identical()
    val leftArray = left.getArray(leftOrdinal)
    if (leftArray != null) {
      binaryIdenticalArray(leftArray)
    }
  }

  @inline private def binaryIdenticalArray(leftArray: ArrayData): Unit = {
    val numElements = leftArray.numElements()
    nestedBinaryIdenticalRecord(
      numElements,
      nestedNotNullRecords = new Iterator[R] {
        private var i = 0
        private var nextStruct: R = _
        def hasNext: Boolean = {
          while (nextStruct == null && i < numElements) {
            nextStruct = leftArray.getStruct(i, leftNumFields)
            i += 1
          }
          nextStruct != null
        }

        def next(): R = {
          val result = nextStruct
          nextStruct = null
          result
        }
      }
    )
  }

  protected def fieldResult(): (String, LeftRightRawDiffsPart[Kx, Any]) =
    fullName -> fieldDiffs.result()
}
