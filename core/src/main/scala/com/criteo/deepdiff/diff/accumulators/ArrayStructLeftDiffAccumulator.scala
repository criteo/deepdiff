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
import com.criteo.deepdiff.utils.HasLeft

/** Base class for comparing left-only arrays of structs.
  *
  * Since we know the field to be left-only, we only care whether the field is present (left-only) or
  * not (identical). Nested difference accumulators will only be used to generate an empty
  * BinaryFieldDiff.
  */
private[diff] abstract class ArrayStructLeftDiffAccumulator(
    arrayDiffField: ArrayStructDiffField[HasLeft],
    maxDiffExamples: Int
) extends FieldLeftDiffAccumulator {
  private[accumulators] final val fullName: String = arrayDiffField.fullName
  private val leftOrdinal = arrayDiffField.raw.left.ordinal
  private val leftNumFields: Int = arrayDiffField.nestedStruct.schema.rawNumFields.left
  private val fieldDiffs = LeftRightRawDiffsPartBuilders.leftOnly(arrayDiffField, maxDiffExamples)

  @inline protected def nestedLeftRecordOnly(key: K, position: Int, nestedLeft: R): Unit

  @inline protected final def leftRecordOnly(key: K, left: R, leftAnyNull: Boolean): KindOfDiff = {
    val leftArray = left.getArray(leftOrdinal)
    if (leftArray == null) {
      fieldDiffs.nullValue()
    } else {
      var i = 0
      while (i < leftArray.numElements()) {
        val leftStruct = leftArray.getStruct(i, leftNumFields)
        // a null field cannot be considered left-only, so it will only be tracked at the array level.
        if (leftStruct != null) {
          nestedLeftRecordOnly(key, i, leftStruct)
        }
        i += 1
      }
      fieldDiffs.leftOnly(key, left)
    }
  }

  protected def fieldResult: (String, LeftRightRawDiffsPart[Kx, Any]) =
    fullName -> fieldDiffs.result()
}
