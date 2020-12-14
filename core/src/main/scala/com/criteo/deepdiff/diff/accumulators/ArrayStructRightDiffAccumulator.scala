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
import com.criteo.deepdiff.utils.HasRight

/** Base class for comparing right-only arrays of structs.
  *
  * Since we know the field to be right-only, we only care whether the field is present (right-only) or
  * not (identical). Nested difference accumulators will only be used to generate an empty
  * BinaryFieldDiff.
  */
private[diff] abstract class ArrayStructRightDiffAccumulator(
    arrayDiffField: ArrayStructDiffField[HasRight],
    maxDiffExamples: Int
) extends FieldRightDiffAccumulator {
  private[accumulators] final val fullName: String = arrayDiffField.fullName
  private val rightOrdinal = arrayDiffField.raw.right.ordinal
  private val rightNumFields: Int = arrayDiffField.nestedStruct.schema.rawNumFields.right
  private val fieldDiffs = LeftRightRawDiffsPartBuilders.rightOnly(arrayDiffField, maxDiffExamples)

  @inline protected def nestedRightRecordOnly(key: K, position: Int, nestedLeft: R): Unit

  @inline protected final def rightRecordOnly(key: K, right: R, rightAnyNull: Boolean): KindOfDiff = {
    val rightArray = right.getArray(rightOrdinal)
    if (rightArray == null) {
      fieldDiffs.nullValue()
    } else {
      var i = 0
      while (i < rightArray.numElements()) {
        val rightStruct = rightArray.getStruct(i, rightNumFields)
        // a null field cannot be considered right-only, so it will only be tracked at the array level.
        if (rightStruct != null) {
          nestedRightRecordOnly(key, i, rightStruct)
        }
        i += 1
      }
      fieldDiffs.rightOnly(key, right)
    }
  }

  protected def fieldResult(): (String, LeftRightRawDiffsPart[Kx, Any]) = {
    fullName -> fieldDiffs.result()
  }
}
