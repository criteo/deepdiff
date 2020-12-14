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

import com.criteo.deepdiff.diff.{K, Kx, R, Rx}
import com.criteo.deepdiff.plan.AnyStructDiffField
import com.criteo.deepdiff.raw_part.{CommonRawDiffsPartBuilder, KindOfDiff, LeftRightRawDiffsPart}
import com.criteo.deepdiff.type_support.example.StructExampleFactory
import com.criteo.deepdiff.utils.Common
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters

/** Utility class to handle common nested records, whether in a nested struct or in an array of structs.
  *
  * @param nestedStruct NestedStructDiffField of the nested record.
  * @param nestedRecordFieldsDiff Used to actually accumulate the field differences, as the root record and nested ones
  *                               behave in the same way.
  * @param maxDiffExamples Maximum number of differences.
  */
private[diff] final class NestedRecordDiffAccumulator(
    nestedStruct: AnyStructDiffField[Common],
    nestedRecordFieldsDiff: RecordFieldsDiffAccumulator,
    maxDiffExamples: Int
) extends Serializable {
  import com.criteo.deepdiff.raw_part.KindOfDiff._

  private val fullName: String = nestedStruct.fullName
  private val leftNumFields = nestedStruct.schema.rawNumFields.left
  private val rightNumFields = nestedStruct.schema.rawNumFields.right
  private var nestedBinaryIdenticalRecords: Int = 0
  private val nestedRecordDiff = new CommonRawDiffsPartBuilder[K, R, Kx, Rx](
    maxDiffExamples = maxDiffExamples,
    rawDiffExampleBuilder = StructExampleFactory.record.rawExampleBuilder
  )

  @inline def compareInputs(key: K,
                            left: SpecializedGetters,
                            leftOrdinal: Int,
                            right: SpecializedGetters,
                            rightOrdinal: Int): KindOfDiff = {
    val nestedLeft = left.getStruct(leftOrdinal, leftNumFields)
    val nestedRight = right.getStruct(rightOrdinal, rightNumFields)
    if (nestedLeft == null && nestedRight == null) {
      nestedRecordDiff.identical()
    } else if (nestedLeft == null) {
      nestedRecordDiff.rightOnly(key, nestedRight)
    } else if (nestedRight == null) {
      nestedRecordDiff.leftOnly(key, nestedLeft)
    } else {
      val flags = nestedRecordFieldsDiff.compareRecords(key, nestedLeft, nestedRight)
      if ((flags | IDENTICAL) == IDENTICAL) nestedRecordDiff.identical()
      else nestedRecordDiff.different(key, nestedLeft, nestedRight)
      flags
    }
  }

  @inline def nestedBinaryIdenticalRecord(nestedRecord: R): Unit = {
    nestedBinaryIdenticalRecords += 1
    if (nestedRecord != null) {
      nestedRecordFieldsDiff.binaryIdenticalRecord(nestedRecord)
    }
  }

  @inline def nestedBinaryIdenticalRecords(count: Int, nestedNotNullRecords: Iterator[R]): Unit = {
    nestedBinaryIdenticalRecords += count
    nestedRecordFieldsDiff.binaryIdenticalRecords(nestedNotNullRecords)
  }

  @inline def nestedLeftOnly(key: K, nestedLeft: R): KindOfDiff = nestedRecordDiff.leftOnly(key, nestedLeft)

  @inline def nestedRightOnly(key: K, nestedRight: R): KindOfDiff = nestedRecordDiff.rightOnly(key, nestedRight)

  def result(): List[(String, LeftRightRawDiffsPart[Kx, Any])] = {
    (fullName -> nestedRecordDiff.result(nestedBinaryIdenticalRecords)) :: nestedRecordFieldsDiff.result()
  }
}
