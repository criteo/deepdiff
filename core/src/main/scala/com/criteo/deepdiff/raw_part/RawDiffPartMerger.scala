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

package com.criteo.deepdiff.raw_part

import com.criteo.deepdiff.{DiffExample, KindOfDifferentRecords}

import scala.language.higherKinds

private[deepdiff] final class RawDiffPartMerger(maxExamples: Int) extends Serializable {

  /** Merges two [[DatasetRawDiffsPart]] into one while respecting the maximum number of examples.
    *
    * @tparam Kx raw key example
    * @tparam Rx raw record example
    */
  def merge[Kx, Rx](a: DatasetRawDiffsPart[Kx, Rx], b: DatasetRawDiffsPart[Kx, Rx]): DatasetRawDiffsPart[Kx, Rx] = {
    DatasetRawDiffsPart(
      root = mergeRecordRawDiffsPart[Kx, Rx, MatchedRecordsRawDiffsPart](a.root, b.root),
      explodedArrays = {
        if (a.explodedArrays.isEmpty || b.explodedArrays.isEmpty) {
          if (a.explodedArrays.isEmpty) b.explodedArrays else a.explodedArrays
        } else {
          assert(a.explodedArrays.keySet == b.explodedArrays.keySet)
          a.explodedArrays
            .map({
              case (key, explodedArray) =>
                key -> mergeRecordRawDiffsPart[Kx, Rx, RecordRawDiffsPart](explodedArray, b.explodedArrays(key))
            })
        }
      }
    )
  }

  private def mergeRecordRawDiffsPart[Kx, Rx, RawPart[U, +V] <: RecordRawDiffsPart[U, V]](
      a: RawPart[Kx, Rx],
      b: RawPart[Kx, Rx]): RawPart[Kx, Rx] = {
    ((a, b) match {
      case (a: MatchedRecordsRawDiffsPart[Kx, Rx], b: MatchedRecordsRawDiffsPart[Kx, Rx]) =>
        MatchedRecordsRawDiffsPart[Kx, Rx](
          multipleMatches = merge(a.multipleMatches, b.multipleMatches),
          recordDiffs = mergeLeftRightRawDiffsPart[Kx, Rx, CommonRawDiffsPart](a.recordDiffs, b.recordDiffs),
          kindOfDifferentRecords = merge(a.kindOfDifferentRecords, b.kindOfDifferentRecords),
          fieldsDiffs = {
            if (a.fieldsDiffs.isEmpty || b.fieldsDiffs.isEmpty) {
              if (a.fieldsDiffs.isEmpty) b.fieldsDiffs else a.fieldsDiffs
            } else {
              assert(a.fieldsDiffs.keySet == b.fieldsDiffs.keySet)
              a.fieldsDiffs
                .map({
                  case (key, fieldDiff) =>
                    key -> mergeLeftRightRawDiffsPart[Kx, Any, LeftRightRawDiffsPart](fieldDiff, b.fieldsDiffs(key))
                })
            }
          }
        )
      case (a: LeftOnlyRecordRawDiffsPart[Kx, Rx], b: LeftOnlyRecordRawDiffsPart[Kx, Rx]) =>
        LeftOnlyRecordRawDiffsPart[Kx, Rx](
          records = merge(a.records, b.records),
          fields = {
            assert(a.fields == b.fields)
            a.fields
          }
        )
      case (a: RightOnlyRecordRawDiffsPart[Kx, Rx], b: RightOnlyRecordRawDiffsPart[Kx, Rx]) =>
        RightOnlyRecordRawDiffsPart[Kx, Rx](
          records = merge(a.records, b.records),
          fields = {
            assert(a.fields == b.fields)
            a.fields
          }
        )
    }).asInstanceOf[RawPart[Kx, Rx]]
  }

  private def mergeLeftRightRawDiffsPart[Kx, Fx, RawPart[U, +V] <: LeftRightRawDiffsPart[U, V]](
      a: RawPart[Kx, Fx],
      b: RawPart[Kx, Fx]): RawPart[Kx, Fx] = {
    ((a, b) match {
      case (a: CommonRawDiffsPart[Kx, Fx], b: CommonRawDiffsPart[Kx, Fx]) =>
        CommonRawDiffsPart(
          identical = a.identical + b.identical,
          different = merge(a.different, b.different),
          leftOnly = merge(a.leftOnly, b.leftOnly),
          rightOnly = merge(a.rightOnly, b.rightOnly)
        )
      case (a: LeftOnlyRawDiffsPart[Kx, Fx], b: LeftOnlyRawDiffsPart[Kx, Fx]) =>
        LeftOnlyRawDiffsPart(
          nulls = a.nulls + b.nulls,
          leftOnly = merge(a.leftOnly, b.leftOnly)
        )
      case (a: RightOnlyRawDiffsPart[Kx, Fx], b: RightOnlyRawDiffsPart[Kx, Fx]) =>
        RightOnlyRawDiffsPart(
          nulls = a.nulls + b.nulls,
          rightOnly = merge(a.rightOnly, b.rightOnly)
        )
    }).asInstanceOf[RawPart[Kx, Fx]]
  }

  private def merge(a: KindOfDifferentRecords, b: KindOfDifferentRecords): KindOfDifferentRecords =
    KindOfDifferentRecords(
      hasDifferentNotNullField = a.hasDifferentNotNullField + b.hasDifferentNotNullField,
      hasOnlyLeftOnlyFields = a.hasOnlyLeftOnlyFields + b.hasOnlyLeftOnlyFields,
      hasOnlyRightOnlyFields = a.hasOnlyRightOnlyFields + b.hasOnlyRightOnlyFields,
      hasOnlyLeftAndRightOnlyFields = a.hasOnlyLeftAndRightOnlyFields + b.hasOnlyLeftAndRightOnlyFields
    )

  private def merge[Kx, Example <: DiffExample[_]](a: RawDiffPart[Kx, Example],
                                                   b: RawDiffPart[Kx, Example]): RawDiffPart[Kx, Example] = {
    RawDiffPart(
      count = a.count + b.count,
      examples = {
        val (big, small) = if (a.count >= b.count) (a, b) else (b, a)
        if (big.count < maxExamples && small.examples.nonEmpty) {
          var examples = big.examples
          var remaining = small.examples
          var missingCount = maxExamples - big.count
          while (missingCount > 0 && remaining.nonEmpty) {
            remaining match {
              case head :: tail =>
                examples ::= head
                remaining = tail
            }
            missingCount -= 1
          }
          examples
        } else {
          big.examples
        }
      }
    )
  }
}
