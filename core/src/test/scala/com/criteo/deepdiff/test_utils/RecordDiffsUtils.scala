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

import com.criteo.deepdiff._

private[test_utils] object RecordDiffsUtils {
  def reverseLeftAndRight[K, R](r: RecordDiffs[K, R]): RecordDiffs[K, R] = {
    r match {
      case MatchedRecordsDiffs(multipleMatches, recordDiff, kindOfDifferentRecords, fieldDiffs) =>
        MatchedRecordsDiffs(
          fieldsDiffs = fieldDiffs.map({
            case (field, binaryFieldDiff) => field -> reverseLeftRightDiff(binaryFieldDiff)
          }),
          recordDiffs = reverseLeftRightDiff(recordDiff).asInstanceOf[CommonDiffs[K, R]],
          multipleMatches = reverseDifferent(multipleMatches),
          kindOfDifferentRecords = kindOfDifferentRecords.copy(
            hasOnlyLeftOnlyFields = kindOfDifferentRecords.hasOnlyRightOnlyFields,
            hasOnlyRightOnlyFields = kindOfDifferentRecords.hasOnlyLeftOnlyFields
          )
        )
      case LeftOnlyRecordDiffs(records, fields) =>
        RightOnlyRecordDiffs(reverseLeftOnly(records), fields)
      case RightOnlyRecordDiffs(records, fields) =>
        LeftOnlyRecordDiffs(reverseRightOnly(records), fields)
    }
  }

  private def reverseLeftRightDiff[K, F](lr: LeftRightDiffs[K, F]): LeftRightDiffs[K, F] = {
    lr match {
      case CommonDiffs(identical, different, leftOnly, rightOnly) =>
        CommonDiffs(
          identical = identical,
          different = reverseDifferent(different),
          leftOnly = reverseRightOnly(rightOnly),
          rightOnly = reverseLeftOnly(leftOnly)
        )
      case LeftOnlyDiffs(identical, leftOnly) =>
        RightOnlyDiffs(
          nulls = identical,
          rightOnly = reverseLeftOnly(leftOnly)
        )
      case RightOnlyDiffs(identical, rightOnly) =>
        LeftOnlyDiffs(
          nulls = identical,
          leftOnly = reverseRightOnly(rightOnly)
        )
    }
  }

  private def reverseDifferent[K, F](d: Diff[K, DifferentExample[F]]): Diff[K, DifferentExample[F]] =
    Diff(d.count,
         d.examples.mapValues({
           case DifferentExample(left, right) => DifferentExample(right, left)
         }))

  private def reverseLeftOnly[K, F](d: Diff[K, LeftOnlyExample[F]]): Diff[K, RightOnlyExample[F]] =
    Diff(d.count,
         d.examples.mapValues({
           case LeftOnlyExample(left) => RightOnlyExample(left)
         }))

  private def reverseRightOnly[K, F](d: Diff[K, RightOnlyExample[F]]): Diff[K, LeftOnlyExample[F]] =
    Diff(d.count,
         d.examples.mapValues({
           case RightOnlyExample(right) => LeftOnlyExample(right)
         }))

  def transform[K, R, Rx](r: RecordDiffs[K, R],
                          transformRecord: R => Rx,
                          transformField: Any => Any): RecordDiffs[K, Rx] = {
    r match {
      case MatchedRecordsDiffs(multipleMatches, recordDiff, kindOfDifferentRecords, fieldDiffs) =>
        MatchedRecordsDiffs[K, Rx](
          multipleMatches = Diff(
            count = multipleMatches.count,
            examples = multipleMatches.examples.mapValues({
              case DifferentExample(left, right) =>
                DifferentExample(left.map(transformRecord), right.map(transformRecord))
            })
          ),
          recordDiffs = transformLeftRightDiff(recordDiff, transformRecord).asInstanceOf[CommonDiffs[K, Rx]],
          kindOfDifferentRecords = kindOfDifferentRecords,
          fieldsDiffs = fieldDiffs.mapValues(transformLeftRightDiff(_, transformField))
        )
      case LeftOnlyRecordDiffs(records, fields) =>
        LeftOnlyRecordDiffs(
          records = Diff(
            count = records.count,
            examples = records.examples.mapValues({
              case LeftOnlyExample(left) => LeftOnlyExample(transformRecord(left))
            })
          ),
          fields
        )
      case RightOnlyRecordDiffs(records, fields) =>
        RightOnlyRecordDiffs(
          records = Diff(
            count = records.count,
            examples = records.examples.mapValues({
              case RightOnlyExample(right) => RightOnlyExample(transformRecord(right))
            })
          ),
          fields
        )
    }
  }

  private def transformLeftRightDiff[K, F, Fx](lr: LeftRightDiffs[K, F],
                                               transformField: F => Fx): LeftRightDiffs[K, Fx] = {
    lr match {
      case CommonDiffs(identical, different, leftOnly, rightOnly) =>
        CommonDiffs(
          identical = identical,
          different = Diff(
            count = different.count,
            examples = different.examples.mapValues({
              case DifferentExample(left, right) => DifferentExample(transformField(left), transformField(right))
            })
          ),
          leftOnly = Diff(
            count = leftOnly.count,
            examples = leftOnly.examples.mapValues({
              case LeftOnlyExample(left) => LeftOnlyExample(transformField(left))
            })
          ),
          rightOnly = Diff(
            count = rightOnly.count,
            examples = rightOnly.examples.mapValues({
              case RightOnlyExample(right) => RightOnlyExample(transformField(right))
            })
          )
        )
      case LeftOnlyDiffs(identical, leftOnly) =>
        LeftOnlyDiffs(
          nulls = identical,
          leftOnly = Diff(
            count = leftOnly.count,
            examples = leftOnly.examples.mapValues({
              case LeftOnlyExample(left) => LeftOnlyExample(transformField(left))
            })
          )
        )
      case RightOnlyDiffs(identical, rightOnly) =>
        RightOnlyDiffs(
          nulls = identical,
          rightOnly = Diff(
            count = rightOnly.count,
            examples = rightOnly.examples.mapValues({
              case RightOnlyExample(right) => RightOnlyExample(transformField(right))
            })
          )
        )
    }
  }
}
