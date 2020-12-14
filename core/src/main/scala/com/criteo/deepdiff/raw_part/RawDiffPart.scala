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

/**
  * Working representation of the [[com.criteo.deepdiff.DatasetDiffs]] and its components.
  *
  * It serves several purposes:
  * - Separation of our public API and our internal one.
  * - Examples stored in those classes are *always raw*, while our public API only has human friendly ones.
  * - Merging Lists of examples is more efficient than merging Maps
  */
package com.criteo.deepdiff.raw_part

import com.criteo.deepdiff._
import com.criteo.deepdiff.utils.LeftRight

private[deepdiff] final case class DatasetRawDiffsPart[Kx, +Rx](root: MatchedRecordsRawDiffsPart[Kx, Rx],
                                                                explodedArrays: Map[String, RecordRawDiffsPart[Kx, Rx]])

private[deepdiff] object DatasetRawDiffsPart {
  def empty[Kx, Rx]: DatasetRawDiffsPart[Kx, Rx] =
    DatasetRawDiffsPart(
      root = MatchedRecordsRawDiffsPart.empty,
      explodedArrays = Map.empty
    )
}

private[deepdiff] sealed trait RecordRawDiffsPart[Kx, +Rx]

private[deepdiff] final case class MatchedRecordsRawDiffsPart[Kx, +Rx](
    multipleMatches: RawDiffPart[Kx, DifferentExample[Seq[Rx]]],
    recordDiffs: CommonRawDiffsPart[Kx, Rx],
    kindOfDifferentRecords: KindOfDifferentRecords,
    fieldsDiffs: Map[String, LeftRightRawDiffsPart[Kx, Any]])
    extends RecordRawDiffsPart[Kx, Rx]

private[deepdiff] object MatchedRecordsRawDiffsPart {
  def empty[Kx, Rx]: MatchedRecordsRawDiffsPart[Kx, Rx] =
    MatchedRecordsRawDiffsPart(
      multipleMatches = RawDiffPart.empty,
      recordDiffs = CommonRawDiffsPart(0, RawDiffPart.empty, RawDiffPart.empty, RawDiffPart.empty),
      kindOfDifferentRecords = KindOfDifferentRecords(0, 0, 0, 0),
      fieldsDiffs = Map.empty
    )
}

private[deepdiff] final case class LeftOnlyRecordRawDiffsPart[Kx, +Rx](records: RawDiffPart[Kx, LeftOnlyExample[Rx]],
                                                                       fields: Set[String])
    extends RecordRawDiffsPart[Kx, Rx]
private[deepdiff] final case class RightOnlyRecordRawDiffsPart[Kx, +Rx](records: RawDiffPart[Kx, RightOnlyExample[Rx]],
                                                                        fields: Set[String])
    extends RecordRawDiffsPart[Kx, Rx]

private[deepdiff] object RecordRawDiffsPart {
  implicit class LeftOnlyRecordDiffsBuilder[Kx, Rx](val raw: LeftOnlyRecordRawDiffsPart[Kx, Rx]) {
    def buildLeftOnlyRecordDiffs[Key, Record](
        recordRawToFriendly: RawToFriendlyDiffExample[Kx, Rx, Key, Record]): LeftOnlyRecordDiffs[Key, Record] = {
      LeftOnlyRecordDiffs(
        records = recordRawToFriendly.leftOnly(raw.records),
        fields = raw.fields
      )
    }
  }

  implicit class RightOnlyRecordDiffsBuilder[Kx, Rx](val raw: RightOnlyRecordRawDiffsPart[Kx, Rx]) {
    def buildRightOnlyRecordDiffs[Key, Record](
        recordRawToFriendly: RawToFriendlyDiffExample[Kx, Rx, Key, Record]): RightOnlyRecordDiffs[Key, Record] = {
      RightOnlyRecordDiffs(
        records = recordRawToFriendly.rightOnly(raw.records),
        fields = raw.fields
      )
    }
  }

  implicit class MatchedRecordDiffsBuilder[Kx, Rx](val raw: MatchedRecordsRawDiffsPart[Kx, Rx]) {
    def buildMatchedRecordsDiffs[Key, Record](
        multipleMatchesRawToFriendly: RawToFriendlyDiffExample[Kx, Seq[Rx], Key, Seq[Record]],
        recordRawToFriendly: RawToFriendlyDiffExample[Kx, Rx, Key, Record],
        rawToFriendlyByFullName: Map[String, RawToFriendlyDiffExample[Kx, Any, Key, Any]]
    ): MatchedRecordsDiffs[Key, Record] = {
      MatchedRecordsDiffs(
        multipleMatches = multipleMatchesRawToFriendly.different(raw.multipleMatches),
        recordDiffs = raw.recordDiffs.buildCommonDiff(recordRawToFriendly),
        kindOfDifferentRecords = raw.kindOfDifferentRecords,
        fieldsDiffs = raw.fieldsDiffs.map({
          case (field, diff) =>
            val rawToFriendly = rawToFriendlyByFullName(field)
            field -> (diff match {
              case value: CommonRawDiffsPart[Kx, Any]    => value.buildCommonDiff(rawToFriendly)
              case value: LeftOnlyRawDiffsPart[Kx, Any]  => value.buildLeftOnlyDiff(rawToFriendly)
              case value: RightOnlyRawDiffsPart[Kx, Any] => value.buildRightOnlyDiff(rawToFriendly)
            })
        })
      )
    }
  }
}

private[deepdiff] sealed trait LeftRightRawDiffsPart[Kx, +Fx]

private[deepdiff] final case class CommonRawDiffsPart[Kx, +Fx](identical: Long,
                                                               different: RawDiffPart[Kx, DifferentExample[Fx]],
                                                               leftOnly: RawDiffPart[Kx, LeftOnlyExample[Fx]],
                                                               rightOnly: RawDiffPart[Kx, RightOnlyExample[Fx]])
    extends LeftRightRawDiffsPart[Kx, Fx]

object CommonRawDiffsPart {
  def empty[Kx, Fx]: CommonRawDiffsPart[Kx, Fx] =
    CommonRawDiffsPart(0, RawDiffPart.empty, RawDiffPart.empty, RawDiffPart.empty)
}

private[deepdiff] final case class LeftOnlyRawDiffsPart[Kx, +Fx](nulls: Long,
                                                                 leftOnly: RawDiffPart[Kx, LeftOnlyExample[Fx]])
    extends LeftRightRawDiffsPart[Kx, Fx]

private[deepdiff] final case class RightOnlyRawDiffsPart[Kx, +Fx](nulls: Long,
                                                                  rightOnly: RawDiffPart[Kx, RightOnlyExample[Fx]])
    extends LeftRightRawDiffsPart[Kx, Fx]

private[deepdiff] object LeftRightRawDiffsPart {
  implicit class CommonDiffBuilder[Kx, Fx](val raw: CommonRawDiffsPart[Kx, Fx]) {
    def buildCommonDiff[Key, Field](
        rawToFriendly: RawToFriendlyDiffExample[Kx, Fx, Key, Field]): CommonDiffs[Key, Field] = {
      CommonDiffs(
        identical = raw.identical,
        different = rawToFriendly.different(raw.different),
        leftOnly = rawToFriendly.leftOnly(raw.leftOnly),
        rightOnly = rawToFriendly.rightOnly(raw.rightOnly)
      )
    }
  }

  implicit class LeftOnlyDiffBuilder[Kx, Fx](val raw: LeftOnlyRawDiffsPart[Kx, Fx]) {
    def buildLeftOnlyDiff[Key, Field](
        rawToFriendly: RawToFriendlyDiffExample[Kx, Fx, Key, Field]): LeftOnlyDiffs[Key, Field] = {
      LeftOnlyDiffs(
        nulls = raw.nulls,
        leftOnly = rawToFriendly.leftOnly(raw.leftOnly)
      )
    }
  }

  implicit class RightOnlyDiffBuilder[Kx, Fx](val raw: RightOnlyRawDiffsPart[Kx, Fx]) {
    def buildRightOnlyDiff[Key, Field](
        rawToFriendly: RawToFriendlyDiffExample[Kx, Fx, Key, Field]): RightOnlyDiffs[Key, Field] = {
      RightOnlyDiffs(
        nulls = raw.nulls,
        rightOnly = rawToFriendly.rightOnly(raw.rightOnly)
      )
    }
  }
}

private[deepdiff] final case class RawDiffPart[Key, +Example <: DiffExample[_]](count: Long,
                                                                                examples: List[(Key, Example)])

private[deepdiff] object RawDiffPart {
  def empty[Key, Example <: DiffExample[_]]: RawDiffPart[Key, Example] = RawDiffPart(count = 0, examples = Nil)
}

private[deepdiff] final case class RawDiffExample[+Kx, +Fx](key: Kx, example: LeftRight[Fx])
