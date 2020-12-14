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

package com.criteo.deepdiff

import org.apache.spark.sql.types.StructType

/** All differences between left and right datasets.
  *
  * @param root Differences from the root level
  * @param explodedArrays Differences for the exploded arrays
  * @param schema Actual schemas used for comparison.
  */
final case class DatasetDiffs[Key, +Record](root: MatchedRecordsDiffs[Key, Record],
                                            explodedArrays: Map[String, RecordDiffs[Key, Record]],
                                            schema: DatasetsSchema)

final case class DatasetsSchema(left: StructType, right: StructType)

/** All differences between left and right records. */
sealed trait RecordDiffs[Key, +Record]

/** Records were matched through a key, hence multiple records could match the same key. */
final case class MatchedRecordsDiffs[Key, +Record](multipleMatches: Diff[Key, DifferentExample[Seq[Record]]],
                                                   recordDiffs: CommonDiffs[Key, Record],
                                                   kindOfDifferentRecords: KindOfDifferentRecords,
                                                   fieldsDiffs: Map[String, LeftRightDiffs[Key, Any]])
    extends RecordDiffs[Key, Record]

/** Used only for exploded arrays, when the array was left-only. */
final case class LeftOnlyRecordDiffs[Key, +Record](records: Diff[Key, LeftOnlyExample[Record]], fields: Set[String])
    extends RecordDiffs[Key, Record]

/** Used only for exploded arrays, when the array was right-only. */
final case class RightOnlyRecordDiffs[Key, +Record](records: Diff[Key, RightOnlyExample[Record]], fields: Set[String])
    extends RecordDiffs[Key, Record]

/** Reasons why a record was different: a field with a different value or being left-only. */
final case class KindOfDifferentRecords(hasDifferentNotNullField: Long,
                                        hasOnlyLeftOnlyFields: Long,
                                        hasOnlyRightOnlyFields: Long,
                                        hasOnlyLeftAndRightOnlyFields: Long)

/** All kind of examples when comparing left to right, usually for a field. */
sealed trait LeftRightDiffs[Key, +Field]
final case class CommonDiffs[Key, +Field](identical: Long,
                                          different: Diff[Key, DifferentExample[Field]],
                                          leftOnly: Diff[Key, LeftOnlyExample[Field]],
                                          rightOnly: Diff[Key, RightOnlyExample[Field]])
    extends LeftRightDiffs[Key, Field]
final case class LeftOnlyDiffs[Key, +Field](nulls: Long, leftOnly: Diff[Key, LeftOnlyExample[Field]])
    extends LeftRightDiffs[Key, Field]
final case class RightOnlyDiffs[Key, +Field](nulls: Long, rightOnly: Diff[Key, RightOnlyExample[Field]])
    extends LeftRightDiffs[Key, Field]

/** Difference count and some examples. */
final case class Diff[Key, +Example <: DiffExample[_]](count: Long, examples: Map[Key, Example])

sealed trait DiffExample[+Field]
final case class DifferentExample[+Field](left: Field, right: Field) extends DiffExample[Field]
final case class LeftOnlyExample[+Field](left: Field) extends DiffExample[Field]
final case class RightOnlyExample[+Field](right: Field) extends DiffExample[Field]
