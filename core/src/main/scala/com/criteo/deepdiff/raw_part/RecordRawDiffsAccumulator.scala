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

/** Interface required by [[MatchedRecordDiffsPartBuilder]] to compare records.
  *
  * @tparam K key
  * @tparam R record
  * @tparam Kx raw key example
  * @tparam Rx raw record example
  */
private[deepdiff] abstract class RecordRawDiffsAccumulator[K, R, Kx, Rx] extends Serializable {
  def recordExampleBuilder: RawDiffExampleBuilder[K, R, Kx, Rx]
  def multipleMatchesExampleBuilder: RawDiffExampleBuilder[K, Iterator[R], Kx, Seq[Rx]]

  /** Used to determine if records are equal when ignoring multiple identical matches. */
  @inline def isLeftEqual(a: R, b: R): Boolean

  /** Used to determine if records are equal when ignoring multiple identical matches. */
  @inline def isRightEqual(a: R, b: R): Boolean

  @inline def compareRecords(key: K, left: R, right: R): KindOfDiff

  /** Used only for exploded arrays, when records are binary identical. */
  @inline def binaryIdenticalRecord(record: R): Unit

  def results: List[(String, LeftRightRawDiffsPart[Kx, Any])]
}
