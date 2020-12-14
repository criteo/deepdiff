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

import com.criteo.deepdiff.{DifferentExample, LeftOnlyExample, RightOnlyExample}

/** Mutable structure used to build a [[CommonRawDiffsPart]].
  *
  * Methods accept the record, and not the field itself as this allows to easily factorize
  * code. Whatever the type of the field, the type of the record doesn't change.
  *
  * @param maxDiffExamples Maximum number of examples to keep.
  * @param rawDiffExampleBuilder Used to generate an example from both records. Called only the
  *                              maximum number of examples is not reached yet.
  * @tparam K key
  * @tparam R record
  * @tparam Kx key example
  * @tparam Fx field example
  */
private[deepdiff] final class CommonRawDiffsPartBuilder[K, R, Kx, Fx](
    maxDiffExamples: Int,
    rawDiffExampleBuilder: RawDiffExampleBuilder[K, R, Kx, Fx]
) extends Serializable {
  import KindOfDiff._

  private var differentExamples: List[(Kx, DifferentExample[Fx])] = Nil
  private var rightOnlyExamples: List[(Kx, RightOnlyExample[Fx])] = Nil
  private var leftOnlyExamples: List[(Kx, LeftOnlyExample[Fx])] = Nil
  private var identicalCount: Long = 0
  private var differentCount: Long = 0
  private var leftOnlyCount: Long = 0
  private var rightOnlyCount: Long = 0

  @inline def identical(): KindOfDiff = {
    identicalCount += 1
    IDENTICAL
  }

  @inline def different(key: K, left: R, right: R): KindOfDiff = {
    if (differentCount < maxDiffExamples) differentExamples ::= rawDiffExampleBuilder.buildDifferent(key, left, right)
    differentCount += 1
    DIFFERENT
  }

  @inline def leftOnly(key: K, left: R): KindOfDiff = {
    if (leftOnlyCount < maxDiffExamples) leftOnlyExamples ::= rawDiffExampleBuilder.buildLeftOnly(key, left)
    leftOnlyCount += 1
    LEFT_ONLY
  }

  @inline def rightOnly(key: K, right: R): KindOfDiff = {
    if (rightOnlyCount < maxDiffExamples) rightOnlyExamples ::= rawDiffExampleBuilder.buildRightOnly(key, right)
    rightOnlyCount += 1
    RIGHT_ONLY
  }

  /** @param binaryIdenticalRecords When the records binary is identical, fields are not compare individually.
    *                              As there's no need to call identical() on each of them, a global counter is
    *                              usually incremented and passed on here.
    */
  def result(binaryIdenticalRecords: Long = 0): CommonRawDiffsPart[Kx, Fx] =
    CommonRawDiffsPart[Kx, Fx](
      identical = identicalCount + binaryIdenticalRecords,
      different = RawDiffPart(differentCount, differentExamples),
      leftOnly = RawDiffPart(leftOnlyCount, leftOnlyExamples),
      rightOnly = RawDiffPart(rightOnlyCount, rightOnlyExamples)
    )
}
