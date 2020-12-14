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

import com.criteo.deepdiff.diff.{K, Kx, R}
import com.criteo.deepdiff.raw_part.{KindOfDiff, LeftRightRawDiffsPart}

/** Used for a left-only field. We know it isn't present on the right, we only care whether the
  * field is present (left-only) or not (identical).
  */
private[diff] abstract class FieldLeftDiffAccumulator extends FieldDiffAccumulator {
  private[accumulators] final def compareRecords(key: K,
                                                 left: R,
                                                 leftAnyNull: Boolean,
                                                 right: R,
                                                 rightAnyNull: Boolean): KindOfDiff =
    leftRecordOnly(key, left, leftAnyNull)

  @inline protected def leftRecordOnly(key: K, left: R, leftAnyNull: Boolean): KindOfDiff

  /** There cannot be any binary identical records, as with left-only fields, schema don't match. */
  private[accumulators] final def result(
      binaryIdenticalRecords: Long): List[(String, LeftRightRawDiffsPart[Kx, Any])] = {
    assert(binaryIdenticalRecords == 0)
    result()
  }

  protected def result(): List[(String, LeftRightRawDiffsPart[Kx, Any])]
}
