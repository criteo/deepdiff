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

import com.criteo.deepdiff._

private[deepdiff] sealed trait RecordRawDiffsPartBuilder[Kx, Rx] extends Serializable {
  def result: RecordRawDiffsPart[Kx, Rx]
}

/** Only used for left-only exploded arrays.
  *
  * @param rawRecordExampleBuilder Used to build examples.
  * @param diffFieldsFullName      All fields fullName, including nested ones, which won't be compared but
  *                                still need to be included in the final [[DatasetDiffs]].
  * @param maxDiffExamples         Maximum number of examples to keep.
  * @tparam K key
  * @tparam R record
  * @tparam Kx raw key example
  * @tparam Rx raw record example
  */
private[deepdiff] final class LeftOnlyRecordDiffsPartBuilder[K, R, Kx, Rx](
    rawRecordExampleBuilder: RawDiffExampleBuilder[K, R, Kx, Rx],
    diffFieldsFullName: Set[String],
    maxDiffExamples: Int
) extends RecordRawDiffsPartBuilder[Kx, Rx] {
  import KindOfDiff._
  private var leftOnlyCount: Long = 0
  private var leftOnlyExamples: List[(Kx, LeftOnlyExample[Rx])] = Nil

  def leftOnly(key: => K, left: R): KindOfDiff = {
    if (leftOnlyCount < maxDiffExamples) leftOnlyExamples ::= rawRecordExampleBuilder.buildLeftOnly(key, left)
    leftOnlyCount += 1
    LEFT_ONLY
  }

  def result: RecordRawDiffsPart[Kx, Rx] = {
    LeftOnlyRecordRawDiffsPart[Kx, Rx](
      records = RawDiffPart(leftOnlyCount, leftOnlyExamples),
      fields = diffFieldsFullName
    )
  }
}

/** Only used for right-only exploded arrays.
  *
  * @param rawRecordExampleBuilder Used to build examples.
  * @param diffFieldsFullName      All fields fullName, including nested ones, which won't be compared but
  *                                still need to be included in the final [[DatasetDiffs]].
  * @param maxDiffExamples         Maximum number of examples to keep.
  * @tparam K key
  * @tparam R record
  * @tparam Kx raw key example
  * @tparam Rx raw record example
  */
private[deepdiff] final class RightOnlyRecordDiffsPartBuilder[K, R, Kx, Rx](
    rawRecordExampleBuilder: RawDiffExampleBuilder[K, R, Kx, Rx],
    diffFieldsFullName: Set[String],
    maxDiffExamples: Int
) extends RecordRawDiffsPartBuilder[Kx, Rx] {
  import KindOfDiff._
  private var rightOnlyCount: Long = 0
  private var rightOnlyExamples: List[(Kx, RightOnlyExample[Rx])] = Nil

  def rightOnly(key: => K, left: R): KindOfDiff = {
    if (rightOnlyCount < maxDiffExamples) rightOnlyExamples ::= rawRecordExampleBuilder.buildRightOnly(key, left)
    rightOnlyCount += 1
    RIGHT_ONLY
  }

  def result: RecordRawDiffsPart[Kx, Rx] = {
    RightOnlyRecordRawDiffsPart[Kx, Rx](
      records = RawDiffPart(rightOnlyCount, rightOnlyExamples),
      fields = diffFieldsFullName
    )
  }
}

/** Mutable structure used to build a [[DatasetDiffs]].
  *
  * When using the Iterator of records, we're extremely careful to process the current element before
  * requesting the next one. In Spark, once the hasNext() has been called, the current row isn't valid
  * anymore, as it's used like a pointer.
  *
  * @param recordRawDiffsAccumulator Used to compare the records and accumulate field differences.
  * @param omitMultipleMatchesIfAllIdentical Whether identical multiple matches should be
  *                                         treated like any other pair of records or not.
  * @param maxDiffExamples Maximum number of examples to keep.
  * @tparam K key
  * @tparam R record
  * @tparam Kx raw key example
  * @tparam Rx raw record example
  */
private[deepdiff] final class MatchedRecordDiffsPartBuilder[K, R, Kx, Rx](
    recordRawDiffsAccumulator: RecordRawDiffsAccumulator[K, R, Kx, Rx],
    omitMultipleMatchesIfAllIdentical: Boolean,
    maxDiffExamples: Int
) extends RecordRawDiffsPartBuilder[Kx, Rx] {
  import KindOfDiff._

  private val record = new CommonRawDiffsPartBuilder[K, R, Kx, Rx](
    maxDiffExamples = maxDiffExamples,
    rawDiffExampleBuilder = recordRawDiffsAccumulator.recordExampleBuilder
  )
  private var binaryIdenticalRecords: Long = 0
  private var multipleMatchesExamples: List[(Kx, DifferentExample[Seq[Rx]])] = Nil
  private var multipleMatchesCount: Long = 0
  // for KindOfDifferentRecords
  private var hasDifferentFieldCount: Long = 0
  private var hasOnlyLeftOnlyFieldsCount: Long = 0
  private var hasOnlyRightOnlyFieldsCount: Long = 0
  private var hasOnlyLeftAndRightOnlyFieldsCount: Long = 0

  /** Only used for exploded arrays which are binary identical. */
  def binaryIdenticalRecords(records: Iterator[R]): Unit = {
    while (records.hasNext) {
      binaryIdenticalRecords += 1
      recordRawDiffsAccumulator.binaryIdenticalRecord(records.next())
    }
  }

  def compare(key: K, leftHead: R, leftTail: Iterator[R], rightHead: R, rightTail: Iterator[R]): KindOfDiff = {
    if (!leftTail.hasNext && !rightTail.hasNext) {
      compareRecord(key, leftHead, rightHead)
    } else {
      if (omitMultipleMatchesIfAllIdentical) {
        val (leftHeadCount, lastLeftCompared, leftAllIdentical) = compareLeftUntilDifferentFromHead(leftHead, leftTail)
        val (rightHeadCount, lastRightCompared, rightAllIdentical) =
          compareRightUntilDifferentFromHead(rightHead, rightTail)
        if (leftAllIdentical && rightAllIdentical) {
          compareRecord(key, leftHead, rightHead)
        } else {
          if (multipleMatchesCount < maxDiffExamples) {
            multipleMatchesExamples ::= recordRawDiffsAccumulator.multipleMatchesExampleBuilder.buildDifferent(
              key,
              Iterator.fill(leftHeadCount)(leftHead) ++ Iterator.single(lastLeftCompared) ++ leftTail,
              Iterator.fill(rightHeadCount)(rightHead) ++ Iterator.single(lastRightCompared) ++ rightTail
            )
          }
          multipleMatchesCount += 1
          MULTIPLE_MATCHES
        }
      } else {
        if (multipleMatchesCount < maxDiffExamples) {
          multipleMatchesExamples ::= recordRawDiffsAccumulator.multipleMatchesExampleBuilder.buildDifferent(
            key,
            Iterator.single(leftHead) ++ leftTail,
            Iterator.single(rightHead) ++ rightTail
          )
        }
        multipleMatchesCount += 1
        MULTIPLE_MATCHES
      }
    }
  }

  @inline private def compareRecord(key: K, left: R, right: R): KindOfDiff = {
    val flags = recordRawDiffsAccumulator.compareRecords(key, left, right)
    if ((flags | IDENTICAL) == IDENTICAL) {
      record.identical()
    } else {
      record.different(key, left, right)
      // we may be different due to multiple matches, without any other differences.
      val diffOnlyFlags = flags & ~(IDENTICAL | MULTIPLE_MATCHES)
      if ((diffOnlyFlags & DIFFERENT) == DIFFERENT) {
        hasDifferentFieldCount += 1
      } else if (diffOnlyFlags == LEFT_ONLY) {
        hasOnlyLeftOnlyFieldsCount += 1
      } else if (diffOnlyFlags == RIGHT_ONLY) {
        hasOnlyRightOnlyFieldsCount += 1
      } else if (diffOnlyFlags == (LEFT_ONLY | RIGHT_ONLY)) {
        hasOnlyLeftAndRightOnlyFieldsCount += 1
      }
    }
    flags
  }

  def leftOnly(key: K, leftHead: R, leftTail: Iterator[R]): KindOfDiff = {
    if (!leftTail.hasNext) {
      record.leftOnly(key, leftHead)
    } else {
      if (omitMultipleMatchesIfAllIdentical) {
        val (headCount, lastCompared, allIdentical) = compareLeftUntilDifferentFromHead(leftHead, leftTail)
        if (allIdentical) {
          record.leftOnly(key, leftHead)
        } else {
          if (multipleMatchesCount < maxDiffExamples) {
            multipleMatchesExamples ::= recordRawDiffsAccumulator.multipleMatchesExampleBuilder.buildDifferent(
              key,
              Iterator.fill(headCount)(leftHead) ++ Iterator.single(lastCompared) ++ leftTail,
              Iterator.empty
            )
          }
          multipleMatchesCount += 1
          MULTIPLE_MATCHES
        }
      } else {
        if (multipleMatchesCount < maxDiffExamples) {
          multipleMatchesExamples ::= recordRawDiffsAccumulator.multipleMatchesExampleBuilder.buildDifferent(
            key,
            Iterator.single(leftHead) ++ leftTail,
            Iterator.empty
          )
        }
        multipleMatchesCount += 1
        MULTIPLE_MATCHES
      }
    }
  }

  @inline private def compareLeftUntilDifferentFromHead(head: R, tail: Iterator[R]): (Int, R, Boolean) = {
    var headCount = 0
    var next = head
    var identical = true
    while (identical && tail.hasNext) {
      headCount += 1
      next = tail.next()
      identical = recordRawDiffsAccumulator.isLeftEqual(head, next)
    }
    (headCount, next, identical)
  }

  def rightOnly(key: K, rightHead: R, rightTail: Iterator[R]): KindOfDiff = {
    if (!rightTail.hasNext) {
      record.rightOnly(key, rightHead)
    } else {
      if (omitMultipleMatchesIfAllIdentical) {
        val (headCount, lastCompared, allIdentical) = compareRightUntilDifferentFromHead(rightHead, rightTail)
        if (allIdentical) {
          record.rightOnly(key, rightHead)
        } else {
          if (multipleMatchesCount < maxDiffExamples) {
            multipleMatchesExamples ::= recordRawDiffsAccumulator.multipleMatchesExampleBuilder.buildDifferent(
              key,
              Iterator.empty,
              Iterator.fill(headCount)(rightHead) ++ Iterator.single(lastCompared) ++ rightTail
            )
          }
          multipleMatchesCount += 1
          MULTIPLE_MATCHES
        }
      } else {
        if (multipleMatchesCount < maxDiffExamples) {
          multipleMatchesExamples ::= recordRawDiffsAccumulator.multipleMatchesExampleBuilder.buildDifferent(
            key,
            Iterator.empty,
            Iterator.single(rightHead) ++ rightTail
          )
        }
        multipleMatchesCount += 1
        MULTIPLE_MATCHES
      }
    }
  }

  @inline private def compareRightUntilDifferentFromHead(head: R, tail: Iterator[R]): (Int, R, Boolean) = {
    var headCount = 0
    var next = head
    var identical = true
    while (identical && tail.hasNext) {
      headCount += 1
      next = tail.next()
      identical = recordRawDiffsAccumulator.isRightEqual(head, next)
    }
    (headCount, next, identical)
  }

  def result: MatchedRecordsRawDiffsPart[Kx, Rx] = {
    MatchedRecordsRawDiffsPart[Kx, Rx](
      multipleMatches = RawDiffPart(multipleMatchesCount, multipleMatchesExamples),
      kindOfDifferentRecords = KindOfDifferentRecords(
        hasDifferentNotNullField = hasDifferentFieldCount,
        hasOnlyLeftOnlyFields = hasOnlyLeftOnlyFieldsCount,
        hasOnlyRightOnlyFields = hasOnlyRightOnlyFieldsCount,
        hasOnlyLeftAndRightOnlyFields = hasOnlyLeftAndRightOnlyFieldsCount
      ),
      recordDiffs = record.result(binaryIdenticalRecords),
      fieldsDiffs = recordRawDiffsAccumulator.results.toMap
    )
  }
}
