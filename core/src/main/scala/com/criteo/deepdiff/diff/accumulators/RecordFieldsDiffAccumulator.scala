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
import com.criteo.deepdiff.raw_part.{CommonRawDiffsPart, KindOfDiff, LeftRightRawDiffsPart, RawDiffPart}
import com.criteo.deepdiff.type_support.unsafe.{UnsafeComparator, UnsafeComparison}
import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.mutable

/** Used to compare all the fields of a record and aggregate their results.
  *
  * @param unsafeRecordComparator Used before comparing the fields individually for performance.
  * @param diffAccumulators A [[FieldDiffAccumulator]] for each field that needs to be compared. It
  *                         must include all the fieldsWithNestedRecords.
  * @param fieldsWithNestedRecords When the binary comparison is sufficient to prove equality of the
  *                                records, we still don't know which fields are actually present or
  *                                null. This is important for nested fields as it changes the identical
  *                                count. For example, if the nested struct was null the identical count
  *                                of its fields is not increased, but it should be otherwise.
  * @param keysFullName            Key fields present in the compared record. A [[FieldDiffAccumulator]] won't be
  *                                generated for them as they've already been compared through the key. But we still
  *                                need to generate a [[LeftRightRawDiffsPart]] entry in the report. As their identical
  *                                depends on the number of matched records, this is the easiest place to do so.
  */
private[diff] final class RecordFieldsDiffAccumulator private (
    unsafeRecordComparator: UnsafeComparator[InternalRow],
    diffAccumulators: Array[FieldDiffAccumulator],
    fieldsWithNestedRecords: Array[FieldWithNestedRecordsDiffAccumulator],
    keysFullName: Set[String]
) extends Serializable {
  import KindOfDiff._

  // Sanity checks
  require(diffAccumulators.map(_.fullName).toSet.size == diffAccumulators.length)
  require(diffAccumulators.map(_.fullName).toSet.intersect(keysFullName).isEmpty)
  require(fieldsWithNestedRecords.toSet[FieldDiffAccumulator].subsetOf(diffAccumulators.toSet))

  /** Number of binary identical records. */
  private var binaryIdenticalCount: Long = 0

  /** Number of comparison (matches) made that weren't binary identical. */
  private var deepComparisonCount: Long = 0

  /** When the parent record is binary equal, we just need process the nested fields. */
  @inline def binaryIdenticalRecord(record: R): Unit = {
    binaryIdenticalCount += 1
    var i = 0
    while (i < fieldsWithNestedRecords.length) {
      fieldsWithNestedRecords(i).binaryIdenticalRecord(record)
      i += 1
    }
  }

  /** Same as binaryIdenticalLeftRecord, but used for the arrays of structs. */
  @inline private[accumulators] def binaryIdenticalRecords(records: Iterator[R]): Int = {
    var notNullRecordsCount: Int = 0
    while (records.hasNext) {
      notNullRecordsCount += 1
      binaryIdenticalRecord(records.next())
    }
    notNullRecordsCount
  }

  @inline def compareRecords(key: K, left: R, right: R): KindOfDiff = {
    import UnsafeComparison._
    unsafeRecordComparator.compare(left, right) match {
      case Equal =>
        binaryIdenticalRecord(left)
        IDENTICAL
      case _ =>
        deepComparisonCount += 1
        // anyNull is a lot faster in UnsafeRows than checking individually for each field.
        val leftAnyNull = left.anyNull
        val rightAnyNull = right.anyNull
        var flags: KindOfDiff = NEUTRAL
        var i = 0
        while (i < diffAccumulators.length) {
          flags |= diffAccumulators(i).compareRecords(key, left, leftAnyNull, right, rightAnyNull)
          i += 1
        }
        flags
    }
  }

  def result(): List[(String, LeftRightRawDiffsPart[Kx, Any])] =
    generateKeyResult() ::: RecordFieldsDiffAccumulator.mergeResults(diffAccumulators, binaryIdenticalCount)

  private def generateKeyResult(): List[(String, LeftRightRawDiffsPart[Kx, Any])] = {
    val binaryFieldDiff =
      CommonRawDiffsPart[Kx, Any](identical = deepComparisonCount + binaryIdenticalCount,
                                  different = RawDiffPart.empty,
                                  leftOnly = RawDiffPart.empty,
                                  rightOnly = RawDiffPart.empty)
    keysFullName.foldRight[List[(String, LeftRightRawDiffsPart[Kx, Any])]](Nil) {
      case (fullName, acc) => (fullName -> binaryFieldDiff) :: acc
    }
  }
}

/** Base interface for all field difference accumulators. */
private[diff] trait FieldDiffAccumulator extends Serializable {

  /** Used to assert uniqueness of each field. */
  private[accumulators] def fullName: String

  /** Compare the field of the record.
    *
    * @param key Matching key
    * @param left Left record
    * @param leftAnyNull Whether there are any null fields on the left. Used as Spark can
    *                    (very) cheaply test this and checking field nullity is actually
    *                    not that negligible as it's the most frequent operation.
    * @param right Right record
    * @param rightAnyNull Similar to leftAnyNull, but for the right record.
    */
  private[accumulators] def compareRecords(key: K,
                                           left: R,
                                           leftAnyNull: Boolean,
                                           right: R,
                                           rightAnyNull: Boolean): KindOfDiff

  /** Returns the generated [[LeftRightRawDiffsPart]] to be included in the report. Returning a
    * list as nested fields should also be included.
    *
    * @param binaryIdenticalRecords Number of records which were binary identical, hence
    *                               their fields were never compared with compareRecords().
    */
  private[accumulators] def result(binaryIdenticalRecords: Long): List[(String, LeftRightRawDiffsPart[Kx, Any])]
}

/** If nested fields are present, additional processing needs to be done to have an
  * accurate identical count in case of binary identical records.
  */
private[diff] trait FieldWithNestedRecordsDiffAccumulator extends FieldDiffAccumulator {

  /** Forbidding the use of the binaryIdenticalRecords as the whole purpose of this class
    * is to NOT rely on this because you CANNOT. Instead whenever a record is identical
    * binaryIdenticalLeftRecord() will be called and you MUST correctly increase the
    * identical count for all concerned fields.
    */
  private[accumulators] final def result(binaryIdenticalRecords: Long): List[(String, LeftRightRawDiffsPart[Kx, Any])] =
    result()
  private[accumulators] def result(): List[(String, LeftRightRawDiffsPart[Kx, Any])]

  /** Called whenever a record is binary identical. */
  private[accumulators] def binaryIdenticalRecord(left: R): Unit
}

private[diff] object RecordFieldsDiffAccumulator {
  def apply(unsafeRecordComparator: UnsafeComparator[InternalRow],
            diffAccumulators: Array[FieldDiffAccumulator],
            keysFullName: Set[String]): RecordFieldsDiffAccumulator = {
    val fieldsWithNestedRecords: Array[FieldWithNestedRecordsDiffAccumulator] = diffAccumulators.flatMap({
      case d: FieldWithNestedRecordsDiffAccumulator => Some(d)
      case _                                        => None
    })
    new RecordFieldsDiffAccumulator(unsafeRecordComparator, diffAccumulators, fieldsWithNestedRecords, keysFullName)
  }

  private[accumulators] def mergeResults(
      diffAccumulators: Seq[FieldDiffAccumulator],
      binaryIdenticalRecords: Long = 0): List[(String, LeftRightRawDiffsPart[Kx, Any])] = {
    val builder = new mutable.ListBuffer[(String, LeftRightRawDiffsPart[Kx, Any])]
    diffAccumulators.foreach(accumulator => {
      builder ++= accumulator.result(binaryIdenticalRecords)
    })
    builder.toList
  }
}
