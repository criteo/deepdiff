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

package com.criteo.deepdiff.diff

import com.criteo.deepdiff.raw_part.{
  DatasetRawDiffsPart,
  MatchedRecordDiffsPartBuilder,
  RecordRawDiffsPartBuilder
}
import com.criteo.deepdiff.utils.Common
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String

/** Interface between the Spark operators and the DeepDiff comparison code.
  *
  * Hence it is the only part aware of what operators have been used or not. The rest of the code does not suppose
  * anything except manipulating an [[InternalRow]]
  *
  * @param numFields number of fields on each side, to retrieve the record. This functions receives the
  *                  row (key, record).
  * @param root [[MatchedRecordDiffsPartBuilder]] for the "root" record, so it's the only one called for comparing the records.
  * @param explodedArrays All the [[RecordRawDiffsPartBuilder]] for the exploded arrays, used only to build the result.
  *                       The actual comparison should be done through the `root` builder.
  */
private[deepdiff] final class DatasetDiffAccumulator(numFields: Common[Int],
                                                     root: MatchedRecordDiffsPartBuilder[K, R, Kx, Rx],
                                                     explodedArrays: Map[String, RecordRawDiffsPartBuilder[Kx, Rx]])
    extends Serializable {

  def processCoGroupPartition(
      rows: Iterator[(UTF8String, Iterator[R], Iterator[R])]): Iterator[DatasetRawDiffsPart[Kx, Rx]] = {
    val leftNumFields = numFields.left
    val rightNumFields = numFields.right
    val getLeft = (r: InternalRow) => r.getStruct(1, leftNumFields)
    val getRight = (r: InternalRow) => r.getStruct(1, rightNumFields)

    rows.foreach({
      // /!\ === /!\ === /!\ === /!\ === /!\ === /!\ === /!\ === /!\ === /!\ === /!\ === /!\ === /!\ === /!\ //
      //   Functions should be *extremely* careful when manipulating the iterators. Spark re-uses the same   //
      //     row object. Thus once hasNext() has been called, any previous row should be discarded. Here     //
      //   only the head is guaranteed to be safe by the underlying StringGroupedIterator which copies it.   //
      // /!\ === /!\ === /!\ === /!\ === /!\ === /!\ === /!\ === /!\ === /!\ === /!\ === /!\ === /!\ === /!\ //
      case (key, l: Iterator[InternalRow], r: Iterator[InternalRow]) =>
        val left = l.map(getLeft)
        val right = r.map(getRight)
        if (!right.hasNext) {
          root.leftOnly(key :: Nil, left.next(), left)
        } else if (!left.hasNext) {
          root.rightOnly(key :: Nil, right.next(), right)
        } else {
          root.compare(key :: Nil, left.next(), left, right.next(), right)
        }
    })
    Iterator.single(
      DatasetRawDiffsPart(
        root.result,
        explodedArrays.map({ case (field, builder) => field -> builder.result })
      ))
  }
}
