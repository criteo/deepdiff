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

package com.criteo.deepdiff.type_support.comparator

import com.criteo.deepdiff.type_support.unsafe.{UnsafeComparator, UnsafeComparison}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters

private[deepdiff] object StructComparator {
  final case class FieldComparator(ordinal: Int, comparator: Comparator)
}

private[deepdiff] final case class StructComparator private[comparator] (
    private val numFields: Int,
    private val fieldComparators: Array[StructComparator.FieldComparator],
    private val unsafeRowComparator: UnsafeComparator[InternalRow])
    extends ObjectComparator[InternalRow] {
  import StructComparator._
  import UnsafeComparison._

  // Sanity check
  require(fieldComparators.length <= numFields)

  @inline protected def get(input: SpecializedGetters, ordinal: Int): InternalRow =
    input.getStruct(ordinal, numFields)

  @inline def isEqual(x: InternalRow, y: InternalRow): Boolean = {
    unsafeRowComparator.compare(x, y) match {
      case Equal    => true
      case NotEqual => false
      case _ =>
        val xAnyNull = x.anyNull
        val yAnyNull = y.anyNull
        var i = 0
        var isEqual = true
        while (i < fieldComparators.length && isEqual) {
          val FieldComparator(ordinal, comparator) = fieldComparators(i)
          isEqual = comparator.isFieldEqual(x, ordinal, xAnyNull, y, ordinal, yAnyNull)
          i += 1
        }
        isEqual
    }
  }
}
