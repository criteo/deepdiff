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
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.ArrayData

private[comparator] final case class ArrayComparator private[comparator] (
    elementComparator: Comparator,
    unsafeArrayComparator: UnsafeComparator[ArrayData])
    extends ObjectComparator[ArrayData] {

  @inline protected def get(input: SpecializedGetters, ordinal: Int): ArrayData = input.getArray(ordinal)

  @inline protected def isEqual(x: ArrayData, y: ArrayData): Boolean = {
    import UnsafeComparison._

    unsafeArrayComparator.compare(x, y) match {
      case Equal    => true
      case NotEqual => false
      case _ =>
        val length = x.numElements()
        var i = 0
        var isEqual = true
        while (i < length && isEqual) {
          isEqual = elementComparator.isFieldEqual(x, i, y, i)
          i += 1
        }
        isEqual
    }
  }
}
