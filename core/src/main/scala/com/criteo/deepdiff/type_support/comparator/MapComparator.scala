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
import org.apache.spark.sql.catalyst.util.MapData

import scala.collection.mutable

private[comparator] final case class MapComparator(notNullKeyAccessor: (SpecializedGetters, Int) => Any,
                                                   keyComparator: Comparator,
                                                   valueComparator: Comparator,
                                                   unsafeMapComparator: UnsafeComparator[MapData])
    extends ObjectComparator[MapData] {

  @inline protected def get(input: SpecializedGetters, ordinal: Int): MapData = input.getMap(ordinal)

  @inline protected def isEqual(x: MapData, y: MapData): Boolean = {
    import UnsafeComparison._

    unsafeMapComparator.compare(x, y) match {
      case Equal    => true
      case NotEqual => false
      case binaryComparison =>
        val xValues = x.valueArray()
        val yValues = y.valueArray()
        val length = xValues.numElements()
        var isEqual = true

        binaryComparison match {
          case OnlyMapKeysAreEqual =>
            var i = 0
            while (i < length && isEqual) {
              isEqual = valueComparator.isFieldEqual(xValues, i, yValues, i)
              i += 1
            }
            isEqual
          case _ =>
            val xKeys = x.keyArray()
            val yKeys = y.keyArray()
            val xKeyToIndex = mutable.OpenHashMap[Any, Int]()
            val yVisited = new java.util.BitSet(length)
            var i = 0
            while (i < length) {
              xKeyToIndex += notNullKeyAccessor(xKeys, i) -> i
              i += 1
            }
            var y_i = 0
            while (y_i < length && isEqual) {
              isEqual = xKeyToIndex.get(notNullKeyAccessor(yKeys, y_i)) match {
                case Some(x_i) =>
                  yVisited.set(x_i)
                  valueComparator.isFieldEqual(xValues, x_i, yValues, y_i)
                case _ => false
              }
              y_i += 1
            }
            isEqual && (yVisited.nextClearBit(0) == length)
        }
    }
  }
}
