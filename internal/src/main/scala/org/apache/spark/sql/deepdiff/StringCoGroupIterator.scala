/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/* Modifications copyright 2020 Criteo
 * Adapted from org.apache.spark.sql.execution.CoGroupedIterator (Spark 2.4.6)
 */

package org.apache.spark.sql.deepdiff

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String

/**
  * Iterates over [[StringGroupedIterator]]s and returns the co-grouped data, i.e. each record is a
  * grouping key with its associated values from all [[StringGroupedIterator]]s.
  * Note: we assume the output of each [[StringGroupedIterator]] is ordered by the grouping key.
  */
final class StringCoGroupIterator(left: Iterator[(UTF8String, Iterator[InternalRow])],
                                  right: Iterator[(UTF8String, Iterator[InternalRow])])
    extends Iterator[(UTF8String, Iterator[InternalRow], Iterator[InternalRow])] {

  private var currentLeftData: (UTF8String, Iterator[InternalRow]) = _
  private var currentRightData: (UTF8String, Iterator[InternalRow]) = _

  override def hasNext: Boolean = {
    if (currentLeftData == null && left.hasNext) {
      currentLeftData = left.next()
    }
    if (currentRightData == null && right.hasNext) {
      currentRightData = right.next()
    }

    currentLeftData != null || currentRightData != null
  }

  override def next(): (UTF8String, Iterator[InternalRow], Iterator[InternalRow]) = {
    assert(hasNext)

    if (currentLeftData == null) {
      // left is null, right is not null, consume the right data.
      rightOnly()
    } else if (currentRightData == null) {
      // left is not null, right is null, consume the left data.
      leftOnly()
    } else {
      val compare = currentLeftData._1.compareTo(currentRightData._1)
      if (compare == 0) {
        // left and right have the same grouping key, consume both of them.
        val result = (currentLeftData._1, currentLeftData._2, currentRightData._2)
        currentLeftData = null
        currentRightData = null
        result
      } else if (compare < 0) {
        // the grouping key of left is smaller, consume the left data.
        leftOnly()
      } else {
        // the grouping key of right is smaller, consume the right data.
        rightOnly()
      }
    }
  }

  private def leftOnly(): (UTF8String, Iterator[InternalRow], Iterator[InternalRow]) = {
    val result = (currentLeftData._1, currentLeftData._2, Iterator.empty)
    currentLeftData = null
    result
  }

  private def rightOnly(): (UTF8String, Iterator[InternalRow], Iterator[InternalRow]) = {
    val result = (currentRightData._1, Iterator.empty, currentRightData._2)
    currentRightData = null
    result
  }
}
