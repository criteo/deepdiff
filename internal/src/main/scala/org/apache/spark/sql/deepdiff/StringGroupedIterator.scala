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
 * Adapted from org.apache.spark.sql.execution.GroupedIterator (Spark 2.4.6)
 */

package org.apache.spark.sql.deepdiff

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, BoundReference, Expression, UnsafeRow}
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

object StringGroupedIterator {
  def apply(input: Iterator[UnsafeRow],
            keyExpression: Expression,
            inputSchema: Seq[Attribute]): Iterator[(UTF8String, Iterator[InternalRow])] = {
    require(keyExpression.dataType.isInstanceOf[StringType])
    if (input.hasNext) {
      new StringGroupedIterator(input.buffered, keyExpression, inputSchema)
    } else {
      Iterator.empty
    }
  }
}

/**
  * Iterates over a presorted set of rows, chunking it up by the grouping expression.  Each call to
  * next will return a pair containing the current group and an iterator that will return all the
  * elements of that group.  Iterators for each group are lazily constructed by extracting rows
  * from the input iterator.  As such, full groups are never materialized by this class.
  *
  * Example input:
  * {{{
  *   Input: [a, 1], [b, 2], [b, 3]
  *   Grouping: x#1
  *   InputSchema: x#1, y#2
  * }}}
  *
  * Result:
  * {{{
  *   First call to next():  ([a], Iterator([a, 1])
  *   Second call to next(): ([b], Iterator([b, 2], [b, 3])
  * }}}
  *
  * Note, the class does not handle the case of an empty input for simplicity of implementation.
  * Use the factory to construct a new instance.
  *
  * @param input An iterator of rows.  This iterator must be ordered by the groupingExpressions or
  *              it is possible for the same group to appear more than once.
  * @param keyExpression The set of expressions used to do grouping.  The result of evaluating
  *                      these expressions will be returned as the first part of each call
  *                      to `next()`.
  * @param inputSchema The schema of the rows in the `input` iterator.
  */
final class StringGroupedIterator private (input: BufferedIterator[UnsafeRow],
                                           keyExpression: Expression,
                                           inputSchema: Seq[Attribute])
    extends Iterator[(UTF8String, Iterator[InternalRow])] {
  private val keyOrdinal =
    BindReferences.bindReference(keyExpression, inputSchema).asInstanceOf[BoundReference].ordinal

  /** Holds a copy of the head of the group. */
  private val head = {
    val head = new UnsafeRow(inputSchema.length)
    // Size will be adapted whenever necessary.
    head.pointTo(new Array[Byte](1024), 1024)
    head.copyFrom(input.next())
    head
  }

  /**
    * Holds null or the row that will be returned on next call to `next()` in the inner iterator.
    */
  private var currentRow = head

  /** Holds a copy of an input row that is in the current group. */
  private var currentGroup = head.getUTF8String(keyOrdinal)
  private var lastCompareTo = -1 // anything but zero

  private var currentIterator = createGroupValuesIterator()

  /**
    * Return true if we already have the next iterator or fetching a new iterator is successful.
    *
    * Note that, if we get the iterator by `next`, we should consume it before call `hasNext`,
    * because we will consume the input data to skip to next group while fetching a new iterator,
    * thus make the previous iterator empty.
    */
  def hasNext: Boolean = currentIterator != null || fetchNextGroupIterator

  def next(): (UTF8String, Iterator[InternalRow]) = {
    assert(hasNext) // Ensure we have fetched the next iterator.
    val ret = (currentGroup, currentIterator)
    currentIterator = null
    ret
  }

  private def fetchNextGroupIterator(): Boolean = {
    assert(currentIterator == null)

    if (currentRow == null && input.hasNext) {
      currentRow = input.next()
    }

    if (currentRow == null) {
      // These is no data left, return false.
      false
    } else {
      // Skip to next group.
      // currentRow may be overwritten by `hasNext`, so we should compare them first.
      while (lastCompareTo == 0 && input.hasNext) {
        currentRow = input.next()
        lastCompareTo = currentGroup.compareTo(currentRow.getUTF8String(keyOrdinal))
      }

      if (lastCompareTo == 0) {
        // We are in the last group, there is no more groups, return false.
        false
      } else {
        // Now the `currentRow` is the first row of next group.
        head.copyFrom(currentRow)
        currentRow = head // point currentRow to copied data.
        currentGroup = head.getUTF8String(keyOrdinal)
        currentIterator = createGroupValuesIterator()
        true
      }
    }
  }

  private def createGroupValuesIterator(): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      def hasNext: Boolean = currentRow != null || fetchNextRowInGroup()

      def next(): InternalRow = {
        assert(hasNext)
        val res = currentRow
        currentRow = null
        res
      }

      private def fetchNextRowInGroup(): Boolean = {
        assert(currentRow == null)

        if (input.hasNext) {
          // The inner iterator should NOT consume the input into next group, here we use `head` to
          // peek the next input, to see if we should continue to process it.
          lastCompareTo = currentGroup.compareTo(input.head.getUTF8String(keyOrdinal))
          if (lastCompareTo == 0) {
            // Next input is in the current group.  Continue the inner iterator.
            currentRow = input.next()
            true
          } else {
            // Next input is not in the right group.  End this inner iterator.
            false
          }
        } else {
          // There is no more data, return false.
          false
        }
      }
    }
  }
}
