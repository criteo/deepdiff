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

package com.criteo.deepdiff.type_support.example

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.ArrayData

private[deepdiff] final case class ArrayExampleFactory[T <: ExampleFactory] private[example] (elementHelper: T)
    extends ExampleFactory {
  type RawExample = ArrayData
  type FriendlyExample = Seq[Any]

  protected def getNotNullRaw(input: SpecializedGetters, ordinal: Int): ArrayData = input.getArray(ordinal).copy()

  protected def rawToFriendly(rawExample: ArrayData): Seq[Any] =
    buildFriendly(rawExample, elementHelper.getFriendly)

  override private[example] def getFriendly(input: SpecializedGetters, ordinal: Int): Seq[Any] =
    rawToFriendly(input.getArray(ordinal))

  @inline private def buildFriendly(example: ArrayData, getNotNullExample: (ArrayData, Int) => Any): Seq[Any] = {
    val builder = Seq.newBuilder[Any]
    var i = 0
    while (i < example.numElements()) {
      builder += (if (!example.isNullAt(i)) getNotNullExample(example, i) else null)
      i += 1
    }
    builder.result()
  }
}
