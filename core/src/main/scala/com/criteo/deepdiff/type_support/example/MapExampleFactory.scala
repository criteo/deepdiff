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
import org.apache.spark.sql.catalyst.util.MapData

private[example] final case class MapExampleFactory(keyExample: ExampleFactory, valueExample: ExampleFactory)
    extends ExampleFactory {
  type RawExample = MapData
  type FriendlyExample = Map[Any, Any]

  protected def getNotNullRaw(input: SpecializedGetters, ordinal: Int): MapData = input.getMap(ordinal).copy()

  override private[example] def getFriendly(input: SpecializedGetters, ordinal: Int): Map[Any, Any] =
    rawToFriendly(input.getMap(ordinal))

  protected def rawToFriendly(rawExample: MapData): Map[Any, Any] =
    buildFriendly(rawExample, keyExample.getFriendly, valueExample.getFriendly)

  @inline private def buildFriendly(example: MapData,
                                    getNotNullKeyExample: (SpecializedGetters, Int) => Any,
                                    getNotNullValueExample: (SpecializedGetters, Int) => Any): Map[Any, Any] = {
    val keyArray = example.keyArray()
    val valueArray = example.valueArray()
    val builder = Map.newBuilder[Any, Any]
    var i = 0
    while (i < example.numElements()) {
      assert(!keyArray.isNullAt(i))
      val key = getNotNullKeyExample(keyArray, i)
      builder += key -> (if (!valueArray.isNullAt(i)) getNotNullValueExample(valueArray, i) else null)
      i += 1
    }
    builder.result()
  }
}
