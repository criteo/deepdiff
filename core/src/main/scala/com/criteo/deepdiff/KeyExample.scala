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

package com.criteo.deepdiff

/** Base class for all keys used to matched records */
sealed trait KeyExample {
  private[deepdiff] def nested(key: SimpleKeyExample): CompositeKeyExample
}

/** Used to differentiate keys from [[CompositeKeyExample]] */
sealed trait SimpleKeyExample extends KeyExample {
  private[deepdiff] def nested(key: SimpleKeyExample): CompositeKeyExample = CompositeKeyExample(key :: this :: Nil)
}

/** Key consisting of one or more fields. Null fields will be represented as None. */
final case class FieldsKeyExample(fields: Map[String, Option[Any]]) extends SimpleKeyExample

/** Representing the position of an element in an array. */
final case class PositionKeyExample(position: Int) extends SimpleKeyExample

/** Used to merge the root key with one or more nested keys, in exploded arrays for example. */
final case class CompositeKeyExample(keyExamples: List[SimpleKeyExample]) extends KeyExample {
  private[deepdiff] def nested(key: SimpleKeyExample): CompositeKeyExample = CompositeKeyExample(key :: keyExamples)
}
