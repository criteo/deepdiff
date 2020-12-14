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

package com.criteo.deepdiff.test_utils

import com.criteo.deepdiff.{FieldsKeyExample, KeyExample, PositionKeyExample, SimpleKeyExample}

object EasyKeyExample {
  def Key(k: String): SimpleKeyExample =
    FieldsKeyExample(Map("key" -> Option(k)))

  def Key(k: (String, Any)*): SimpleKeyExample = {
    require(k.nonEmpty)
    FieldsKeyExample(k.toMap.mapValues(Option(_)))
  }
  def Key(p: Int): SimpleKeyExample = PositionKeyExample(p)
  implicit class KeyOps(val k: KeyExample) extends AnyVal {
    def >>(nested: SimpleKeyExample): KeyExample = k.nested(nested)
  }
}
