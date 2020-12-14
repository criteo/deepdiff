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

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class KeyExampleSpec extends AnyWordSpec with Matchers {
  "Simple keys should be nestable" in {
    FieldsKeyExample(Map("key" -> Some("a"))).nested(PositionKeyExample(1)) should be(
      CompositeKeyExample(
        PositionKeyExample(1) :: FieldsKeyExample(Map("key" -> Some("a"))) :: Nil
      ))
  }

  "Composite keys should support adding a new nested key" in {
    CompositeKeyExample(PositionKeyExample(1) :: Nil).nested(PositionKeyExample(2)) should be(
      CompositeKeyExample(PositionKeyExample(2) :: PositionKeyExample(1) :: Nil)
    )
  }
}
