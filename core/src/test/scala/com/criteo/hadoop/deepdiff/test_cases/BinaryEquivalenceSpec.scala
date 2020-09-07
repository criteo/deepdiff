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

package com.criteo.hadoop.deepdiff.test_cases
import com.criteo.hadoop.deepdiff.test_utils.DeepDiffSpec

class BinaryEquivalenceSpec extends DeepDiffSpec {
  "do unsafe comparison correctly" in {
    // how good are ignored fields taken into account ? <<<<- (multiple matches & normal)
    // leftOnly/rightOnly
  }
}
