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

package com.criteo.deepdiff.raw_part

import com.criteo.deepdiff.{DifferentExample, LeftOnlyExample, RightOnlyExample}

/** Used to generate a RAW example of a field/record.
  *
  * Raw examples are generated on the spot, in the executors, they do not require to be human
  * friendly and hence can stay in serialized/binary format. Human friendly examples are not
  * always needed as each executor may generate `maxExamples` examples and we only need
  * `maxExamples` at the end. So human friendly examples are only generated in the driver
  * once everything has been aggregated.
  *
  * @tparam K key
  * @tparam R record
  * @tparam Kx raw key example
  * @tparam Fx raw field example
  */
private[deepdiff] abstract class RawDiffExampleBuilder[K, R, Kx, Fx] extends Serializable {

  @inline protected def asRawKeyExample(key: K): Kx
  @inline protected def asRawLeftExample(left: R): Fx
  @inline protected def asRawRightExample(right: R): Fx

  @inline final def buildDifferent(key: K, left: R, right: R): (Kx, DifferentExample[Fx]) =
    asRawKeyExample(key) -> DifferentExample(
      left = asRawLeftExample(left),
      right = asRawRightExample(right)
    )

  @inline final def buildLeftOnly(key: K, left: R): (Kx, LeftOnlyExample[Fx]) =
    asRawKeyExample(key) -> LeftOnlyExample(asRawLeftExample(left))

  @inline final def buildRightOnly(key: K, right: R): (Kx, RightOnlyExample[Fx]) =
    asRawKeyExample(key) -> RightOnlyExample(asRawRightExample(right))
}
