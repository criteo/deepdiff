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

import com.criteo.deepdiff.{Diff, DifferentExample, LeftOnlyExample, RightOnlyExample}

/** Used to generate human friendly examples from raw ones.
  *
  * See [[RawDiffExampleBuilder]] for more information.
  *
  * @tparam Kx raw key example
  * @tparam Fx raw field example
  * @tparam Key human friendly key example
  * @tparam Field human friendly field example
  */
private[deepdiff] abstract class RawToFriendlyDiffExample[Kx, Fx, Key, Field] extends Serializable {

  protected def rawToFriendlyKey(rawKey: Kx): Key
  protected def rawToFriendlyLeft(left: Fx): Field
  protected def rawToFriendlyRight(right: Fx): Field

  final def different(raw: RawDiffPart[Kx, DifferentExample[Fx]]): Diff[Key, DifferentExample[Field]] =
    Diff(
      count = raw.count,
      examples = raw.examples
        .map({
          case (key, example) =>
            rawToFriendlyKey(key) -> DifferentExample(
              left = rawToFriendlyLeft(example.left),
              right = rawToFriendlyRight(example.right)
            )
        })
        .toMap
    )

  final def leftOnly(raw: RawDiffPart[Kx, LeftOnlyExample[Fx]]): Diff[Key, LeftOnlyExample[Field]] =
    Diff(
      count = raw.count,
      examples = raw.examples
        .map({
          case (key, example) => rawToFriendlyKey(key) -> LeftOnlyExample(rawToFriendlyLeft(example.left))
        })
        .toMap
    )

  final def rightOnly(raw: RawDiffPart[Kx, RightOnlyExample[Fx]]): Diff[Key, RightOnlyExample[Field]] =
    Diff(
      count = raw.count,
      examples = raw.examples
        .map({
          case (key, example) => rawToFriendlyKey(key) -> RightOnlyExample(rawToFriendlyRight(example.right))
        })
        .toMap
    )
}
