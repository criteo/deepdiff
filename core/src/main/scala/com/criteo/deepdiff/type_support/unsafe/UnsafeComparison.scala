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

package com.criteo.deepdiff.type_support.unsafe

/** Enum representing the result of the binary comparison */
private[deepdiff] sealed trait UnsafeComparison

private[deepdiff] object UnsafeComparison {
  case object Equal extends UnsafeComparison
  case object NotEqual extends UnsafeComparison
  case object Unknown extends UnsafeComparison
  // Specific to map
  case object OnlyMapKeysAreEqual extends UnsafeComparison
}
