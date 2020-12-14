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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String

package object diff {
  // "Generics" used everywhere. Type aliases are used as they provide some consistency between the `raw_part`
  // and `diff` packages. It also allows to distinct the cases when it is expected to match the interfaces defined in
  // `raw_part` and when it has no connection. For example `UTF8String` is also when comparing string values and when
  // generating keys.
  type K = List[UTF8String]
  type R = InternalRow
  // raw example
  type Kx = UTF8String
  type Rx = R
}
