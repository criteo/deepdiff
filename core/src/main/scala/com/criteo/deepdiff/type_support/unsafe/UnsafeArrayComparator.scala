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

import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.sql.catalyst.util.ArrayData

private[unsafe] abstract class UnsafeArrayComparator extends UnsafeComparator[ArrayData] {
  import UnsafeComparison._
  @inline def compare(x: ArrayData, y: ArrayData): UnsafeComparison = {
    if (x.numElements() != y.numElements()) {
      NotEqual
    } else if (x.numElements() == 0) {
      Equal
    } else if (x.isInstanceOf[UnsafeArrayData] && y.isInstanceOf[UnsafeArrayData]) {
      compareUnsafe(x.asInstanceOf[UnsafeArrayData], y.asInstanceOf[UnsafeArrayData])
    } else {
      Unknown
    }
  }

  @inline protected def compareUnsafe(x: UnsafeArrayData, y: UnsafeArrayData): UnsafeComparison
}
