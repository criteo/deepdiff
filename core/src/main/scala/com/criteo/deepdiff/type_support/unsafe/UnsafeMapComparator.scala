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

import org.apache.spark.sql.catalyst.expressions.{UnsafeArrayData, UnsafeMapData}
import org.apache.spark.sql.catalyst.util.MapData

private[unsafe] abstract class UnsafeMapComparator extends UnsafeComparator[MapData] {
  import UnsafeComparison._
  @inline def compare(x: MapData, y: MapData): UnsafeComparison = {
    if (x.numElements() != y.numElements()) {
      NotEqual
    } else if (x.numElements() == 0) {
      Equal
    } else if (x.isInstanceOf[UnsafeMapData] && y.isInstanceOf[UnsafeMapData]) {
      val unsafeX = x.asInstanceOf[UnsafeMapData]
      val unsafeY = y.asInstanceOf[UnsafeMapData]
      val xKeys = unsafeX.keyArray()
      val yKeys = unsafeY.keyArray()
      if (xKeys.getSizeInBytes != yKeys.getSizeInBytes) {
        NotEqual
      } else {
        compareUnsafeValues(xKeys == yKeys, unsafeX.valueArray(), unsafeY.valueArray())
      }
    } else {
      Unknown
    }
  }

  @inline protected def compareUnsafeValues(equalKeys: Boolean,
                                            x: UnsafeArrayData,
                                            y: UnsafeArrayData): UnsafeComparison
}
