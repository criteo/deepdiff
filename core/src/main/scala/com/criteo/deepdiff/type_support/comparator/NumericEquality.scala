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

package com.criteo.deepdiff.type_support.comparator

private[comparator] sealed abstract class NumericEquality {
  @inline def isEqual(x: Double, y: Double): Boolean =
    (x == y) || isCloseEnough(x, y) || (java.lang.Double.isNaN(x) && java.lang.Double.isNaN(y))
  @inline def isEqual(x: Float, y: Float): Boolean =
    (x == y) || isCloseEnough(x.toDouble, y.toDouble) || (java.lang.Float.isNaN(x) && java.lang.Float.isNaN(y))
  @inline def isEqual(x: Long, y: Long): Boolean =
    (x == y) || isCloseEnough(x.toDouble, y.toDouble)
  @inline def isEqual(x: Int, y: Int): Boolean =
    (x == y) || isCloseEnough(x.toDouble, y.toDouble)
  @inline def isEqual(x: Short, y: Short): Boolean =
    (x == y) || isCloseEnough(x.toDouble, y.toDouble)

  @inline protected def isCloseEnough(x: Double, y: Double): Boolean
}

private[comparator] case object StrictTolerance extends NumericEquality {
  @inline def isCloseEnough(x: Double, y: Double): Boolean = false
}

private[comparator] final case class AbsoluteTolerance(threshold: Double) extends NumericEquality {
  @inline def isCloseEnough(x: Double, y: Double): Boolean = Math.abs(x - y) <= threshold
}

private[comparator] final case class RelativeTolerance(threshold: Double) extends NumericEquality {
  @inline def isCloseEnough(x: Double, y: Double): Boolean = {
    import Math._
    (abs(x - y) / min(abs(x), abs(y))) <= threshold
  }
}

private[comparator] final case class AbsoluteAndRelativeTolerance(absoluteThreshold: Double, relativeThreshold: Double)
    extends NumericEquality {
  @inline def isCloseEnough(x: Double, y: Double): Boolean = {
    import Math._
    val diff = abs(x - y)
    (diff <= absoluteThreshold) && ((diff / min(abs(x), abs(y))) <= relativeThreshold)
  }
}

private[comparator] final case class AbsoluteOrRelativeTolerance(absoluteThreshold: Double, relativeThreshold: Double)
    extends NumericEquality {
  @inline def isCloseEnough(x: Double, y: Double): Boolean = {
    import Math._
    val diff = abs(x - y)
    (diff <= absoluteThreshold) || ((diff / min(abs(x), abs(y))) <= relativeThreshold)
  }
}
