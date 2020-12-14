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

import com.criteo.deepdiff.config.{Tolerance, ToleranceSatisfactionCriterion}

/** All options relative to the equality of data.
  *
  * Those are propagated to nested types.
  *
  * @param absoluteTolerance Absolute tolerance to be applied when comparing numerical data.
  * @param relativeTolerance Relative tolerance to be applied when comparing numerical data.
  *                          The tolerance is left-centred.
  */
private[deepdiff] final case class EqualityParams(absoluteTolerance: Option[Double],
                                                  relativeTolerance: Option[Double],
                                                  mustSatisfyAbsoluteAndRelativeTolerance: Boolean) {
  require(
    absoluteTolerance.forall(_ > 0) && relativeTolerance.forall(_ > 0),
    s"relative tolerance $relativeTolerance or absolute tolerance $absoluteTolerance is not strictly positive"
  )

  val numericEquality: NumericEquality = {
    (absoluteTolerance, relativeTolerance) match {
      case (Some(absolute), Some(relative)) =>
        if (mustSatisfyAbsoluteAndRelativeTolerance)
          AbsoluteAndRelativeTolerance(absolute, relative)
        else
          AbsoluteOrRelativeTolerance(absolute, relative)
      case (Some(absolute), None) => AbsoluteTolerance(absolute)
      case (None, Some(relative)) => RelativeTolerance(relative)
      case (None, None)           => StrictTolerance
    }
  }
}

private[deepdiff] object EqualityParams {
  val strict: EqualityParams =
    EqualityParams(absoluteTolerance = None, relativeTolerance = None, mustSatisfyAbsoluteAndRelativeTolerance = true)

  def fromConf(tolerance: Option[Tolerance], default: EqualityParams): EqualityParams = {
    EqualityParams(
      absoluteTolerance = tolerance.flatMap(_.absolute) match {
        case absolute @ Some(_) => absolute.filter(_ > 0)
        case None               => default.absoluteTolerance
      },
      relativeTolerance = tolerance.flatMap(_.relative) match {
        case relative @ Some(_) => relative.filter(_ > 0)
        case None               => default.relativeTolerance
      },
      mustSatisfyAbsoluteAndRelativeTolerance = tolerance.flatMap(_.satisfies) match {
        case Some(satisfies) => satisfies == ToleranceSatisfactionCriterion.All
        case None            => default.mustSatisfyAbsoluteAndRelativeTolerance
      }
    )
  }
}
