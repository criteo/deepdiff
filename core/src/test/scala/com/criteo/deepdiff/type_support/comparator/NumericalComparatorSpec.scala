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

import com.criteo.deepdiff.test_utils.SparkUtils
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object NumericalComparatorSpec {
  final case class NumericalEqualityTest(left: Short,
                                         right: Short,
                                         equal: Boolean,
                                         relativeTolerance: Option[Double] = None,
                                         absoluteTolerance: Option[Double] = None,
                                         absoluteAndRelative: Boolean = true)
}

final class NumericalComparatorSpec extends AnyFlatSpec with Matchers {
  import NumericalComparatorSpec._

  val dataTypeAndCast = Seq[(DataType, Short => Any)](
    (ShortType, identity),
    (IntegerType, _.toInt),
    (LongType, _.toLong),
    (FloatType, _.toFloat),
    (DoubleType, _.toDouble)
  )
  val commonTestCases: Seq[NumericalEqualityTest] = Seq(
    // no tolerance
    NumericalEqualityTest(100, 100, equal = true),
    NumericalEqualityTest(100, 101, equal = false),
    // absolute tolerance
    NumericalEqualityTest(100, 101, equal = true, absoluteTolerance = Some(1)),
    NumericalEqualityTest(100, 101, equal = false, absoluteTolerance = Some(.5)),
    // relative tolerance
    NumericalEqualityTest(100, 101, equal = true, relativeTolerance = Some(.1)),
    NumericalEqualityTest(100, 120, equal = false, relativeTolerance = Some(.1)),
    // absolute AND relative tolerance
    NumericalEqualityTest(100, 101, equal = true, relativeTolerance = Some(.1), absoluteTolerance = Some(1)),
    NumericalEqualityTest(100, 102, equal = false, relativeTolerance = Some(10), absoluteTolerance = Some(1)),
    NumericalEqualityTest(100, 120, equal = false, relativeTolerance = Some(.1), absoluteTolerance = Some(100)),
    NumericalEqualityTest(100, 120, equal = false, relativeTolerance = Some(.1), absoluteTolerance = Some(.5)),
    // absolute OR relative tolerance
    NumericalEqualityTest(100,
                          101,
                          equal = true,
                          relativeTolerance = Some(.1),
                          absoluteTolerance = Some(1),
                          absoluteAndRelative = false),
    NumericalEqualityTest(100,
                          102,
                          equal = true,
                          relativeTolerance = Some(10),
                          absoluteTolerance = Some(1),
                          absoluteAndRelative = false),
    NumericalEqualityTest(100,
                          120,
                          equal = true,
                          relativeTolerance = Some(.1),
                          absoluteTolerance = Some(100),
                          absoluteAndRelative = false),
    NumericalEqualityTest(100,
                          120,
                          equal = false,
                          relativeTolerance = Some(.1),
                          absoluteTolerance = Some(.5),
                          absoluteAndRelative = false),
    // proper zero handling
    NumericalEqualityTest(0, 0, equal = true, relativeTolerance = Some(1), absoluteTolerance = Some(1)),
    NumericalEqualityTest(0, 1, equal = true, relativeTolerance = Some(1), absoluteTolerance = Some(1)),
    NumericalEqualityTest(1, 0, equal = true, relativeTolerance = Some(1), absoluteTolerance = Some(1)),
    NumericalEqualityTest(0, 1, equal = false, relativeTolerance = Some(.1), absoluteTolerance = Some(1)),
    NumericalEqualityTest(1, 0, equal = false, relativeTolerance = Some(1), absoluteTolerance = Some(.5)),
    NumericalEqualityTest(1, 0, equal = false, relativeTolerance = Some(.1), absoluteTolerance = Some(.5))
  )

  it should "support absolute & relative tolerance" in {
    checkTestCases(
      Seq(
        // no tolerance
        NumericalEqualityTest(100, 100, equal = true),
        NumericalEqualityTest(100, 101, equal = false),
        // absolute tolerance
        NumericalEqualityTest(100, 101, equal = true, absoluteTolerance = Some(1)),
        NumericalEqualityTest(100, 101, equal = false, absoluteTolerance = Some(.5)),
        // relative tolerance
        NumericalEqualityTest(100, 101, equal = true, relativeTolerance = Some(.1)),
        NumericalEqualityTest(100, 120, equal = false, relativeTolerance = Some(.1)),
        // absolute AND relative tolerance
        NumericalEqualityTest(100, 101, equal = true, relativeTolerance = Some(.1), absoluteTolerance = Some(1)),
        NumericalEqualityTest(100, 102, equal = false, relativeTolerance = Some(10), absoluteTolerance = Some(1)),
        NumericalEqualityTest(100, 120, equal = false, relativeTolerance = Some(.1), absoluteTolerance = Some(100)),
        NumericalEqualityTest(100, 120, equal = false, relativeTolerance = Some(.1), absoluteTolerance = Some(.5)),
        // absolute OR relative tolerance
        NumericalEqualityTest(100,
                              101,
                              equal = true,
                              relativeTolerance = Some(.1),
                              absoluteTolerance = Some(1),
                              absoluteAndRelative = false),
        NumericalEqualityTest(100,
                              102,
                              equal = true,
                              relativeTolerance = Some(10),
                              absoluteTolerance = Some(1),
                              absoluteAndRelative = false),
        NumericalEqualityTest(100,
                              120,
                              equal = true,
                              relativeTolerance = Some(.1),
                              absoluteTolerance = Some(100),
                              absoluteAndRelative = false),
        NumericalEqualityTest(100,
                              120,
                              equal = false,
                              relativeTolerance = Some(.1),
                              absoluteTolerance = Some(.5),
                              absoluteAndRelative = false)
      ))
  }

  it should "handle correctly zero" in {
    checkTestCases(
      Seq(
        // absolute
        NumericalEqualityTest(1, 0, equal = true, absoluteTolerance = Some(1)),
        NumericalEqualityTest(1, 0, equal = false, absoluteTolerance = Some(.5)),
        // relative
        NumericalEqualityTest(1, 0, equal = false, relativeTolerance = Some(1)),
        // absolute AND relative
        NumericalEqualityTest(1, 0, equal = false, relativeTolerance = Some(1), absoluteTolerance = Some(1)),
        // absolute OR relative
        NumericalEqualityTest(1,
                              0,
                              equal = true,
                              relativeTolerance = Some(1),
                              absoluteTolerance = Some(1),
                              absoluteAndRelative = false),
        NumericalEqualityTest(1,
                              0,
                              equal = false,
                              relativeTolerance = Some(1),
                              absoluteTolerance = Some(.5),
                              absoluteAndRelative = false)
      ))

    checkTestCases(
      Seq(
        NumericalEqualityTest(0, 0, equal = true),
        NumericalEqualityTest(0, 0, equal = true, absoluteTolerance = Some(1)),
        NumericalEqualityTest(0, 0, equal = true, relativeTolerance = Some(1)),
        NumericalEqualityTest(0, 0, equal = true, absoluteTolerance = Some(1), relativeTolerance = Some(1)),
        NumericalEqualityTest(0,
                              0,
                              equal = true,
                              absoluteTolerance = Some(1),
                              relativeTolerance = Some(1),
                              absoluteAndRelative = false)
      ))
  }

  private def checkTestCases(tests: Seq[NumericalEqualityTest]): Unit = {
    for (
      test <- tests.flatMap(t => Seq(t, t.copy(left = t.right, right = t.left)));
      (dataType, cast) <- dataTypeAndCast
    ) {
      withClue(s"${test.toString} [${dataType.simpleString}]") {
        val comparator =
          Comparator(dataType, EqualityParams(test.absoluteTolerance, test.relativeTolerance, test.absoluteAndRelative))

        val left = SparkUtils.asSparkRow(("placeholder", cast(test.left)))
        val leftOrdinal = 1
        val right = SparkUtils.asSparkRow((cast(test.right), "placeholder"))
        val rightOrdinal = 0
        comparator.isFieldEqual(left, leftOrdinal, right, rightOrdinal) should be(test.equal)
        comparator.isFieldEqual(right, rightOrdinal, left, leftOrdinal) should be(test.equal)
      }
    }
  }
}
