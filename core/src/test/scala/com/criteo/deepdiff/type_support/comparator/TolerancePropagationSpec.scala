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

object TolerancePropagationSpec {
  final case class PropagationTest[+T](dataType: DataType,
                                       left: T,
                                       right: T,
                                       equal: Boolean,
                                       relativeTolerance: Option[Double] = None,
                                       absoluteTolerance: Option[Double] = None)
}

final class TolerancePropagationSpec extends AnyFlatSpec with Matchers {
  import TolerancePropagationSpec._

  val dummyStruct = StructType(Seq(StructField("string", StringType), StructField("int", IntegerType)))
  // tolerances are propagated
  val propagationTestCases: Seq[PropagationTest[Any]] = Seq(
    // no tolerances equality
    PropagationTest[Seq[Int]](ArrayType(IntegerType), Seq(100, 200), Seq(100, 200), equal = true),
    PropagationTest[Map[String, Int]](MapType(StringType, IntegerType), Map("a" -> 100), Map("a" -> 100), equal = true),
    PropagationTest[(String, Int)](dummyStruct, ("a", 100), ("a", 100), equal = true),
    // no tolerances inequality
    PropagationTest[Seq[Int]](ArrayType(IntegerType), Seq(100, 200), Seq(110, 220), equal = false),
    PropagationTest[Map[String, Int]](MapType(StringType, IntegerType),
                                      Map("a" -> 100),
                                      Map("a" -> 110),
                                      equal = false),
    PropagationTest[(String, Int)](dummyStruct, ("a", 100), ("a", 110), equal = false),
    // absolute tolerance
    PropagationTest[Seq[Int]](ArrayType(IntegerType),
                              Seq(100, 200),
                              Seq(110, 220),
                              equal = true,
                              absoluteTolerance = Some(20)),
    PropagationTest[Map[String, Int]](MapType(StringType, IntegerType),
                                      Map("a" -> 100),
                                      Map("a" -> 110),
                                      equal = true,
                                      absoluteTolerance = Some(10)),
    PropagationTest[(String, Int)](dummyStruct, ("a", 100), ("a", 110), equal = true, absoluteTolerance = Some(10)),
    // relative tolerance
    PropagationTest[Seq[Int]](ArrayType(IntegerType),
                              Seq(100, 200),
                              Seq(110, 220),
                              equal = true,
                              relativeTolerance = Some(.1)),
    PropagationTest[Map[String, Int]](MapType(StringType, IntegerType),
                                      Map("a" -> 100),
                                      Map("a" -> 110),
                                      equal = true,
                                      relativeTolerance = Some(.1)),
    PropagationTest[(String, Int)](dummyStruct, ("a", 100), ("a", 110), equal = true, relativeTolerance = Some(.1)),
    // absolute AND relative tolerance
    PropagationTest[Seq[Int]](ArrayType(IntegerType),
                              Seq(100, 200),
                              Seq(110, 220),
                              equal = true,
                              absoluteTolerance = Some(20),
                              relativeTolerance = Some(.1)),
    PropagationTest[Map[String, Int]](MapType(StringType, IntegerType),
                                      Map("a" -> 100),
                                      Map("a" -> 110),
                                      equal = true,
                                      absoluteTolerance = Some(10),
                                      relativeTolerance = Some(.1)),
    PropagationTest[(String, Int)](dummyStruct,
                                   ("a", 100),
                                   ("a", 110),
                                   equal = true,
                                   absoluteTolerance = Some(10),
                                   relativeTolerance = Some(.1))
  )

  it should "propagate tolerances" in {
    for (test <- propagationTestCases) {
      withClue(test.toString) {
        val comparator =
          Comparator(test.dataType,
                     EqualityParams(test.absoluteTolerance,
                                    test.relativeTolerance,
                                    mustSatisfyAbsoluteAndRelativeTolerance = true))
        val left = SparkUtils.asSparkRow(("placeholder", test.left))
        val leftOrdinal = 1
        val right = SparkUtils.asSparkRow((test.right, "placeholder"))
        val rightOrdinal = 0
        comparator.isFieldEqual(left, leftOrdinal, right, rightOrdinal) should be(test.equal)
        comparator.isFieldEqual(right, rightOrdinal, left, leftOrdinal) should be(test.equal)
      }
    }
  }
}
