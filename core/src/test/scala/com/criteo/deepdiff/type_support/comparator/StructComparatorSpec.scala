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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class StructComparatorSpec extends AnyFlatSpec with Matchers {
  import SparkUtils._

  private val left = LeftRow(
    1,
    2L,
    3f,
    4d,
    "5",
    true,
    Seq[java.lang.Integer](100, 101, null),
    Map[String, java.lang.Integer]("hello" -> 200, "bye" -> null),
    Seq((1.2f, "first"), (1.3f, "second")),
    (47, "nop", null)
  )
  private val right = RightRow(
    1,
    2L,
    3f,
    4d,
    "5",
    true,
    Seq[java.lang.Integer](100, 101, null),
    Map[String, java.lang.Integer]("hello" -> 200, "bye" -> null),
    Seq((1.2f, "first"), (1.3f, "second")),
    (47, "nop", null)
  )

  it should "be able to compare structs" in {
    val comparator = Comparator(dummyLeftSchema, defaultEqualityParams)

    for (
      (a, b, isEqual) <- Seq(
        (left, left, true),
        (left.copy(int = 10), left, false),
        (left.copy(long = 20L), left, false),
        (left.copy(float = 30f), left, false),
        (left.copy(double = 40d), left, false),
        (left.copy(string = "50"), left, false),
        (left.copy(boolean = false), left, false),
        (left.copy(array = Seq.empty), left, false),
        (left.copy(array = Seq(1, 11)), left, false),
        (left.copy(map = Map.empty), left, false),
        (left.copy(exploded_array = Seq((2f, "diff"))), left, false),
        (left.copy(struct = (10, "nop", null)), left, false),
        (left, left.copy(int = 10), false),
        (left, left.copy(long = 20L), false),
        (left, left.copy(float = 30f), false),
        (left, left.copy(double = 40d), false),
        (left, left.copy(string = "50"), false),
        (left, left.copy(boolean = false), false),
        (left, left.copy(array = Seq.empty), false),
        (left, left.copy(array = Seq(1, 11)), false),
        (left, left.copy(map = Map.empty), false),
        (left, left.copy(exploded_array = Seq((2f, "diff"))), false),
        (left, left.copy(struct = (10, "nop", null)), false),
        (left.copy(int = null), left, false),
        (left.copy(long = null), left, false),
        (left.copy(float = null), left, false),
        (left.copy(double = null), left, false),
        (left.copy(string = null), left, false),
        (left.copy(boolean = null), left, false),
        (left.copy(array = null), left, false),
        (left.copy(map = null), left, false),
        (left.copy(exploded_array = null), left, false),
        (left.copy(struct = null), left, false),
        (left, left.copy(int = null), false),
        (left, left.copy(long = null), false),
        (left, left.copy(float = null), false),
        (left, left.copy(double = null), false),
        (left, left.copy(string = null), false),
        (left, left.copy(boolean = null), false),
        (left, left.copy(array = null), false),
        (left, left.copy(map = null), false),
        (left, left.copy(exploded_array = null), false),
        (left, left.copy(struct = null), false)
      )
    ) {
      withClue(s"$left ${if (isEqual) "==" else "!="} $right") {
        val x = asSparkRow(("placeholder", a))
        val y = asSparkRow((b, "placeholder"))
        comparator.isFieldEqual(x, 1, y, 0) should be(isEqual)
        comparator.isFieldEqual(y, 0, x, 1) should be(isEqual)
      }
    }
  }
}
