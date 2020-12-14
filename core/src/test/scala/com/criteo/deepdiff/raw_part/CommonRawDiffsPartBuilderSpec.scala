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

import com.criteo.deepdiff.{DifferentExample, LeftOnlyExample, RightOnlyExample}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class CommonRawDiffsPartBuilderSpec extends AnyFlatSpec with Matchers {
  it should "count correctly and generate at most N examples" in {
    var buildExampleCalledCount = 0
    val builder = new CommonRawDiffsPartBuilder[String, Int, String, Int](
      maxDiffExamples = 5,
      rawDiffExampleBuilder = new RawDiffExampleBuilder[String, Int, String, Int] {
        def asRawKeyExample(key: String): String = {
          buildExampleCalledCount += 1
          key
        }
        def asRawLeftExample(left: Int): Int = left * 2
        def asRawRightExample(right: Int): Int = right * 3
      }
    )

    val expectedIdentical = 29
    val expectedDifferent = 37
    val expectedLeftOnly = 53
    val expectedRightOnly = 11

    for (_ <- 0 until expectedIdentical) { builder.identical() }
    for (i <- 0 until expectedDifferent) { builder.different(s"different_$i", 1000 + i, 2000 + i) }
    for (i <- 0 until expectedLeftOnly) { builder.leftOnly(s"left_$i", 3000 + i) }
    for (i <- 0 until expectedRightOnly) { builder.rightOnly(s"right_$i", 4000 + i) }

    buildExampleCalledCount should be(15)

    builder.result() should be(
      CommonRawDiffsPart[String, Int](
        identical = expectedIdentical,
        different = RawDiffPart(
          expectedDifferent,
          List(
            "different_4" -> DifferentExample(2008, 6012),
            "different_3" -> DifferentExample(2006, 6009),
            "different_2" -> DifferentExample(2004, 6006),
            "different_1" -> DifferentExample(2002, 6003),
            "different_0" -> DifferentExample(2000, 6000)
          )
        ),
        leftOnly = RawDiffPart(
          expectedLeftOnly,
          List(
            "left_4" -> LeftOnlyExample(6008),
            "left_3" -> LeftOnlyExample(6006),
            "left_2" -> LeftOnlyExample(6004),
            "left_1" -> LeftOnlyExample(6002),
            "left_0" -> LeftOnlyExample(6000)
          )
        ),
        rightOnly = RawDiffPart(
          expectedRightOnly,
          List(
            "right_4" -> RightOnlyExample(12012),
            "right_3" -> RightOnlyExample(12009),
            "right_2" -> RightOnlyExample(12006),
            "right_1" -> RightOnlyExample(12003),
            "right_0" -> RightOnlyExample(12000)
          )
        )
      ))
  }

  it should "correctly increase the identical count" in {
    val builder = new CommonRawDiffsPartBuilder[String, Int, String, Int](
      maxDiffExamples = 5,
      rawDiffExampleBuilder = new RawDiffExampleBuilder[String, Int, String, Int] {
        def asRawKeyExample(key: String): String = key
        def asRawLeftExample(left: Int): Int = left
        def asRawRightExample(right: Int): Int = right
      }
    )
    builder.identical()
    builder.identical()
    builder.identical()

    builder.result(binaryIdenticalRecords = 89).identical should be(92)
  }
}
