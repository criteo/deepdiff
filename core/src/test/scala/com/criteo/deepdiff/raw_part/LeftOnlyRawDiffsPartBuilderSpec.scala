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

import com.criteo.deepdiff.LeftOnlyExample
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class LeftOnlyRawDiffsPartBuilderSpec extends AnyFlatSpec with Matchers {
  it should "build LeftOnlyRawDiffsPart" in {
    var buildExampleCalledCount = 0
    val builder = new LeftOnlyRawDiffsPartBuilder[String, Int, String, Int](
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
    val expectedLeftOnly = 53

    for (_ <- 0 until expectedIdentical) { builder.nullValue() }
    for (i <- 0 until expectedLeftOnly) { builder.leftOnly(s"left_$i", 3000 + i) }

    buildExampleCalledCount should be(5)

    builder.result() should be(
      LeftOnlyRawDiffsPart[String, Int](
        nulls = expectedIdentical,
        leftOnly = RawDiffPart(
          expectedLeftOnly,
          List(
            "left_4" -> LeftOnlyExample(6008),
            "left_3" -> LeftOnlyExample(6006),
            "left_2" -> LeftOnlyExample(6004),
            "left_1" -> LeftOnlyExample(6002),
            "left_0" -> LeftOnlyExample(6000)
          )
        )
      ))
  }
}
