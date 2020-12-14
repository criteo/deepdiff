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

import com.criteo.deepdiff.{LeftOnlyExample, RightOnlyExample}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class RecordRawDiffsPartBuilderSpec extends AnyFlatSpec with Matchers {
  final class DummyExampleBuilder extends RawDiffExampleBuilder[String, Map[String, Int], String, Map[String, Int]] {
    var calls: Int = 0
    @inline protected def asRawKeyExample(key: String): String = {
      calls += 1
      key * 2
    }
    @inline protected def asRawLeftExample(left: Map[String, Int]): Map[String, Int] = {
      calls += 1
      left.mapValues(_ * 2)
    }
    @inline protected def asRawRightExample(right: Map[String, Int]): Map[String, Int] = {
      calls += 1
      right.mapValues(_ + 2)
    }
  }

  final class KeyGenerator {
    var calls: Int = 0
    private val counter = Iterator.from(0)
    def create(): String = {
      calls += 1
      counter.next().toString
    }
  }

  "LeftOnlyRecordDiffsPartBuilder" should "count correctly and generate at most N examples" in {
    val dummyExampleBuilder = new DummyExampleBuilder()
    val keyGenerator = new KeyGenerator()
    val builder = new LeftOnlyRecordDiffsPartBuilder(dummyExampleBuilder, Set("field"), maxDiffExamples = 5)

    for (i <- 0 until 21) { builder.leftOnly(keyGenerator.create(), Map("x" -> i)) should be(KindOfDiff.LEFT_ONLY) }

    // Everything is expected to be lazy.
    dummyExampleBuilder.calls should be(10)
    keyGenerator.calls should be(5)

    builder.result should be(
      LeftOnlyRecordRawDiffsPart(
        records = RawDiffPart(
          21,
          examples = List(
            "44" -> LeftOnlyExample(Map("x" -> 8)),
            "33" -> LeftOnlyExample(Map("x" -> 6)),
            "22" -> LeftOnlyExample(Map("x" -> 4)),
            "11" -> LeftOnlyExample(Map("x" -> 2)),
            "00" -> LeftOnlyExample(Map("x" -> 0))
          )
        ),
        fields = Set("field")
      ))
  }

  "RightOnlyRecordDiffsPartBuilder" should "count correctly and generate at most N examples" in {
    val dummyExampleBuilder = new DummyExampleBuilder()
    val keyGenerator = new KeyGenerator()
    val builder = new RightOnlyRecordDiffsPartBuilder(dummyExampleBuilder, Set("field"), maxDiffExamples = 5)

    for (i <- 0 until 21) { builder.rightOnly(keyGenerator.create(), Map("x" -> i)) should be(KindOfDiff.RIGHT_ONLY) }

    // Everything is expected to be lazy.
    dummyExampleBuilder.calls should be(10)
    keyGenerator.calls should be(5)

    builder.result should be(
      RightOnlyRecordRawDiffsPart(
        records = RawDiffPart(
          21,
          examples = List(
            "44" -> RightOnlyExample(Map("x" -> 6)),
            "33" -> RightOnlyExample(Map("x" -> 5)),
            "22" -> RightOnlyExample(Map("x" -> 4)),
            "11" -> RightOnlyExample(Map("x" -> 3)),
            "00" -> RightOnlyExample(Map("x" -> 2))
          )
        ),
        fields = Set("field")
      ))
  }
}
