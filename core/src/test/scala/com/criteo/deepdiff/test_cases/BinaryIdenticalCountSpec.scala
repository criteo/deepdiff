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

package com.criteo.deepdiff.test_cases

import com.criteo.deepdiff.KeyExample
import com.criteo.deepdiff.test_utils.{DatasetDiffsBuilder, DeepDiffSpec}

object BinaryIdenticalCountSpec {
  final case class Part(key: Int, field: String = null)
  final case class KeyOnly(key: Int)
  final case class RecordNested(key: Int, field: String = null, part: Part = null)
  final case class RecordArray(key: Int, field: String = null, parts: Seq[Part] = null)
}

/**
  * As records are initially compared directly through their binary representation, the identical
  * count must still be correct even though we do not actually read the fields themselves.
  */
final class BinaryIdenticalCountSpec extends DeepDiffSpec {
  import BinaryIdenticalCountSpec._
  import DeepDiffSpec._
  val haveCorrectIdenticalCountsIf = afterWord("have correct identical counts for a")

  "Deep Diff" should {
    "generate exact identical counts" when {
      "comparing nested structs" which {
        "have the same schema" when runningDeepDiffTestCase {
          val data = Seq(
            RecordNested(key = 1, field = "1"),
            RecordNested(key = 2, field = "2", Part(key = 10, field = "10"))
          )
          DeepDiffTestCase
            .defaultConf()
            .compare(left = data, right = data.reverse)
            .expectRaw(
              DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
                .record(identical = 2)
                .common("key", identical = 2)
                .common("field", identical = 2)
                .common("part", identical = 2)
                .common("part.key", identical = 1)
                .common("part.field", identical = 1)
            )
        }
        "have a different schema" when runningDeepDiffTestCase {
          DeepDiffTestCase
            .testReversedLeftAndRight()
            .testPruned()
            .defaultConf()
            .compare(left = Seq(RecordNested(1), RecordNested(2)), right = Seq(KeyOnly(1), KeyOnly(2)))
            .expectRaw(
              DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
                .record(identical = 2)
                .common("key", identical = 2)
                .leftOnly("field", identical = 2)
                .leftOnly("part", identical = 2)
                .leftOnly("part.key")
                .leftOnly("part.field"))
        }
      }
      "comparing nested arrays" which {
        val commonData = Seq(
          RecordArray(key = 1, field = "1"),
          RecordArray(key = 2, field = "2", Nil),
          RecordArray(key = 3, field = "3", Seq(null)),
          RecordArray(key = 4, field = "4", Seq(Part(key = 10, field = "10"))),
          RecordArray(key = 5, field = "5", Seq(Part(key = 20, field = "20"), Part(key = 30, field = "30")))
        )
        val sameSchemaBuilder = DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
          .record(identical = 5)
          .common("key", identical = 5)
          .common("field", identical = 5)
          .common("parts", identical = 5)

        val raw = Seq(RecordArray(1), RecordArray(2))
        val pruned = Seq(KeyOnly(1), KeyOnly(2))
        val oneSideOnlyBuilder = DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
          .record(identical = 2)
          .common("key", identical = 2)
          .leftOnly("field", identical = 2)
          .leftOnly("parts", identical = 2)

        "are NOT exploded" which {
          "have the same schema" when runningDeepDiffTestCase {
            DeepDiffTestCase
              .defaultConf()
              .compare(left = commonData, right = commonData)
              .expectRaw(
                sameSchemaBuilder
                  .common("parts (struct)", identical = 4)
                  .common("parts.key", identical = 3)
                  .common("parts.field", identical = 3))
          }
          "have a different schema" when runningDeepDiffTestCase {
            DeepDiffTestCase
              .testReversedLeftAndRight()
              .testPruned()
              .defaultConf()
              .compare(left = raw, right = pruned)
              .expectRaw(
                oneSideOnlyBuilder
                  .leftOnly("parts (struct)")
                  .leftOnly("parts.key")
                  .leftOnly("parts.field"))
          }
        }

        "are exploded" which {
          val explodedConf = // language=HOCON
            """keys = ["key"]
              |exploded-arrays {
              |  parts = { keys = ["key"]}
              |}
              |""".stripMargin

          "have the same schema" when runningDeepDiffTestCase {
            DeepDiffTestCase
              .conf(explodedConf)
              .compare(left = commonData, right = commonData)
              .expectRaw(
                sameSchemaBuilder
                  // when exploding, nulls are not counted as they don't have a key.
                  .explodedArray(
                    "parts",
                    DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
                      .record(identical = 3)
                      .common("key", identical = 3)
                      .common("field", identical = 3)
                  )
              )
          }
          "have a different schema" when runningDeepDiffTestCase {
            DeepDiffTestCase
              .testReversedLeftAndRight()
              .testPruned()
              .conf(explodedConf)
              .compare(left = raw, right = pruned)
              .expectRaw(
                oneSideOnlyBuilder.explodedArray(
                  "parts",
                  DatasetDiffsBuilder.leftOnlyRecordDiffs[KeyExample, Map[String, Any]](
                    fields = Set("key", "field")
                  )
                ))
          }
        }
      }
    }
  }
}
