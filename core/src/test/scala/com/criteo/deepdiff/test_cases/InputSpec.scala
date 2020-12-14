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
import com.criteo.deepdiff.test_utils.DeepDiffSpec.DeepDiffTestCase
import com.criteo.deepdiff.test_utils.{DatasetDiffsBuilder, DeepDiffSpec}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.UUID

object InputSpec {
  final case class RecordA(key: String, value: java.lang.Integer = null)
  final case class RecordB(key: String, value: java.lang.Integer = null, count: java.lang.Integer = null)
}

/** Parquet is the primary target for the DeepDiff, but the internals shouldn't rely on it. */
final class InputSpec extends DeepDiffSpec {
  import com.criteo.deepdiff.test_utils.EasyKeyExample._

  "DeepDiff" should {
    val expected = DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
      .kindOfDifferent(content = 1, leftOnly = 1, rightOnly = 2)
      .record(
        identical = 1,
        diffExamples = Seq(
          (
            Key("different"),
            Map("key" -> "different", "value" -> 2),
            Map("key" -> "different", "value" -> 20, "count" -> null)
          ),
          (
            Key("left-only"),
            Map("key" -> "left-only", "value" -> 3),
            Map("key" -> "left-only", "value" -> null, "count" -> null)
          ),
          (
            Key("right-only"),
            Map("key" -> "right-only", "value" -> null),
            Map("key" -> "right-only", "value" -> 4, "count" -> null)
          ),
          (
            Key("new-field"),
            Map("key" -> "new-field", "value" -> null),
            Map("key" -> "new-field", "value" -> null, "count" -> 7)
          )
        ),
        leftExamples = Seq(
          (Key("left-record"), Map("key" -> "left-record", "value" -> null))
        ),
        rightExamples = Seq(
          (Key("right-record"), Map("key" -> "right-record", "value" -> null, "count" -> null))
        )
      )
      .common("key", identical = 5)
      .common(
        "value",
        identical = 2,
        diffExamples = Seq((Key("different"), 2, 20)),
        leftExamples = Seq((Key("left-only"), 3)),
        rightExamples = Seq((Key("right-only"), 4))
      )
      .rightOnly("count", identical = 4, rightExamples = Seq((Key("new-field"), 7)))

    "support json" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .defaultConf()
        .testReversedLeftAndRight()
        .compare(
          left = jsonDataFrame( // language=JSON
            """{"key": "identical", "value": 1}
              |{"key": "different", "value": 2}
              |{"key": "left-only", "value": 3}
              |{"key": "right-only"}
              |{"key": "left-record"}
              |{"key": "new-field"}
              |""".stripMargin),
          right = jsonDataFrame( // language=JSON
            """{"key": "identical", "value": 1}
              |{"key": "different", "value": 20}
              |{"key": "left-only"}
              |{"key": "right-only", "value": 4}
              |{"key": "right-record"}
              |{"key": "new-field", "count": 7}
              |""".stripMargin)
        )
        .expectRaw(expected)
    }

    "support CSV" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .defaultConf()
        .testReversedLeftAndRight()
        .compare(
          left = csvDataFrame( // language=CSV
            """key,value
              |identical,1
              |different,2
              |left-only,3
              |right-only,
              |left-record,
              |new-field
              |""".stripMargin),
          right = csvDataFrame( // language=CSV
            """key,value,count
              |identical,1,
              |different,20,
              |left-only,,
              |right-only,4,
              |right-record,,
              |new-field,,7
              |""".stripMargin)
        )
        .expectRaw(expected)
    }
  }

  private def jsonDataFrame(json: String): SparkSession => DataFrame =
    (spark: SparkSession) => {
      withTmpDir { tmpDir =>
        val path = tmpDir.resolve(UUID.randomUUID().toString)
        Files.write(path, json.getBytes(StandardCharsets.UTF_8))
        val df = spark.read.json(path.normalize().toAbsolutePath.toString)
        df.cache().count() // force loading
        df
      }
    }

  private def csvDataFrame(csv: String): SparkSession => DataFrame =
    (spark: SparkSession) => {
      withTmpDir { tmpDir =>
        val path = tmpDir.resolve(UUID.randomUUID().toString)
        Files.write(path, csv.getBytes(StandardCharsets.UTF_8))
        val df = spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(path.normalize().toAbsolutePath.toString)
        df.cache().count() // force loading
        df
      }
    }
}
