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

package com.criteo.deepdiff.diff

import com.criteo.deepdiff.KeyExample
import com.criteo.deepdiff.config.{DeepDiffConfig, ExplodedArrayConfig}
import com.criteo.deepdiff.test_utils.{Checker, DatasetDiffsBuilder, EasyKeyExample, SparkUtils}
import org.apache.spark.sql.types._

final class ExplodedArrayDiffSpec extends DFDatasetDiffsBuilderSpec {
  import EasyKeyExample._
  import com.criteo.deepdiff.test_utils.SparkUtils._

  val config = DeepDiffConfig(
    keys = Set("string"),
    explodedArrays = Map(
      "array" -> ExplodedArrayConfig(keys = Set("key")),
      "array.nested_array" -> ExplodedArrayConfig(keys = Set("nested_key"))
    )
  )
  val schema = struct(
    "string" -> StringType,
    "array" -> ArrayType(
      struct(
        "key" -> StringType,
        "int" -> IntegerType,
        "double" -> DoubleType,
        "nested_array" -> ArrayType(
          struct("nested_key" -> StringType, "string" -> StringType)
        )
      )
    )
  )

  val (left, leftExample) = SparkUtils.asSparkRowAndExample(
    schema,
    (
      "a",
      Seq(
        ("b1", 1, 1d, Seq(("c10", "Hello"))),
        ("b2", 2, 2d, Seq(("c20", "Where"), ("c21", "am I?"))),
        ("b3", 3, 3d, Seq(("c30", "Am I"), ("c31", "a"), ("c32", "joke ?"))),
        ("b4", 4, 4d, Seq(("c40", "Yet"), ("c41", "another")))
      )
    )
  )

  it should "find no difference in identical exploded arrays" in {
    Checker.check(
      result = buildDiff(config, schema, schema, rows = Seq((left, left))),
      expected = DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
        .schema(schema, schema)
        .record(identical = 1)
        .common("string", identical = 1)
        .common("array", identical = 1)
        .explodedArray(
          "array",
          DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
            .record(identical = 4)
            .common("key", identical = 4)
            .common("int", identical = 4)
            .common("double", identical = 4)
            .common("nested_array", identical = 4)
        )
        .explodedArray(
          "array.nested_array",
          DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
            .record(identical = 8)
            .common("nested_key", identical = 8)
            .common("string", identical = 8)
        )
    )
  }

  it should "explode different arrays" in {
    val (right, rightExample) = SparkUtils.asSparkRowAndExample(
      schema,
      (
        "a",
        Seq(
          ("b1", 1, 1d, Seq(("c10", "different"))), // different
          ("b2", 2, 20d, Seq(("c20", "Where"), ("c21", "am I?"))), // different
          ("b3", 3, 3d, Seq(("c30", "Am I"), ("c31", "a"), ("c32", "joke ?"))),
          ("b4", 4, 4d, Seq(("c40", "Yet"), null))
        )
      )
    )

    Checker.check(
      result = buildDiff(config, schema, schema, rows = Seq((left, right))),
      expected = DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
        .schema(schema, schema)
        .kindOfDifferent(content = 1)
        .record(
          diffExamples = Seq((Key("string" -> "a"), leftExample, rightExample))
        )
        .common("string", identical = 1)
        .common(
          "array",
          diffExamples = Seq(
            (Key("string" -> "a"), leftExample("array"), rightExample("array"))
          )
        )
        .explodedArray(
          "array",
          DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
            .record(
              identical = 1,
              diffExamples = Seq(
                (
                  Key("string" -> "a") >> Key("b1"),
                  leftExample("array").asInstanceOf[Seq[Map[String, Any]]].head,
                  rightExample("array").asInstanceOf[Seq[Map[String, Any]]].head
                ),
                (
                  Key("string" -> "a") >> Key("b2"),
                  leftExample("array").asInstanceOf[Seq[Map[String, Any]]](1),
                  rightExample("array").asInstanceOf[Seq[Map[String, Any]]](1)
                ),
                (
                  Key("string" -> "a") >> Key("b4"),
                  leftExample("array").asInstanceOf[Seq[Map[String, Any]]](3),
                  rightExample("array").asInstanceOf[Seq[Map[String, Any]]](3)
                )
              )
            )
            .kindOfDifferent(content = 3)
            .common("key", identical = 4)
            .common("int", identical = 4)
            .common(
              "double",
              identical = 3,
              diffExamples = Seq((Key("string" -> "a") >> Key("b2"), 2d, 20d))
            )
            .common(
              "nested_array",
              identical = 2,
              diffExamples = Seq(
                (
                  Key("string" -> "a") >> Key("b1"),
                  Seq(Map("nested_key" -> "c10", "string" -> "Hello")),
                  Seq(Map("nested_key" -> "c10", "string" -> "different"))
                ),
                (
                  Key("string" -> "a") >> Key("b4"),
                  Seq(
                    Map("nested_key" -> "c40", "string" -> "Yet"),
                    Map("nested_key" -> "c41", "string" -> "another")
                  ),
                  Seq(Map("nested_key" -> "c40", "string" -> "Yet"), null)
                )
              )
            )
        )
        .explodedArray(
          "array.nested_array",
          DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
            .record(
              identical = 6,
              diffExamples = Seq(
                (
                  Key("string" -> "a") >> Key("b1") >> Key(
                    "nested_key" -> "c10"
                  ),
                  Map("nested_key" -> "c10", "string" -> "Hello"),
                  Map("nested_key" -> "c10", "string" -> "different")
                )
              ),
              leftExamples = Seq(
                (
                  Key("string" -> "a") >> Key("b4") >> Key(
                    "nested_key" -> "c41"
                  ),
                  Map("nested_key" -> "c41", "string" -> "another")
                )
              )
            )
            .kindOfDifferent(content = 1)
            .common("nested_key", identical = 7)
            .common(
              "string",
              identical = 6,
              diffExamples = Seq(
                (
                  Key("string" -> "a") >> Key("b1") >> Key(
                    "nested_key" -> "c10"
                  ),
                  "Hello",
                  "different"
                )
              )
            )
        )
    )
  }
}
