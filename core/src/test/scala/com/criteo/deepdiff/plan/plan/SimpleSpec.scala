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

package com.criteo.deepdiff.plan.plan

import com.criteo.deepdiff.config.{DeepDiffConfig, IgnoreConfig}
import com.criteo.deepdiff.plan._
import com.criteo.deepdiff.test_utils.SparkUtils
import com.criteo.deepdiff.utils.Common
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class SimpleSpec extends AnyFlatSpec with Matchers {
  import FieldUtils._
  import SparkUtils._

  it should "not change identical schemas" in {
    val diffPlanBuilder = new DiffPlanBuilder(DeepDiffConfig(keys = Set("string")))
    for (
      rawSchema <- Seq(
        struct("string" -> StringType, "b" -> IntegerType),
        SparkUtils.dummyLeftSchema,
        struct(
          "string" -> StringType,
          "a" -> ArrayType(
            struct("1" -> IntegerType, "2" -> struct("2b" -> DoubleType), "3" -> ArrayType(struct("3b" -> FloatType)))))
      )
    ) {
      diffPlanBuilder.build(rawSchema, rawSchema) match {
        case DiffPlan(schema, diffKeys) =>
          diffKeys should contain theSameElementsAs Seq(commonKey("string"))
          schema should be(
            DiffSchema(
              common = schema.common,
              left = Seq.empty,
              right = Seq.empty,
              raw = Common(rawSchema, rawSchema)
            ))
          schema.pruned should be(Common(rawSchema, rawSchema))
          schema.aliasedPruned should be(schema.pruned)
      }
    }
  }

  it should "support a different ordering of the fields" in {
    val left = struct(
      "a" -> StringType,
      "b" -> IntegerType,
      "c" -> MapType(StringType, FloatType),
      "d" -> DoubleType
    )
    val right = struct(
      "c" -> MapType(StringType, FloatType),
      "a" -> StringType,
      "d" -> DoubleType,
      "b" -> IntegerType
    )
    new DiffPlanBuilder(DeepDiffConfig(keys = Set("a"))).build(left, right) match {
      case DiffPlan(schema, diffKeys) =>
        diffKeys should contain theSameElementsAs Seq(commonKey("a"))
        schema should be(
          DiffSchema(
            common = Seq(
              atomic(left, "a", right),
              atomic(left, "b", right),
              atomic(left, "c", right),
              atomic(left, "d", right)
            ),
            left = Seq.empty,
            right = Seq.empty,
            raw = Common(left, right)
          ))
    }
  }

  it should "detect nested schema with left or right only fields" in {
    a[Throwable] should be thrownBy {
      new DiffPlanBuilder(
        DeepDiffConfig(keys = Set("float"), ignore = IgnoreConfig(Set("int"), leftOnly = false, rightOnly = false)))
        .build(SparkUtils.dummyLeftSchema, struct("int" -> IntegerType))
    }

    a[Throwable] should be thrownBy {
      new DiffPlanBuilder(
        DeepDiffConfig(keys = Set("float"), ignore = IgnoreConfig(Set("int"), leftOnly = false, rightOnly = false)))
        .build(struct("int" -> IntegerType), SparkUtils.dummyLeftSchema)
    }

    a[Throwable] should be thrownBy {
      new DiffPlanBuilder(
        DeepDiffConfig(keys = Set("float"),
                       ignore = IgnoreConfig(Set("int", "long"), leftOnly = false, rightOnly = false)))
        .build(struct("int" -> IntegerType), struct("long" -> LongType))
    }
  }
}
