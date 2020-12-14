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

final class IgnoreSpec extends AnyFlatSpec with Matchers {

  import FieldUtils._
  import SparkUtils._

  it should "ignore fields" in {
    val left = StructType(SparkUtils.dummyLeftSchema.fields :+ StructField("left-only", IntegerType))
    val right = StructType(SparkUtils.dummyLeftSchema.fields :+ StructField("right-only", IntegerType))
    new DiffPlanBuilder(
      DeepDiffConfig(keys = Set("string"),
                     ignore = IgnoreConfig(Set("float", "array", "exploded_array", "struct", "left-only", "right-only"),
                                           leftOnly = false,
                                           rightOnly = false)))
      .build(left, right) match {
      case DiffPlan(schema, diffKeys) =>
        diffKeys should contain theSameElementsAs Seq(commonKey("string"))
        schema should be(
          DiffSchema(
            common = Seq(
              atomic(left, "int", right),
              atomic(left, "long", right),
              atomic(left, "double", right),
              atomic(left, "string", right),
              atomic(left, "boolean", right),
              atomic(left, "map", right)
            ),
            left = Seq.empty,
            right = Seq.empty,
            raw = Common(left, right)
          ))
        schema.pruned should be(
          Common(
            struct(
              "int" -> IntegerType,
              "long" -> LongType,
              "double" -> DoubleType,
              "string" -> StringType,
              "boolean" -> BooleanType,
              "map" -> MapType(StringType, IntegerType)
            ),
            struct(
              "int" -> IntegerType,
              "long" -> LongType,
              "double" -> DoubleType,
              "string" -> StringType,
              "boolean" -> BooleanType,
              "map" -> MapType(StringType, IntegerType)
            )
          ))
        schema.aliasedPruned should be(schema.pruned)
    }
  }

  it should "ignore nested fields" in {
    val left = StructType(SparkUtils.dummyLeftSchema.fields)
    val right = StructType(SparkUtils.dummyLeftSchema.fields)
    new DiffPlanBuilder(
      DeepDiffConfig(keys = Set("string"),
                     ignore = IgnoreConfig(Set("exploded_array.a1", "struct.s2"), leftOnly = false, rightOnly = false)))
      .build(left, right) match {
      case DiffPlan(schema, diffKeys) =>
        diffKeys should contain theSameElementsAs Seq(commonKey("string"))
        schema should be(
          DiffSchema(
            common = Seq(
              atomic(left, "int", right),
              atomic(left, "long", right),
              atomic(left, "float", right),
              atomic(left, "double", right),
              atomic(left, "string", right),
              atomic(left, "boolean", right),
              atomic(left, "array", right),
              atomic(left, "map", right),
              ArrayStructDiffField(
                fullName = "exploded_array",
                raw = Common(field(left, "exploded_array"), field(right, "exploded_array")),
                nestedStruct = PositionalNestedStructDiffField(
                  fullName = "exploded_array (struct)",
                  schema = DiffSchema(
                    common = Seq(
                      atomic(left, "exploded_array.a2", right)
                    ),
                    left = Seq.empty,
                    right = Seq.empty,
                    raw = Common(
                      struct("a1" -> FloatType, "a2" -> StringType),
                      struct("a1" -> FloatType, "a2" -> StringType)
                    )
                  )
                )
              ),
              StructDiffField(
                fullName = "struct",
                raw = Common(field(left, "struct"), field(right, "struct")),
                schema = DiffSchema(
                  common = Seq(
                    atomic(left, "struct.s1", right),
                    atomic(left, "struct.s3", right)
                  ),
                  left = Seq.empty,
                  right = Seq.empty,
                  raw = Common(
                    struct("s1" -> IntegerType, "s2" -> StringType, "s3" -> DoubleType),
                    struct("s1" -> IntegerType, "s2" -> StringType, "s3" -> DoubleType)
                  )
                )
              )
            ),
            left = Seq.empty,
            right = Seq.empty,
            raw = Common(left, right)
          ))
        schema.pruned should be(
          Common(
            struct(
              "int" -> IntegerType,
              "long" -> LongType,
              "float" -> FloatType,
              "double" -> DoubleType,
              "string" -> StringType,
              "boolean" -> BooleanType,
              "array" -> ArrayType(IntegerType),
              "map" -> MapType(StringType, IntegerType),
              "exploded_array" -> ArrayType(struct("a2" -> StringType)),
              "struct" -> struct("s1" -> IntegerType, "s3" -> DoubleType)
            ),
            struct(
              "int" -> IntegerType,
              "long" -> LongType,
              "float" -> FloatType,
              "double" -> DoubleType,
              "string" -> StringType,
              "boolean" -> BooleanType,
              "array" -> ArrayType(IntegerType),
              "map" -> MapType(StringType, IntegerType),
              "exploded_array" -> ArrayType(struct("a2" -> StringType)),
              "struct" -> struct("s1" -> IntegerType, "s3" -> DoubleType)
            )
          ))
        schema.aliasedPruned should be(schema.pruned)
    }
  }

  it should "ignore struct (array) fields when all their nested fields are ignored" in {
    val left = SparkUtils.dummyLeftSchema
    val right = left
    new DiffPlanBuilder(
      DeepDiffConfig(
        keys = Set("string"),
        ignore = IgnoreConfig(Set("exploded_array.a1", "exploded_array.a2", "struct.s1", "struct.s2", "struct.s3"),
                              leftOnly = false,
                              rightOnly = false)
      ))
      .build(left, right) match {
      case DiffPlan(schema, diffKeys) =>
        diffKeys should contain theSameElementsAs Seq(commonKey("string"))
        schema should be(
          DiffSchema(
            common = Seq(
              atomic(left, "int", right),
              atomic(left, "long", right),
              atomic(left, "float", right),
              atomic(left, "double", right),
              atomic(left, "string", right),
              atomic(left, "boolean", right),
              atomic(left, "array", right),
              atomic(left, "map", right)
            ),
            left = Seq.empty,
            right = Seq.empty,
            raw = Common(left, right)
          ))
        schema.pruned should be(
          Common(
            struct(
              "int" -> IntegerType,
              "long" -> LongType,
              "float" -> FloatType,
              "double" -> DoubleType,
              "string" -> StringType,
              "boolean" -> BooleanType,
              "array" -> ArrayType(IntegerType),
              "map" -> MapType(StringType, IntegerType)
            ),
            struct(
              "int" -> IntegerType,
              "long" -> LongType,
              "float" -> FloatType,
              "double" -> DoubleType,
              "string" -> StringType,
              "boolean" -> BooleanType,
              "array" -> ArrayType(IntegerType),
              "map" -> MapType(StringType, IntegerType)
            )
          ))
        schema.aliasedPruned should be(schema.pruned)
    }
  }
}
