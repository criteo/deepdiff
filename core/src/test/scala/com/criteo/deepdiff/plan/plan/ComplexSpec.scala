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

final class ComplexSpec extends AnyFlatSpec with Matchers {

  import FieldUtils._
  import SparkUtils._

  val left = SparkUtils.dummyLeftSchema
  val right = struct(
    "map" -> MapType(StringType, IntegerType), // 0
    "float" -> FloatType, // 1
    "long" -> StringType, // 2 - different type
    "new-field" -> DoubleType, // 3 - new field
    "int2" -> IntegerType, // 4 - different name
    "array" -> ArrayType(IntegerType), // 5
    "exploded_array" -> ArrayType(
      struct(
        "a2" -> IntegerType, // different type
        "a3" -> StringType, // new field
        "a1" -> FloatType
      )),
    "double" -> DoubleType, // 7
    "struct" -> struct(
      // missing s1
      "s2" -> StringType,
      "s3" -> IntegerType // different type
    )
  )

  it should "merge complex schemas" in {
    new DiffPlanBuilder(DeepDiffConfig(keys = Set("float")))
      .build(left, right) match {
      case DiffPlan(schema, diffKeys) =>
        diffKeys should contain theSameElementsAs Seq(commonKey("float"))
        schema should be(
          DiffSchema(
            common = Seq(
              atomic(left, "float", right),
              atomic(left, "double", right),
              atomic(left, "array", right),
              atomic(left, "map", right),
              ArrayStructDiffField(
                fullName = "exploded_array",
                raw = Common(field(left, "exploded_array"), field(right, "exploded_array")),
                nestedStruct = PositionalNestedStructDiffField(
                  fullName = "exploded_array (struct)",
                  schema = DiffSchema(
                    common = Seq(atomic(left, "exploded_array.a1", right)),
                    left = Seq(atomic(left, "exploded_array.a2").copy(fullName = "exploded_array.a2 (string)")),
                    right = Seq(
                      atomic("exploded_array.a2", right).copy(fullName = "exploded_array.a2 (int)"),
                      atomic("exploded_array.a3", right)
                    ),
                    raw = Common(
                      struct("a1" -> FloatType, "a2" -> StringType),
                      struct("a2" -> IntegerType, "a3" -> StringType, "a1" -> FloatType)
                    )
                  )
                )
              ),
              StructDiffField(
                fullName = "struct",
                raw = Common(field(left, "struct"), field(right, "struct")),
                schema = {
                  DiffSchema(
                    common = Seq(atomic(left, "struct.s2", right)),
                    left = Seq(
                      atomic(left, "struct.s1"),
                      atomic(left, "struct.s3").copy(fullName = "struct.s3 (double)")
                    ),
                    right = Seq(
                      atomic("struct.s3", right).copy(fullName = "struct.s3 (int)")
                    ),
                    raw = Common(
                      left("struct").dataType.asInstanceOf[StructType],
                      right("struct").dataType.asInstanceOf[StructType]
                    )
                  )
                }
              )
            ),
            left = Seq(
              atomic(left, "int"),
              atomic(left, "long").copy(fullName = "long (bigint)"),
              atomic(left, "string"),
              atomic(left, "boolean")
            ),
            right = Seq(
              atomic("long", right).copy(fullName = "long (string)"),
              atomic("new-field", right),
              atomic("int2", right)
            ),
            raw = Common(left, right)
          ))
        schema.pruned should be(
          Common(
            StructType(
              Seq(
                // Common
                left("float"),
                left("double"),
                left("array"),
                left("map"),
                left("exploded_array"),
                StructField("struct", struct("s2" -> StringType, "s1" -> IntegerType, "s3" -> DoubleType)),
                // left-only in left-ordering
                left("int"),
                left("long"),
                left("string"),
                left("boolean")
              )),
            StructType(
              Seq(
                // common
                right("float"),
                right("double"),
                right("array"),
                right("map"),
                StructField("exploded_array",
                            ArrayType(struct("a1" -> FloatType, "a2" -> IntegerType, "a3" -> StringType))),
                right("struct"),
                // common but different type in left-ordering
                right("long"),
                // right-only
                right("new-field"),
                right("int2")
              ))
          ))
        schema.aliasedPruned should be(schema.pruned)
    }
  }

  it should "merge complex schemas while ignoring left/right only fields" in {
    new DiffPlanBuilder(
      DeepDiffConfig(keys = Set("float"), ignore = IgnoreConfig(Set.empty, leftOnly = true, rightOnly = true)))
      .build(left, right) match {
      case DiffPlan(schema, diffKeys) =>
        diffKeys should contain theSameElementsAs Seq(commonKey("float"))
        schema should be(
          DiffSchema(
            common = Seq(
              atomic(left, "float", right),
              atomic(left, "double", right),
              atomic(left, "array", right),
              atomic(left, "map", right),
              ArrayStructDiffField(
                fullName = "exploded_array",
                raw = Common(field(left, "exploded_array"), field(right, "exploded_array")),
                nestedStruct = PositionalNestedStructDiffField(
                  fullName = "exploded_array (struct)",
                  schema = DiffSchema(
                    common = Seq(atomic(left, "exploded_array.a1", right)),
                    left = Seq(atomic(left, "exploded_array.a2").copy(fullName = "exploded_array.a2 (string)")),
                    right = Seq(atomic("exploded_array.a2", right).copy(fullName = "exploded_array.a2 (int)")),
                    raw = Common(
                      struct("a1" -> FloatType, "a2" -> StringType),
                      struct("a2" -> IntegerType, "a3" -> StringType, "a1" -> FloatType)
                    )
                  )
                )
              ),
              StructDiffField(
                fullName = "struct",
                raw = Common(field(left, "struct"), field(right, "struct")),
                schema = {
                  DiffSchema(
                    common = Seq(atomic(left, "struct.s2", right)),
                    left = Seq(atomic(left, "struct.s3").copy(fullName = "struct.s3 (double)")),
                    right = Seq(atomic("struct.s3", right).copy(fullName = "struct.s3 (int)")),
                    raw = Common(
                      struct("s1" -> IntegerType, "s2" -> StringType, "s3" -> DoubleType),
                      struct("s2" -> StringType, "s3" -> IntegerType)
                    )
                  )
                }
              )
            ),
            left = Seq(atomic(left, "long").copy(fullName = "long (bigint)")),
            right = Seq(atomic("long", right).copy(fullName = "long (string)")),
            raw = Common(left, right)
          ))
        schema.pruned should be(
          Common(
            StructType(Seq(
              left("float"),
              left("double"),
              left("array"),
              left("map"),
              StructField("exploded_array", ArrayType(struct("a1" -> FloatType, "a2" -> StringType))),
              StructField("struct", struct("s2" -> StringType, "s3" -> DoubleType)),
              left("long")
            )),
            StructType(Seq(
              right("float"),
              right("double"),
              right("array"),
              right("map"),
              StructField("exploded_array", ArrayType(struct("a1" -> FloatType, "a2" -> IntegerType))),
              StructField("struct", struct("s2" -> StringType, "s3" -> IntegerType)),
              right("long")
            ))
          ))
        schema.aliasedPruned should be(schema.pruned)
    }
  }
}
