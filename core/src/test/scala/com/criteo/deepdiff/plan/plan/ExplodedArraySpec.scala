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

import com.criteo.deepdiff.config.{DeepDiffConfig, ExplodedArrayConfig, MultipleMatchesConfig}
import com.criteo.deepdiff.plan.{DiffPlan, DiffPlanBuilder, FieldUtils, _}
import com.criteo.deepdiff.test_utils.SparkUtils
import com.criteo.deepdiff.utils.Common
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class ExplodedArraySpec extends AnyFlatSpec with Matchers {

  import FieldUtils._
  import SparkUtils._

  it should "support exploded arrays" in {
    val left = struct(
      "string" -> StringType,
      "exploded_array" -> ArrayType(
        struct(
          "key" -> StringType,
          "int" -> IntegerType,
          "double" -> DoubleType
        ))
    )
    val right = left
    for (omitMultipleMatchesIfAllIdentical <- Seq(true, false)) {
      withClue(s"Omit identical multiple matches ${omitMultipleMatchesIfAllIdentical}") {
        new DiffPlanBuilder(
          DeepDiffConfig(
            keys = Set("string"),
            explodedArrays = Map(
              "exploded_array" -> ExplodedArrayConfig(
                keys = Set("key"),
                MultipleMatchesConfig(omitIfAllIdentical = omitMultipleMatchesIfAllIdentical)
              )
            )
          ))
          .build(left, right) match {
          case DiffPlan(schema, diffKeys) =>
            diffKeys should contain theSameElementsAs Seq(commonKey("string"))
            schema should be(
              DiffSchema(
                common = Seq(
                  atomic(left, "string", right),
                  ArrayStructDiffField(
                    fullName = "exploded_array",
                    raw = Common(field(left, "exploded_array"), field(right, "exploded_array")),
                    nestedStruct = ExplodedNestedStructDiffField(
                      fullName = "exploded_array",
                      schema = DiffSchema(
                        common = {
                          val l = explodedStruct(left, "exploded_array")
                          val r = explodedStruct(right, "exploded_array")
                          Seq(
                            atomic(l, "key", r),
                            atomic(l, "int", r),
                            atomic(l, "double", r)
                          )
                        },
                        left = Nil,
                        right = Nil,
                        raw = Common(
                          struct("key" -> StringType, "int" -> IntegerType, "double" -> DoubleType),
                          struct("key" -> StringType, "int" -> IntegerType, "double" -> DoubleType)
                        )
                      ),
                      keyFields = Seq(commonKey("key")),
                      omitMultipleMatchesIfAllIdentical = omitMultipleMatchesIfAllIdentical
                    )
                  )
                ),
                left = Nil,
                right = Nil,
                raw = Common(left, right)
              ))
        }
      }
    }
  }
}
