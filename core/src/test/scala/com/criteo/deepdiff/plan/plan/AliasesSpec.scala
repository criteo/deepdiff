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

import com.criteo.deepdiff.config.{DeepDiffConfig, ExplodedArrayConfig, IgnoreConfig}
import com.criteo.deepdiff.plan.field.FieldPath
import com.criteo.deepdiff.plan.{KeyDiffField, _}
import com.criteo.deepdiff.test_utils.SparkUtils
import com.criteo.deepdiff.type_support.comparator.defaultEqualityParams
import com.criteo.deepdiff.utils.{Common, HasLeft, HasRight}
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class AliasesSpec extends AnyFlatSpec with Matchers {

  import FieldUtils._
  import SparkUtils._

  val left = struct(
    "intL" -> IntegerType,
    "left-only" -> StringType,
    "identical" -> BinaryType,
    "exploded_arrayL" -> ArrayType(
      struct(
        "L_a1" -> FloatType,
        "L_a2" -> DoubleType
      )),
    "struct" -> struct("L_s1" -> IntegerType, "L_s2" -> LongType)
  )
  val right = struct(
    "intR" -> IntegerType,
    "right-only" -> StringType,
    "identical" -> BinaryType,
    "exploded_arrayR" -> ArrayType(
      struct(
        "R_a1" -> FloatType,
        "R_a2" -> DoubleType
      )),
    "struct" -> struct("R_s1" -> IntegerType, "R_s2" -> LongType)
  )

  val config = new DeepDiffConfig(
    keys = Set("int"),
    ignore = IgnoreConfig(Set("string", "struct.s2"), leftOnly = false, rightOnly = false),
    leftAliases = Map(
      "intL" -> "int",
      "struct.L_s1" -> "s1",
      "struct.L_s2" -> "s2",
      "exploded_arrayL" -> "exploded_array",
      "exploded_arrayL.L_a1" -> "a1",
      "exploded_arrayL.L_a2" -> "a2",
      "left-only" -> "string",
      "identical" -> "LEFT"
    ).map({ case (k, v) => k -> v }),
    rightAliases = Map(
      "intR" -> "int",
      "struct.R_s1" -> "s1",
      "struct.R_s2" -> "s2",
      "exploded_arrayR" -> "exploded_array",
      "exploded_arrayR.R_a1" -> "a1",
      "exploded_arrayR.R_a2" -> "a2",
      "right-only" -> "string",
      "identical" -> "RIGHT"
    ).map({ case (k, v) => k -> v })
  )

  val expectedKeys = Seq(
    KeyDiffField(
      fullName = "int",
      relativeRawPath = Common(FieldPath("intL"), FieldPath("intR"))
    ))

  val expected = DiffSchema(
    common = Seq(
      AtomicDiffField(
        fullName = "int",
        raw = Common(field(left, fullName = "intL", alias = "int"), field(right, fullName = "intR", alias = "int")),
        leftRightEqualityParams = defaultEqualityParams
      ),
      ArrayStructDiffField(
        fullName = "exploded_array",
        raw = Common(field(left, fullName = "exploded_arrayL", alias = "exploded_array"),
                     field(right, fullName = "exploded_arrayR", alias = "exploded_array")),
        nestedStruct = PositionalNestedStructDiffField(
          fullName = "exploded_array (struct)",
          schema = DiffSchema(
            common = Seq(
              AtomicDiffField(
                fullName = "exploded_array.a1",
                raw = Common(field(left, fullName = "exploded_arrayL.L_a1", alias = "a1"),
                             field(right, fullName = "exploded_arrayR.R_a1", alias = "a1")),
                leftRightEqualityParams = defaultEqualityParams
              ),
              AtomicDiffField(
                fullName = "exploded_array.a2",
                raw = Common(field(left, fullName = "exploded_arrayL.L_a2", alias = "a2"),
                             field(right, fullName = "exploded_arrayR.R_a2", alias = "a2")),
                leftRightEqualityParams = defaultEqualityParams
              )
            ),
            left = Seq.empty,
            right = Seq.empty,
            raw = Common(
              struct("L_a1" -> FloatType, "L_a2" -> DoubleType),
              struct("R_a1" -> FloatType, "R_a2" -> DoubleType)
            )
          )
        )
      ),
      StructDiffField(
        fullName = "struct",
        raw = Common(field(left, "struct"), field(right, "struct")),
        schema = DiffSchema(
          common = Seq(
            AtomicDiffField(
              fullName = "struct.s1",
              raw = Common(field(left, fullName = "struct.L_s1", alias = "s1"),
                           field(right, fullName = "struct.R_s1", alias = "s1")),
              leftRightEqualityParams = defaultEqualityParams
            )
          ),
          left = Seq.empty,
          right = Seq.empty,
          raw = Common(
            struct("L_s1" -> IntegerType, "L_s2" -> LongType),
            struct("R_s1" -> IntegerType, "R_s2" -> LongType)
          )
        )
      )
    ),
    left = Seq(
      AtomicDiffField(
        fullName = "LEFT",
        raw = HasLeft(field(left, fullName = "identical", alias = "LEFT")),
        leftRightEqualityParams = defaultEqualityParams
      )
    ),
    right = Seq(
      AtomicDiffField(
        fullName = "RIGHT",
        raw = HasRight(field(right, fullName = "identical", alias = "RIGHT")),
        leftRightEqualityParams = defaultEqualityParams
      )
    ),
    raw = Common(left, right)
  )

  it should "uses aliases" in {
    new DiffPlanBuilder(config).build(left, right) match {
      case DiffPlan(schema, diffKeys) =>
        diffKeys should contain theSameElementsAs expectedKeys
        schema should be(expected)
        schema.pruned should be(
          Common(
            struct(
              "intL" -> IntegerType,
              "exploded_arrayL" -> ArrayType(
                struct(
                  "L_a1" -> FloatType,
                  "L_a2" -> DoubleType
                )),
              "struct" -> struct("L_s1" -> IntegerType),
              "identical" -> BinaryType
            ),
            struct(
              "intR" -> IntegerType,
              "exploded_arrayR" -> ArrayType(
                struct(
                  "R_a1" -> FloatType,
                  "R_a2" -> DoubleType
                )),
              "struct" -> struct("R_s1" -> IntegerType),
              "identical" -> BinaryType
            )
          ))
        schema.aliasedPruned should be(
          Common(
            struct(
              "int" -> IntegerType,
              "exploded_array" -> ArrayType(
                struct(
                  "a1" -> FloatType,
                  "a2" -> DoubleType
                )),
              "struct" -> struct("s1" -> IntegerType),
              "LEFT" -> BinaryType
            ),
            struct(
              "int" -> IntegerType,
              "exploded_array" -> ArrayType(
                struct(
                  "a1" -> FloatType,
                  "a2" -> DoubleType
                )),
              "struct" -> struct("s1" -> IntegerType),
              "RIGHT" -> BinaryType
            )
          ))
    }
  }

  it should "uses aliases while ignoring left/right only fields" in {
    new DiffPlanBuilder(config.copy(ignore = config.ignore.copy(leftOnly = true, rightOnly = true)))
      .build(left, right) match {
      case DiffPlan(schema, diffKeys) =>
        diffKeys should contain theSameElementsAs expectedKeys
        schema should be(
          expected.copy(
            left = Seq.empty,
            right = Seq.empty
          ))
        schema.pruned should be(
          Common(
            struct(
              "intL" -> IntegerType,
              "exploded_arrayL" -> ArrayType(
                struct(
                  "L_a1" -> FloatType,
                  "L_a2" -> DoubleType
                )),
              "struct" -> struct("L_s1" -> IntegerType)
            ),
            struct(
              "intR" -> IntegerType,
              "exploded_arrayR" -> ArrayType(
                struct(
                  "R_a1" -> FloatType,
                  "R_a2" -> DoubleType
                )),
              "struct" -> struct("R_s1" -> IntegerType)
            )
          ))
        schema.aliasedPruned should be(
          Common.twin(
            struct(
              "int" -> IntegerType,
              "exploded_array" -> ArrayType(
                struct(
                  "a1" -> FloatType,
                  "a2" -> DoubleType
                )),
              "struct" -> struct("s1" -> IntegerType)
            )
          ))
    }
  }

  it should "uses aliases with exploded arrays" in {
    val left = struct(
      "string" -> StringType,
      "exploded_arrayL" -> ArrayType(
        struct(
          "keyL" -> StringType,
          "intL" -> IntegerType
        ))
    )
    val right = struct(
      "string" -> StringType,
      "exploded_arrayR" -> ArrayType(
        struct(
          "keyR" -> StringType,
          "intR" -> IntegerType
        ))
    )
    new DiffPlanBuilder(
      new DeepDiffConfig(
        keys = Set("string"),
        leftAliases = Map(
          "exploded_arrayL" -> "exploded_array",
          "exploded_arrayL.keyL" -> "key",
          "exploded_arrayL.intL" -> "int"
        ).map({ case (k, v) => k -> v }),
        rightAliases = Map(
          "exploded_arrayR" -> "exploded_array",
          "exploded_arrayR.keyR" -> "key",
          "exploded_arrayR.intR" -> "int"
        ).map({ case (k, v) => k -> v }),
        explodedArrays = Map("exploded_array" -> ExplodedArrayConfig(keys = Set("key")))
      )).build(left, right) match {
      case DiffPlan(schema, diffKeys) =>
        diffKeys should contain theSameElementsAs Seq(commonKey("string"))
        schema should be(
          DiffSchema(
            common = Seq(
              atomic(left, "string", right),
              ArrayStructDiffField(
                fullName = "exploded_array",
                raw = Common(field(left, fullName = "exploded_arrayL", alias = "exploded_array"),
                             field(right, fullName = "exploded_arrayR", alias = "exploded_array")),
                nestedStruct = ExplodedNestedStructDiffField(
                  fullName = "exploded_array",
                  schema = DiffSchema(
                    common = Seq(
                      AtomicDiffField(
                        fullName = "key",
                        raw = Common(field(left, fullName = "exploded_arrayL.keyL", alias = "key"),
                                     field(right, fullName = "exploded_arrayR.keyR", alias = "key")),
                        leftRightEqualityParams = defaultEqualityParams
                      ),
                      AtomicDiffField(
                        fullName = "int",
                        raw = Common(field(left, fullName = "exploded_arrayL.intL", alias = "int"),
                                     field(right, fullName = "exploded_arrayR.intR", alias = "int")),
                        leftRightEqualityParams = defaultEqualityParams
                      )
                    ),
                    left = Nil,
                    right = Nil,
                    raw = Common(
                      struct("keyL" -> StringType, "intL" -> IntegerType),
                      struct("keyR" -> StringType, "intR" -> IntegerType)
                    )
                  ),
                  keyFields = Seq(
                    KeyDiffField(
                      fullName = "key",
                      relativeRawPath = Common(FieldPath("keyL"), FieldPath("keyR"))
                    )),
                  omitMultipleMatchesIfAllIdentical = false
                )
              )
            ),
            left = Nil,
            right = Nil,
            raw = Common(left, right)
          ))
        schema.aliasedPruned should be(
          Common.twin(
            struct(
              "string" -> StringType,
              "exploded_array" -> ArrayType(
                struct(
                  "key" -> StringType,
                  "int" -> IntegerType
                ))
            )
          ))
    }
  }
}
