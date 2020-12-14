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

package com.criteo.deepdiff

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Optimized Deep Diff, but supplied schema MUST be respected.
  *
  * It removes all ignored fields from the dataset, so less data is read. Furthermore it ensures that left and right
  * schemas are as close as possible. Whenever those are identical, DeepDiff will compare the binary representation
  * of rows directly which can speed up comparison noticeably.
  */
abstract class FastDeepDiff(implicit spark: SparkSession) extends DeepDiff {

  protected def leftSchema: StructType
  protected def rightSchema: StructType
  protected def leftDataFrame(schema: StructType): DataFrame
  protected def rightDataFrame(schema: StructType): DataFrame

  protected final def leftAndRightDataFrames(): (DataFrame, DataFrame) = {
    val prunedSchema = generateDiffPlan(leftSchema, rightSchema).schema.pruned

    val prunedLeft = leftDataFrame(prunedSchema.left)
    val prunedRight = rightDataFrame(prunedSchema.right)
    // Sanity checks, schema should only includes the same fields with the same dataType
    assertEquivalentSchemas(prunedLeft.schema, prunedSchema.left)
    assertEquivalentSchemas(prunedRight.schema, prunedSchema.right)
    // if both pruned schema were equal, the same is expected from the actual schemas.
    if (prunedSchema.left == prunedSchema.right && prunedLeft.schema != prunedRight.schema) {
      throw new AssertionError(
        s"""Pruned schemas were expected to be equal, but left
           |${prunedRight.schema.treeString}
           |did not match
           |${prunedSchema.right.treeString}""".stripMargin
      )
    }

    (prunedLeft, prunedRight)
  }

  private def assertEquivalentSchemas(actual: StructType, expected: StructType): Unit = {
    val expectedFields = expected.fields.map({ f => f.name -> f }).toMap
    actual.fields.foreach { a =>
      expectedFields.get(a.name) match {
        case Some(e) =>
          // It is not expected for nested structs / array of structs to have different field ordering.
          // The only case I'm aware of are partition columns which are always placed at the end.
          assert(a.dataType == e.dataType,
                 s"${a.name} does not the same type, expected ${e.dataType} found ${a.dataType}")
        case None =>
          throw new AssertionError(s"Unexpected field ${a.name}")
      }
    }
    assert(expectedFields.keySet == actual.fieldNames.toSet,
           s"Missing fields ${expectedFields.keySet.diff(actual.fieldNames.toSet)}")
  }
}
