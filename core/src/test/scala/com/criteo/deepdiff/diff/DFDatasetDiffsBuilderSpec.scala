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

import com.criteo.deepdiff.config.DeepDiffConfig
import com.criteo.deepdiff.plan.DiffPlanBuilder
import com.criteo.deepdiff.{DatasetDiffs, KeyExample}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util

trait DFDatasetDiffsBuilderSpec extends AnyFlatSpec with Matchers {
  import com.criteo.deepdiff.test_utils.SparkUtils.asSparkRow
  protected def buildDiff(config: DeepDiffConfig,
                          leftSchema: StructType,
                          rightSchema: StructType,
                          rows: Seq[(InternalRow, InternalRow)]): DatasetDiffs[KeyExample, Map[String, Any]] = {
    val plan = new DiffPlanBuilder(config).build(leftSchema, rightSchema)
    val diffAccumulator = new DatasetDiffAccumulatorsBuilder(config).build(plan)
    val result = diffAccumulator.processCoGroupPartition(
      rows
        .map({
          case (left, right) =>
            val keyBuffer = plan.groupKey.buildLeft(left)
            val key = UTF8String.fromBytes(util.Arrays.copyOf(keyBuffer.array, keyBuffer.position))
            (key, Iterator.single(asSparkRow(key, left)), Iterator.single(asSparkRow(key, right)))
        })
        .iterator)

    new DFRawToFriendly(plan).generateDatasetDiffs(result.next())
  }
}
