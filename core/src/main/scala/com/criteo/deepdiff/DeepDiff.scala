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

import com.criteo.deepdiff.config.DeepDiffConfig
import com.criteo.deepdiff.diff.{DFRawToFriendly, DatasetDiffAccumulatorsBuilder, Kx, Rx}
import com.criteo.deepdiff.plan._
import com.criteo.deepdiff.raw_part.{DatasetRawDiffsPart, RawDiffPartMerger}
import org.apache.spark.sql.deepdiff.{UnsafeDataset, UnsafeStrategies}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}

/** Base class implementing Deep Diff.
  *
  * Prefer using [[FastDeepDiff]] for best performance, but this one has the most flexibility.
  */
abstract class DeepDiff(implicit val spark: SparkSession) {

  protected val config: DeepDiffConfig
  protected def leftAndRightDataFrames(): (DataFrame, DataFrame)

  final def run(): DatasetDiffs[KeyExample, Map[String, Any]] = {
    spark.experimental.extraStrategies = UnsafeStrategies :: spark.experimental.extraStrategies.toList

    val (left, right) = leftAndRightDataFrames()
    val numPartitions = coGroupNumPartitions(left, right)

    val diffPlan = generateDiffPlan(left.schema, right.schema)
    val diffAccumulator = new DatasetDiffAccumulatorsBuilder(config).build(diffPlan)
    val rawToFriendly = new DFRawToFriendly(diffPlan)
    val diffPartMerger = new RawDiffPartMerger(config.maxExamples)
    implicit val datasetDiffsPartEncoder: Encoder[DatasetRawDiffsPart[Kx, Rx]] =
      Encoders.kryo[DatasetRawDiffsPart[Kx, Rx]]

    val keyedLeft = new UnsafeDataset(left).groupWithKeyBuilder(diffPlan.groupKey.buildLeft)
    val keyedRight = new UnsafeDataset(right).groupWithKeyBuilder(diffPlan.groupKey.buildRight)
    val rawDatasetDiffsPart = keyedLeft
      .mapCoGroupPartitions(keyedRight, numPartitions)(diffAccumulator.processCoGroupPartition)
      .rdd
      // treeAggregate becomes necessary with huge number of partitions where the driver
      // cannot retrieve all the examples.
      .treeAggregate(DatasetRawDiffsPart.empty[Kx, Rx])(diffPartMerger.merge,
                                                        diffPartMerger.merge,
                                                        depth = treeDepth(numPartitions, config.maxExamples))
    rawToFriendly.generateDatasetDiffs(rawDatasetDiffsPart)
  }

  private[deepdiff] final def generateDiffPlan(leftSchema: StructType, rightSchema: StructType): DiffPlan =
    new DiffPlanBuilder(config).build(leftSchema, rightSchema)

  private def coGroupNumPartitions(left: DataFrame, right: DataFrame): Int = {
    Math.max(
      spark.conf
        // Unless specified explicitly, we're using the same number of partition as left and right combined
        // as it's pretty good heuristic to even handle somewhat skewed keys.
        .get(SQLConf.SHUFFLE_PARTITIONS.key, (left.rdd.getNumPartitions + right.rdd.getNumPartitions).toString)
        .toInt,
      20
    )
  }

  private def treeDepth(numPartitions: Int, maxExamples: Int): Int = {
    import Math._
    val minDepth = 2
    if (maxExamples > 0) {
      // By default -driver accepts max result size of 1GB, so we're limiting the total number of example
      // it might receive in total.
      val maxExamplesOnSingleNode = 1000
      val maxPartitionsToAggregateAtOnce = max(maxExamplesOnSingleNode / maxExamples, 2)
      max(ceil(log(numPartitions) / log(maxPartitionsToAggregateAtOnce)), minDepth).toInt
    } else {
      minDepth
    }
  }
}
