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

package org.apache.spark.sql.deepdiff

import java.nio.ByteBuffer

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.{Dataset, Encoder}

final class UnsafeDataset[T](val dataset: Dataset[T]) {
  private implicit val tExprEnc: Encoder[T] = dataset.exprEnc

  def groupWithKeyBuilder(key: SpecializedGetters => ByteBuffer): UnsafeKeyRecordGroupedDataset = {
    val withGroupingKey = UnsafeKeyBy(key, dataset.logicalPlan)
    val executed = dataset.sparkSession.sessionState.executePlan(withGroupingKey)
    new UnsafeKeyRecordGroupedDataset(
      executed,
      withGroupingKey.keyAttr
    )
  }
}
