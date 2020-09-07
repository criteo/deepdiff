/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/* Modifications copyright 2020 Criteo
 * Adapted from org.apache.spark.sql.KeyValueGroupedDataset (Spark 2.4.6)
 */

package org.apache.spark.sql.deepdiff

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.{Dataset, Encoder}
import org.apache.spark.unsafe.types.UTF8String

final class UnsafeKeyRecordGroupedDataset private[sql] (
    @transient val queryExecution: QueryExecution,
    private val keyAttr: Attribute
) extends Serializable {

  private def logicalPlan = queryExecution.analyzed
  private def sparkSession = queryExecution.sparkSession

  def mapCoGroupPartitions[R: Encoder](
      other: UnsafeKeyRecordGroupedDataset,
      numPartitions: Int
  )(f: Iterator[(UTF8String, Iterator[InternalRow], Iterator[InternalRow])] => Iterator[R]): Dataset[R] = {
    Dataset[R](
      sparkSession,
      UnsafeMapCoGroupPartitions[R](
        f,
        this.keyAttr,
        other.keyAttr,
        this.logicalPlan,
        other.logicalPlan,
        numPartitions
      )
    )
  }
}
