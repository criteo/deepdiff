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

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

object UnsafeStrategies extends Strategy with Serializable {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] =
    plan match {
      case UnsafeMapCoGroupPartitions(f, lKey, rKey, outputAttr, left, right, numPartitions) =>
        UnsafeMapCoGroupPartitionsExec(f,
                                       lKey,
                                       rKey,
                                       outputAttr,
                                       planLater(left),
                                       planLater(right),
                                       numPartitions) :: Nil
      case l @ UnsafeKeyBy(keyExtractor, child, _, _) =>
        UnsafeKeyByExec(keyExtractor, l.output, planLater(child)) :: Nil
      case _ => Nil
    }
}
