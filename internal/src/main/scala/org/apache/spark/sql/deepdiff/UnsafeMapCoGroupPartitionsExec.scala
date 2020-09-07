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
 * Adapted from org.apache.spark.sql.execution.CoGroupExec (Spark 2.4.6)
 */

package org.apache.spark.sql.deepdiff

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, SortOrder, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution}
import org.apache.spark.sql.execution._
import org.apache.spark.unsafe.types.UTF8String

final case class UnsafeMapCoGroupPartitionsExec(
    func: Iterator[(UTF8String, Iterator[InternalRow], Iterator[InternalRow])] => Iterator[Any],
    leftKey: Attribute,
    rightKey: Attribute,
    outputObjAttr: Attribute,
    left: SparkPlan,
    right: SparkPlan,
    numPartitions: Int
) extends BinaryExecNode
    with ObjectProducerExec {

  override def requiredChildDistribution: Seq[Distribution] =
    Seq(ClusteredDistribution(leftKey :: Nil, Some(numPartitions)),
        ClusteredDistribution(rightKey :: Nil, Some(numPartitions)))

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(SortOrder(leftKey, Ascending)) :: Seq(SortOrder(rightKey, Ascending)) :: Nil

  override protected def doExecute(): RDD[InternalRow] = {
    left.execute().zipPartitions(right.execute()) { (leftData, rightData) =>
      val leftGrouped =
        StringGroupedIterator(leftData.asInstanceOf[Iterator[UnsafeRow]], leftKey, left.output)
      val rightGrouped =
        StringGroupedIterator(rightData.asInstanceOf[Iterator[UnsafeRow]], rightKey, right.output)
      val outputObject = ObjectOperator.wrapObjectToRow(outputObjAttr.dataType)

      func(new StringCoGroupIterator(leftGrouped, rightGrouped)).map(outputObject)
    }
  }
}
