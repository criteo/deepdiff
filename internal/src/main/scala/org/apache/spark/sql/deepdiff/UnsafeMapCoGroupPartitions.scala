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
 * Adapted from org.apache.spark.sql.catalyst.plans.logical.CoGroup (Spark 2.4.6)
 */

package org.apache.spark.sql.deepdiff

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, CatalystSerde, LogicalPlan, ObjectProducer}
import org.apache.spark.unsafe.types.UTF8String

final case class UnsafeMapCoGroupPartitions(
    func: Iterator[(UTF8String, Iterator[InternalRow], Iterator[InternalRow])] => Iterator[Any],
    leftKey: Attribute,
    rightKey: Attribute,
    outputObjAttr: Attribute,
    left: LogicalPlan,
    right: LogicalPlan,
    numPartitions: Int
) extends BinaryNode
    with ObjectProducer

object UnsafeMapCoGroupPartitions {
  def apply[OUT: Encoder](
      func: Iterator[(UTF8String, Iterator[InternalRow], Iterator[InternalRow])] => Iterator[OUT],
      leftKey: Attribute,
      rightKey: Attribute,
      left: LogicalPlan,
      right: LogicalPlan,
      numPartitions: Int
  ): LogicalPlan = {
    val coGrouped = UnsafeMapCoGroupPartitions(
      func,
      leftKey,
      rightKey,
      CatalystSerde.generateObjAttr[OUT],
      left,
      right,
      numPartitions: Int
    )
    CatalystSerde.serialize[OUT](coGrouped)
  }
}
