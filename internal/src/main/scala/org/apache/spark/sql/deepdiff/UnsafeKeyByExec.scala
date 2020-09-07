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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, SpecializedGetters, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.unsafe.types.UTF8String

final case class UnsafeKeyByExec(
    keyExtractor: SpecializedGetters => ByteBuffer,
    override val output: Seq[Attribute],
    child: SparkPlan
) extends UnaryExecNode {
  override def producedAttributes: AttributeSet = AttributeSet(output)
  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val unsafeWriter = new UnsafeRowWriter(2, 10 * 1024)

      // Spark seems to converts systematically everything to UnsafeRow as soon as possible.
      // So we shouldn't find any GenericInternalRow here.
      iter.asInstanceOf[Iterator[UnsafeRow]].map { row =>
        unsafeWriter.reset()
        val buffer = keyExtractor(row)
        unsafeWriter
          .write(0, UTF8String.fromBytes(buffer.array(), 0, buffer.position()))
        unsafeWriter.write(1, row)
        unsafeWriter.getRow
      }
    }
  }
}
