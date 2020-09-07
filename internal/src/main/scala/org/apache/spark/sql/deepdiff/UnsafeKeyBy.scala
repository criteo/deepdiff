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

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}

final case class UnsafeKeyBy(
    keyExtractor: SpecializedGetters => ByteBuffer,
    child: LogicalPlan,
    keyAttr: Attribute,
    recordAttr: Attribute
) extends UnaryNode {
  override def output: Seq[Attribute] = Seq(keyAttr, recordAttr)

  override def producedAttributes: AttributeSet = AttributeSet(output)
}

object UnsafeKeyBy {
  def apply(keyExtractor: SpecializedGetters => ByteBuffer, child: LogicalPlan): UnsafeKeyBy = {
    val keyExpr = Alias(encoderFor(Encoders.STRING).namedExpressions.head.toAttribute, "key")()
    val recordExpr = Alias(CreateStruct(child.output), "record")()
    UnsafeKeyBy(
      keyExtractor,
      child,
      keyExpr.toAttribute,
      recordExpr.toAttribute
    )
  }
}
