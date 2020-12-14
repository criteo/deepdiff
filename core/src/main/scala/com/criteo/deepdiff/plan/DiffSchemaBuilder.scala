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

package com.criteo.deepdiff.plan

import com.criteo.deepdiff.utils._
import org.apache.spark.sql.types.StructType

private[plan] final class DiffSchemaBuilder(rawSchema: LeftRight[StructType]) {
  private var commonFields: List[DiffSchemaField[Common]] = Nil
  private var leftFields: List[DiffSchemaField[HasLeft]] = Nil
  private var rightFields: List[DiffSchemaField[HasRight]] = Nil

  def addCommon(field: DiffSchemaField[Common]): Unit = {
    commonFields ::= field
  }

  def addLeft(field: DiffSchemaField[HasLeft]): Unit = {
    leftFields ::= field
  }

  def addRight(field: DiffSchemaField[HasRight]): Unit = {
    rightFields ::= field
  }

  // Imitates a pattern matching on the type parameter.
  def matchLeftRight[T](
      common: DiffSchema[Common] => T = matchError.common _,
      left: DiffSchema[HasLeft] => T = matchError.leftOnly _,
      right: DiffSchema[HasRight] => T = matchError.rightOnly _,
      empty: () => T = matchError.noFields _
  ): T = {
    // keep original ordering as they have been added in the order in which
    // the pruned schema should be.
    val c = commonFields.reverse
    val l = leftFields.reverse
    val r = rightFields.reverse

    if (c.nonEmpty || (l.nonEmpty && r.nonEmpty)) {
      common(
        DiffSchema[Common](
          common = c,
          left = l,
          right = r,
          raw = rawSchema match { case c: Common[StructType] => c }
        )
      )
    } else if (l.nonEmpty) {
      left(
        DiffSchema[HasLeft](
          common = c,
          left = l,
          right = r,
          raw = rawSchema match { case l: HasLeft[StructType] => l }
        )
      )
    } else if (r.nonEmpty) {
      right(
        DiffSchema[HasRight](
          common = c,
          left = l,
          right = r,
          raw = rawSchema match { case r: HasRight[StructType] => r }
        )
      )
    } else {
      empty()
    }
  }
}
