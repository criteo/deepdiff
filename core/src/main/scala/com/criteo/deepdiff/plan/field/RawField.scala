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

package com.criteo.deepdiff.plan.field

import org.apache.spark.sql.types.{DataType, StructField}

private[plan] final case class RawField private[field] (ordinal: Int,
                                                        structField: StructField,
                                                        maybeAlias: Option[String] = None) {
  def aliasedName: String = maybeAlias.getOrElse(name)
  def dataType: DataType = structField.dataType
  def name: String = structField.name
}

private[plan] object RawField {
  def apply(name: String, ordinal: Int, dataType: DataType): RawField =
    new RawField(ordinal, StructField(name, dataType))
}
