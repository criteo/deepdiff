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

import com.criteo.deepdiff.config.DeepDiffConfig
import com.criteo.deepdiff.plan.field._
import com.criteo.deepdiff.type_support.comparator.EqualityParams
import com.criteo.deepdiff.utils._
import org.apache.spark.sql.types.StructType

private[deepdiff] final class DiffPlanBuilder(config: DeepDiffConfig) {
  val fieldDiffFactory = new DiffFieldsFactory(config)

  def build(left: StructType, right: StructType): DiffPlan = {
    val ignoredKeys = config.ignore.fields.intersect(config.keys)
    if (ignoredKeys.nonEmpty) {
      throw new IllegalArgumentException(s"Following keys have been ignored: ${ignoredKeys.mkString(", ")}")
    }

    fieldDiffFactory
      .buildDiffFields(
        rootField = RootField,
        keys = config.keys.map(FieldPath(_)),
        rawSchema = Common(left, right),
        defaultEqualityParams = EqualityParams.fromConf(Some(config.defaultTolerance), default = EqualityParams.strict)
      )
      // If no fields are common, just fail. It is not supported.
      .matchLeftRight(
        common = { (keys: Seq[KeyDiffField[Common]], diffSchema: DiffSchema[Common]) =>
          DiffPlan(diffSchema, keys)
        }
      )
  }
}
