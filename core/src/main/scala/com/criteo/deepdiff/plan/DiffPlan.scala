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

import com.criteo.deepdiff.plan.key.GroupKeyMaker
import com.criteo.deepdiff.type_support.example.StructExampleFactory
import com.criteo.deepdiff.utils.Common

/** The Plan describes what must actually be done by the Deep Diff. The same information is used
  * at different places, such as how to read/write DiffExample when comparing and when generating
  * human-friendly example.
  *
  * @param schema    Defines how to compare the records.
  * @param keyFields Keys used to identify identical records on the left and right to co-group them.
  */
private[deepdiff] final case class DiffPlan private[plan] (
    schema: DiffSchema[Common],
    keyFields: Seq[KeyDiffField[Common]]
) {
  val groupKey: GroupKeyMaker = new GroupKeyMaker(schema.raw, keyFields)
  val recordExample: Common[StructExampleFactory] = schema.recordExampleFactory
}
