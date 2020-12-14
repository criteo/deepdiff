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

package com.criteo.deepdiff.diff

import com.criteo.deepdiff.plan.DiffSchemaField
import com.criteo.deepdiff.raw_part.{
  CommonRawDiffsPartBuilder,
  LeftOnlyRawDiffsPartBuilder,
  RightOnlyRawDiffsPartBuilder
}
import com.criteo.deepdiff.type_support.example.{ExampleFactory, StructExampleFactory}
import com.criteo.deepdiff.utils.{Common, HasLeft, HasRight}

private[diff] object LeftRightRawDiffsPartBuilders {
  def common(diffField: DiffSchemaField[Common], maxDiffExamples: Int): CommonRawDiffsPartBuilder[K, R, Kx, Any] =
    new CommonRawDiffsPartBuilder[K, R, Kx, Any](
      maxDiffExamples = maxDiffExamples,
      rawDiffExampleBuilder =
        ExampleFactory.diffExample.rawExampleBuilder(diffField.raw.map(_.ordinal), diffField.exampleFactory)
    )

  def leftOnly(diffField: DiffSchemaField[HasLeft], maxDiffExamples: Int): LeftOnlyRawDiffsPartBuilder[K, R, Kx, Any] =
    new LeftOnlyRawDiffsPartBuilder[K, R, Kx, Any](
      maxDiffExamples = maxDiffExamples,
      rawDiffExampleBuilder =
        ExampleFactory.diffExample.rawExampleBuilder(diffField.raw.map(_.ordinal), diffField.exampleFactory)
    )

  def rightOnly(diffField: DiffSchemaField[HasRight],
                maxDiffExamples: Int): RightOnlyRawDiffsPartBuilder[K, R, Kx, Any] =
    new RightOnlyRawDiffsPartBuilder[K, R, Kx, Any](
      maxDiffExamples = maxDiffExamples,
      rawDiffExampleBuilder =
        ExampleFactory.diffExample.rawExampleBuilder(diffField.raw.map(_.ordinal), diffField.exampleFactory)
    )

  def leftOnlyRecord(maxDiffExamples: Int): LeftOnlyRawDiffsPartBuilder[K, R, Kx, Rx] =
    new LeftOnlyRawDiffsPartBuilder[K, R, Kx, Rx](
      maxDiffExamples = maxDiffExamples,
      rawDiffExampleBuilder = StructExampleFactory.record.rawExampleBuilder
    )

  def rightOnlyRecord(maxDiffExamples: Int): RightOnlyRawDiffsPartBuilder[K, R, Kx, Rx] =
    new RightOnlyRawDiffsPartBuilder[K, R, Kx, Rx](
      maxDiffExamples = maxDiffExamples,
      rawDiffExampleBuilder = StructExampleFactory.record.rawExampleBuilder
    )
}
