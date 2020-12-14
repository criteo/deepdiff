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

import com.criteo.deepdiff.plan._
import com.criteo.deepdiff.plan.key.KeyMaker
import com.criteo.deepdiff.raw_part._
import com.criteo.deepdiff.type_support.example.{ExampleFactory, StructExampleFactory}
import com.criteo.deepdiff.utils.LeftRight
import com.criteo.deepdiff.{DatasetDiffs, DatasetsSchema, KeyExample}

import scala.collection.mutable
import scala.language.higherKinds

private object DFRawToFriendly {
  type FieldRawToFriendly = RawToFriendlyDiffExample[Kx, Any, KeyExample, Any]
  final case class ExplodedArray(keyMaker: KeyMaker,
                                 structDiffField: ExplodedNestedStructDiffField[LeftRight],
                                 rawToFriendlyByFullName: Map[String, FieldRawToFriendly])
}

/** Builds the final result, a [[DatasetDiffs]], from the working object [[DatasetRawDiffsPart]]
  *
  * @param plan Plan used to generate the [[DatasetRawDiffsPart]]
  */
private[deepdiff] final class DFRawToFriendly(plan: DiffPlan) {
  import DFRawToFriendly._

  private val explodedArrays = new mutable.ListBuffer[(String, ExplodedArray)]()
  // fills explodedArrays
  private val rawToFriendlyByFullName = buildFieldsRawToFriendly(plan.groupKey, plan.schema).toMap

  def generateDatasetDiffs(rawPart: DatasetRawDiffsPart[Kx, Rx]): DatasetDiffs[KeyExample, Map[String, Any]] = {
    val fieldToExplodedArrays = explodedArrays.toMap
    DatasetDiffs(
      root = rawPart.root.buildMatchedRecordsDiffs(
        multipleMatchesRawToFriendly =
          StructExampleFactory.multipleMatches.rawToFriendly(plan.groupKey, plan.recordExample),
        recordRawToFriendly = StructExampleFactory.record.rawToFriendly(plan.groupKey, plan.recordExample),
        rawToFriendlyByFullName
      ),
      explodedArrays = rawPart.explodedArrays.map({
        case (fullName, matchedRecordsRawDiffsPart: RecordRawDiffsPart[Kx, Rx]) =>
          val ExplodedArray(nestedKey, nestedStruct, rawToFriendlyByFullName) = fieldToExplodedArrays(fullName)
          val recordRawToFriendly = StructExampleFactory.record.rawToFriendly(nestedKey, nestedStruct.exampleFactory)
          fullName -> (matchedRecordsRawDiffsPart match {
            case m: MatchedRecordsRawDiffsPart[Kx, Rx] =>
              m.buildMatchedRecordsDiffs(
                multipleMatchesRawToFriendly =
                  StructExampleFactory.multipleMatches.rawToFriendly(nestedKey, nestedStruct.exampleFactory),
                recordRawToFriendly,
                rawToFriendlyByFullName
              )
            case m: LeftOnlyRecordRawDiffsPart[Kx, Rx]  => m.buildLeftOnlyRecordDiffs(recordRawToFriendly)
            case m: RightOnlyRecordRawDiffsPart[Kx, Rx] => m.buildRightOnlyRecordDiffs(recordRawToFriendly)
          })
      }),
      schema = {
        val aliasedPrunedSchema = plan.schema.aliasedPruned
        DatasetsSchema(
          left = aliasedPrunedSchema.left,
          right = aliasedPrunedSchema.right
        )
      }
    )
  }

  private def buildFieldsRawToFriendly[LR[+T] <: LeftRight[T]](
      keyMaker: KeyMaker,
      schema: DiffSchema[LR]): List[(String, FieldRawToFriendly)] =
    schema.fields
      .foldRight[List[(String, FieldRawToFriendly)]](Nil)({
        case (field, acc) => buildRawToFriendly(keyMaker, field) ::: acc
      })

  private def buildRawToFriendly(key: KeyMaker,
                                 diffField: DiffSchemaField[LeftRight]): List[(String, FieldRawToFriendly)] = {
    (
      diffField.fullName -> ExampleFactory.diffExample.rawToFriendly(key, diffField.exampleFactory)
    ) :: (diffField match {
      case struct: StructDiffField[LeftRight] =>
        buildFieldsRawToFriendly(key, struct.schema)
      case array: ArrayStructDiffField[LeftRight] =>
        array.nestedStruct match {
          case nestedStruct: PositionalNestedStructDiffField[LeftRight] =>
            val nestedKey = nestedStruct.getVirtualKey(key)
            val nestedStructRawToFriendly = nestedStruct.fullName -> {
              ExampleFactory.diffExample
                .rawToFriendly(nestedKey, nestedStruct.exampleFactory)
            }
            nestedStructRawToFriendly :: buildFieldsRawToFriendly(nestedKey, nestedStruct.schema)
          case nestedStruct: ExplodedNestedStructDiffField[LeftRight] =>
            val nestedKey = nestedStruct.getKey(key)
            explodedArrays += nestedStruct.fullName -> ExplodedArray(
              nestedKey,
              nestedStruct,
              buildFieldsRawToFriendly(nestedKey, nestedStruct.schema).toMap
            )
            Nil
        }
      case _: AtomicDiffField[LeftRight] => Nil
    })
  }
}
