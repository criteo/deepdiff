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

import com.criteo.deepdiff.config.DeepDiffConfig
import com.criteo.deepdiff.diff.accumulators._
import com.criteo.deepdiff.plan._
import com.criteo.deepdiff.plan.key.KeyMaker
import com.criteo.deepdiff.raw_part._
import com.criteo.deepdiff.type_support.example.StructExampleFactory
import com.criteo.deepdiff.type_support.unsafe.UnsafeComparator
import com.criteo.deepdiff.utils.{Common, HasLeft, HasRight}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.unsafe.types.UTF8String

import java.util
import java.util.function.{BiConsumer, BiFunction}

object DatasetDiffAccumulatorsBuilder {
  private def nestedLeftOnlyFields(
      struct: AnyStructDiffField[HasLeft]): List[(String, LeftRightRawDiffsPart[Kx, Any])] = {
    struct.allNestedDiffFieldsFullName
      .map(_ -> LeftOnlyRawDiffsPart[Kx, Any](0, RawDiffPart.empty))
      .toList
  }

  private def nestedRightOnlyFields(
      struct: AnyStructDiffField[HasRight]): List[(String, LeftRightRawDiffsPart[Kx, Any])] = {
    struct.allNestedDiffFieldsFullName
      .map(_ -> RightOnlyRawDiffsPart[Kx, Any](0, RawDiffPart.empty))
      .toList
  }
}

/** Creates the [[DatasetDiffAccumulator]] based on the Deep Diff plan.
  *
  * @param config Deep Diff configuration
  */
private[deepdiff] final class DatasetDiffAccumulatorsBuilder(config: DeepDiffConfig) {
  import DatasetDiffAccumulatorsBuilder._
  import KindOfDiff._

  private var explodedArrays: List[(String, RecordRawDiffsPartBuilder[Kx, Rx])] = Nil

  def build(plan: DiffPlan): DatasetDiffAccumulator = {
    assert(explodedArrays.isEmpty)
    new DatasetDiffAccumulator(
      // it will build the explodedArrays seq
      numFields = plan.schema.rawNumFields,
      root = commonMatchedRecordDiffsPartBuilder(plan.groupKey, plan.schema, config.multipleMatches.omitIfAllIdentical),
      explodedArrays = explodedArrays.toMap
    )
  }

  private def commonMatchedRecordDiffsPartBuilder(
      keyMaker: KeyMaker,
      schema: DiffSchema[Common],
      omitMultipleMatchesIfAllIdentical: Boolean): MatchedRecordDiffsPartBuilder[K, R, Kx, Rx] = {
    new MatchedRecordDiffsPartBuilder[K, R, Kx, Rx](
      recordRawDiffsAccumulator = new RecordRawDiffsAccumulator[K, R, Kx, Rx] {
        val recordExampleBuilder: RawDiffExampleBuilder[K, R, Kx, Rx] = StructExampleFactory.record.rawExampleBuilder
        val multipleMatchesExampleBuilder: RawDiffExampleBuilder[K, Iterator[R], Kx, Seq[Rx]] =
          StructExampleFactory.multipleMatches.rawExampleBuilder
        private val recordFieldsDiffAccumulator = buildRecordFieldsDiffAccumulator(keyMaker, schema)
        private val leftRecordComparator = schema.multipleMatchesComparator.left
        private val rightRecordComparator = schema.multipleMatchesComparator.right

        @inline def compareRecords(key: K, left: R, right: R): KindOfDiff =
          recordFieldsDiffAccumulator.compareRecords(key, left, right)

        @inline def isLeftEqual(x: R, y: R): Boolean =
          leftRecordComparator.isEqual(x, y)

        @inline def isRightEqual(x: R, y: R): Boolean =
          rightRecordComparator.isEqual(x, y)

        @inline def binaryIdenticalRecord(record: R): Unit =
          recordFieldsDiffAccumulator.binaryIdenticalRecord(record)

        def results: List[(String, LeftRightRawDiffsPart[Kx, Any])] = recordFieldsDiffAccumulator.result()
      },
      omitMultipleMatchesIfAllIdentical = omitMultipleMatchesIfAllIdentical,
      maxDiffExamples = config.maxExamples
    )
  }

  private def buildRecordFieldsDiffAccumulator(keyMaker: KeyMaker,
                                               schema: DiffSchema[Common]): RecordFieldsDiffAccumulator = {
    val keysFullName = schema.common.map(_.fullName).toSet.intersect(keyMaker.keysFullName.toSet)
    RecordFieldsDiffAccumulator(
      unsafeRecordComparator = UnsafeComparator.struct(schema.binaryEquivalence),
      diffAccumulators = schema
        .mapCompared(
          leftOnly = { field =>
            Some(buildLeftDiffAccumulator(keyMaker, field))
          },
          rightOnly = { field =>
            Some(buildRightDiffAccumulator(keyMaker, field))
          },
          common = { field =>
            // No need to compare fields already compared through the key.
            if (!keysFullName.contains(field.fullName)) {
              Some(buildCommonDiffAccumulator(keyMaker, field))
            } else {
              None
            }
          }
        )
        .flatten
        .toArray,
      keysFullName = keysFullName
    )
  }

  private def buildCommonDiffAccumulator(keyMaker: KeyMaker,
                                         diffField: DiffSchemaField[Common]): FieldDiffAccumulator = {
    diffField match {

      // -- COMMON ATOMIC --
      case atomic: AtomicDiffField[Common] =>
        new FieldDiffAccumulator {
          val fullName: String = diffField.fullName
          private val leftOrdinal = diffField.raw.left.ordinal
          private val rightOrdinal = diffField.raw.right.ordinal
          private val fieldDiffs = LeftRightRawDiffsPartBuilders.common(diffField, config.maxExamples)
          private val comparator = atomic.comparator

          def compareRecords(key: K, left: R, leftAnyNull: Boolean, right: R, rightAnyNull: Boolean): KindOfDiff =
            comparator.addComparisonResult(key,
                                           left,
                                           leftAnyNull,
                                           leftOrdinal,
                                           right,
                                           rightAnyNull,
                                           rightOrdinal,
                                           fieldDiffs)

          def result(binaryIdenticalRecords: Long = 0): List[(String, LeftRightRawDiffsPart[Kx, Any])] =
            (fullName -> fieldDiffs.result(binaryIdenticalRecords)) :: Nil
        }

      // -- COMMON STRUCT --
      case struct: StructDiffField[Common] =>
        val nestedRecordDiffAccumulator = buildNestedRecordDiffAccumulator(keyMaker, struct)
        new FieldWithNestedRecordsDiffAccumulator {
          val fullName: String = struct.fullName
          private val leftOrdinal = struct.raw.left.ordinal
          private val rightOrdinal = struct.raw.right.ordinal
          private val leftNumFields: Int = struct.schema.rawNumFields.left

          def compareRecords(key: K, left: R, leftAnyNull: Boolean, right: R, rightAnyNull: Boolean): KindOfDiff =
            nestedRecordDiffAccumulator.compareInputs(key, left, leftOrdinal, right, rightOrdinal)

          def binaryIdenticalRecord(record: R): Unit =
            nestedRecordDiffAccumulator.nestedBinaryIdenticalRecord(record.getStruct(leftOrdinal, leftNumFields))

          def result(): List[(String, LeftRightRawDiffsPart[Kx, Any])] =
            nestedRecordDiffAccumulator.result()
        }
      case array: ArrayStructDiffField[Common] =>
        array.nestedStruct match {

          // -- COMMON STRUCT ARRAY --
          case nestedStruct: PositionalNestedStructDiffField[Common] =>
            val positionKeyMaker = nestedStruct.getVirtualKey(keyMaker)
            val nestedRecordDiffAccumulator = buildNestedRecordDiffAccumulator(positionKeyMaker, array.nestedStruct)
            new ArrayStructDiffAccumulator(array, config.maxExamples) {
              @inline def compareArrayElements(key: K, left: ArrayData, right: ArrayData): KindOfDiff = {
                var flags: KindOfDiff = NEUTRAL
                val length: Int =
                  if (left.numElements() < right.numElements()) left.numElements() else right.numElements()
                var i = 0

                while (i < length) {
                  flags |= nestedRecordDiffAccumulator.compareInputs(KeyMaker.nested(key, positionKeyMaker.build(i)),
                                                                     left,
                                                                     i,
                                                                     right,
                                                                     i)
                  i += 1
                }

                while (i < left.numElements()) {
                  val leftStruct = left.getStruct(i, leftNumFields)
                  if (leftStruct != null) {
                    flags |= nestedRecordDiffAccumulator.nestedLeftOnly(KeyMaker.nested(key, positionKeyMaker.build(i)),
                                                                        leftStruct)
                  } else {
                    flags |= DIFFERENT
                  }
                  i += 1
                }

                while (i < right.numElements()) {
                  val rightStruct = right.getStruct(i, rightNumFields)
                  if (rightStruct != null) {
                    flags |= nestedRecordDiffAccumulator.nestedRightOnly(
                      KeyMaker.nested(key, positionKeyMaker.build(i)),
                      rightStruct)
                  } else {
                    flags |= DIFFERENT
                  }
                  i += 1
                }

                flags
              }

              @inline def nestedBinaryIdenticalRecord(count: Int, nestedNotNullRecords: Iterator[R]): Unit = {
                nestedRecordDiffAccumulator.nestedBinaryIdenticalRecords(count, nestedNotNullRecords)
              }

              def result(): List[(String, LeftRightRawDiffsPart[Kx, Any])] =
                fieldResult() :: nestedRecordDiffAccumulator.result()
            }

          // -- COMMON EXPLODED STRUCT ARRAY --
          case nestedStruct: ExplodedNestedStructDiffField[Common] =>
            val nestedKeyMaker = nestedStruct.getKey(keyMaker)
            val matchedRecordDiffsPartBuilder = commonMatchedRecordDiffsPartBuilder(
              nestedKeyMaker,
              nestedStruct.schema,
              // Beware that in all cases, binary equivalent arrays, with multiple matches or not, WILL and MUST be
              // considered equal. The point of DeepDiff is to ensure both datasets are equivalent. We don't really
              // care whether they are multiple matches or not as long it doesn't impact comparison.
              omitMultipleMatchesIfAllIdentical = nestedStruct.omitMultipleMatchesIfAllIdentical
            )
            explodedArrays ::= nestedStruct.fullName -> matchedRecordDiffsPartBuilder
            new ArrayStructDiffAccumulator(array, config.maxExamples) {
              private val leftKeyBuilder = nestedKeyMaker.builder.left
              private val rightKeyBuilder = nestedKeyMaker.builder.right
              type Matches = Common[List[R]]
              private val leftMatchesMerger = new BiFunction[Matches, Matches, Matches] with Serializable {
                def apply(previous: Matches, value: Matches): Matches = {
                  Common(value.left.head :: previous.left, previous.right)
                }
              }
              private val rightMatchesMerger = new BiFunction[Matches, Matches, Matches] with Serializable {
                def apply(previous: Matches, value: Matches): Matches = {
                  Common(previous.left, value.right.head :: previous.right)
                }
              }

              @inline def compareArrayElements(key: K, left: ArrayData, right: ArrayData): KindOfDiff = {
                var flags: KindOfDiff = NEUTRAL
                var nullsDifference: Int = 0
                val exploded = new util.HashMap[UTF8String, Matches](left.numElements() + right.numElements())

                var i = 0
                while (i < left.numElements()) {
                  val leftStruct = left.getStruct(i, leftNumFields)
                  if (leftStruct == null) nullsDifference += 1
                  else exploded.merge(leftKeyBuilder(leftStruct), Common(leftStruct :: Nil, Nil), leftMatchesMerger)
                  i += 1
                }

                i = 0
                while (i < right.numElements()) {
                  val rightStruct = right.getStruct(i, rightNumFields)
                  if (rightStruct == null) nullsDifference -= 1
                  else
                    exploded.merge(rightKeyBuilder(rightStruct), Common(Nil, rightStruct :: Nil), rightMatchesMerger)
                  i += 1
                }

                exploded.forEach(new BiConsumer[UTF8String, Matches] {
                  def accept(explodeKey: UTF8String, matches: Matches): Unit = {
                    val Common(left, right) = matches
                    val nestedKey = KeyMaker.nested(key, explodeKey)
                    flags |= {
                      if (right.isEmpty) {
                        matchedRecordDiffsPartBuilder.leftOnly(nestedKey, left.head, left.tail.iterator)
                      } else if (left.isEmpty) {
                        matchedRecordDiffsPartBuilder.rightOnly(nestedKey, right.head, right.tail.iterator)
                      } else {
                        matchedRecordDiffsPartBuilder.compare(nestedKey,
                                                              left.head,
                                                              left.tail.iterator,
                                                              right.head,
                                                              right.tail.iterator)
                      }
                    }
                  }
                })

                flags | (if (nullsDifference == 0) IDENTICAL else DIFFERENT)
              }

              @inline def nestedBinaryIdenticalRecord(count: Int, nestedNotNullRecords: Iterator[R]): Unit =
                matchedRecordDiffsPartBuilder.binaryIdenticalRecords(nestedNotNullRecords)

              def result(): List[(String, LeftRightRawDiffsPart[Kx, Any])] =
                fieldResult() :: Nil
            }
        }
    }
  }

  private def buildLeftDiffAccumulator(keyMaker: KeyMaker,
                                       diffField: DiffSchemaField[HasLeft]): FieldLeftDiffAccumulator =
    diffField match {

      // -- LEFT ATOMIC --
      case atomic: AtomicDiffField[HasLeft] =>
        new FieldLeftDiffAccumulator {
          val fullName: String = atomic.fullName
          private val leftOrdinal = atomic.raw.left.ordinal
          private val fieldDiffs = LeftRightRawDiffsPartBuilders.leftOnly(atomic, config.maxExamples)

          @inline def leftRecordOnly(key: K, left: R, leftAnyNull: Boolean): KindOfDiff =
            if (leftAnyNull && left.isNullAt(leftOrdinal)) fieldDiffs.nullValue() else fieldDiffs.leftOnly(key, left)

          def result(): List[(String, LeftRightRawDiffsPart[Kx, Any])] =
            (fullName -> fieldDiffs.result()) :: Nil
        }

      // -- LEFT STRUCT --
      case struct: StructDiffField[HasLeft] =>
        new FieldLeftDiffAccumulator {
          val fullName: String = struct.fullName
          private val leftOrdinal = struct.raw.left.ordinal
          private val leftNumFields = struct.schema.rawNumFields.left
          private val fieldDiffs = LeftRightRawDiffsPartBuilders.leftOnlyRecord(config.maxExamples)
          private val leftOnlyFields = nestedLeftOnlyFields(struct)

          @inline def leftRecordOnly(key: K, left: R, leftAnyNull: Boolean): KindOfDiff = {
            val leftStruct = left.getStruct(leftOrdinal, leftNumFields)
            if (leftStruct == null) {
              fieldDiffs.nullValue()
            } else {
              fieldDiffs.leftOnly(key, leftStruct)
            }
          }

          def result(): List[(String, LeftRightRawDiffsPart[Kx, Any])] =
            (fullName -> fieldDiffs.result()) :: leftOnlyFields
        }
      case array: ArrayStructDiffField[HasLeft] =>
        array.nestedStruct match {

          // -- LEFT STRUCT ARRAY --
          case nestedStruct: PositionalNestedStructDiffField[HasLeft] =>
            new ArrayStructLeftDiffAccumulator(array, config.maxExamples) {
              private val positionKeyMaker = nestedStruct.getVirtualKey(keyMaker)
              private val fieldDiffs = LeftRightRawDiffsPartBuilders.leftOnlyRecord(config.maxExamples)
              private val nestedStructFullName = nestedStruct.fullName
              private val leftOnlyFields = nestedLeftOnlyFields(nestedStruct)

              @inline protected def nestedLeftRecordOnly(key: K, position: Int, nestedLeft: R): Unit =
                fieldDiffs.leftOnly(KeyMaker.nested(key, positionKeyMaker.build(position)), nestedLeft)

              def result(): List[(String, LeftRightRawDiffsPart[Kx, Any])] =
                fieldResult :: (nestedStructFullName -> fieldDiffs.result()) :: leftOnlyFields
            }

          // -- LEFT EXPLODED STRUCT ARRAY --
          case nestedStruct: ExplodedNestedStructDiffField[HasLeft] =>
            val nestedKeyMaker = nestedStruct.getKey(keyMaker)
            val leftOnlyRecordDiffsPartBuilder = new LeftOnlyRecordDiffsPartBuilder(
              rawRecordExampleBuilder = StructExampleFactory.record.rawExampleBuilder,
              diffFieldsFullName = nestedStruct.allNestedDiffFieldsFullName,
              maxDiffExamples = config.maxExamples
            )
            explodedArrays ::= nestedStruct.fullName -> leftOnlyRecordDiffsPartBuilder
            new ArrayStructLeftDiffAccumulator(array, config.maxExamples) {
              private val leftKeyBuilder = nestedKeyMaker.builder.left
              @inline protected def nestedLeftRecordOnly(key: K, position: Int, nestedLeft: R): Unit =
                leftOnlyRecordDiffsPartBuilder.leftOnly(KeyMaker.nested(key, leftKeyBuilder(nestedLeft)), nestedLeft)

              def result(): List[(String, LeftRightRawDiffsPart[Kx, Any])] =
                fieldResult :: Nil
            }
        }
    }

  private def buildRightDiffAccumulator(keyMaker: KeyMaker,
                                        diffField: DiffSchemaField[HasRight]): FieldRightDiffAccumulator =
    diffField match {

      // -- RIGHT ATOMIC --
      case atomic: AtomicDiffField[HasRight] =>
        new FieldRightDiffAccumulator {
          val fullName: String = atomic.fullName
          private val rightOrdinal = atomic.raw.right.ordinal
          private val fieldDiffs = LeftRightRawDiffsPartBuilders.rightOnly(atomic, config.maxExamples)

          @inline def rightRecordOnly(key: K, right: R, rightAnyNull: Boolean): KindOfDiff =
            if (rightAnyNull && right.isNullAt(rightOrdinal)) fieldDiffs.nullValue()
            else fieldDiffs.rightOnly(key, right)

          def result(): List[(String, LeftRightRawDiffsPart[Kx, Any])] =
            (fullName -> fieldDiffs.result()) :: Nil
        }

      // -- RIGHT STRUCT --
      case struct: StructDiffField[HasRight] =>
        new FieldRightDiffAccumulator {
          val fullName: String = struct.fullName
          private val rightOrdinal = struct.raw.right.ordinal
          private val rightNumFields = struct.schema.rawNumFields.right
          private val fieldDiffs = LeftRightRawDiffsPartBuilders.rightOnlyRecord(config.maxExamples)
          private val rightOnlyFields = nestedRightOnlyFields(struct)

          @inline def rightRecordOnly(key: K, right: R, rightAnyNull: Boolean): KindOfDiff = {
            val rightStruct = right.getStruct(rightOrdinal, rightNumFields)
            if (rightStruct == null) {
              fieldDiffs.nullValue()
            } else {
              fieldDiffs.rightOnly(key, rightStruct)
            }
          }

          def result(): List[(String, LeftRightRawDiffsPart[Kx, Any])] =
            (fullName -> fieldDiffs.result()) :: rightOnlyFields
        }
      case array: ArrayStructDiffField[HasRight] =>
        array.nestedStruct match {

          // -- RIGHT STRUCT ARRAY --
          case nestedStruct: PositionalNestedStructDiffField[HasRight] =>
            new ArrayStructRightDiffAccumulator(array, config.maxExamples) {
              private val positionKeyMaker = nestedStruct.getVirtualKey(keyMaker)
              private val fieldDiffs = LeftRightRawDiffsPartBuilders.rightOnlyRecord(config.maxExamples)
              private val nestedStructFullName = nestedStruct.fullName
              private val rightOnlyFields = nestedRightOnlyFields(nestedStruct)

              @inline protected def nestedRightRecordOnly(key: K, position: Int, nestedLeft: R): Unit =
                fieldDiffs.rightOnly(KeyMaker.nested(key, positionKeyMaker.build(position)), nestedLeft)

              def result(): List[(String, LeftRightRawDiffsPart[Kx, Any])] =
                fieldResult :: (nestedStructFullName -> fieldDiffs.result()) :: rightOnlyFields
            }

          // -- RIGHT EXPLODED STRUCT ARRAY --
          case nestedStruct: ExplodedNestedStructDiffField[HasRight] =>
            val nestedKeyMaker = nestedStruct.getKey(keyMaker)
            val rightOnlyRecordDiffsPartBuilder = new RightOnlyRecordDiffsPartBuilder(
              rawRecordExampleBuilder = StructExampleFactory.record.rawExampleBuilder,
              diffFieldsFullName = nestedStruct.allNestedDiffFieldsFullName,
              maxDiffExamples = config.maxExamples
            )
            explodedArrays ::= nestedStruct.fullName -> rightOnlyRecordDiffsPartBuilder
            new ArrayStructRightDiffAccumulator(array, config.maxExamples) {
              private val rightKeyBuilder = nestedKeyMaker.builder.right
              @inline protected def nestedRightRecordOnly(key: K, position: Int, nestedLeft: R): Unit =
                rightOnlyRecordDiffsPartBuilder.rightOnly(KeyMaker.nested(key, rightKeyBuilder(nestedLeft)), nestedLeft)

              def result(): List[(String, LeftRightRawDiffsPart[Kx, Any])] =
                fieldResult :: Nil
            }
        }
    }

  private def buildNestedRecordDiffAccumulator(keyMaker: KeyMaker,
                                               struct: AnyStructDiffField[Common]): NestedRecordDiffAccumulator = {
    new NestedRecordDiffAccumulator(
      struct,
      nestedRecordFieldsDiff = buildRecordFieldsDiffAccumulator(keyMaker, struct.schema),
      config.maxExamples
    )
  }
}
