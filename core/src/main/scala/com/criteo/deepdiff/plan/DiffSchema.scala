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

import com.criteo.deepdiff.plan.field.RawField
import com.criteo.deepdiff.type_support.comparator.{Comparator, StructComparator}
import com.criteo.deepdiff.type_support.example.{ExampleFactory, StructExampleFactory}
import com.criteo.deepdiff.type_support.unsafe.BinaryEquivalence
import com.criteo.deepdiff.utils._
import org.apache.spark.sql.types.{MapType, StructField, StructType}

import scala.language.higherKinds

/** Used everywhere a schema is involved, at the root level and all kinds of nested structs.
  *
  * Its main purpose is to aggregate everything from the [[DiffSchemaField]] and determine what must be done / inferred
  * for the whole schema.
  *
  * @param common Fields present on both left and right side.
  * @param left Fields only present on the left side.
  * @param right Fields only present on the right side.
  * @param raw Raw schema(s) of the records.
  *
  * @tparam LR Whether the schema is common of left/right only. Beware that this is more complex
  *            than fields. The [[DiffSchema]] can be left only despite the struct field being
  *            present on both sides. This typically happens when nested fields are ignored, but
  *            not pruned. If all fields of one side are ignored but not on the other, because
  *            of different schemas, then the nested struct will be treated as if it existed on
  *            one side only. In this case either [[HasLeft]] or [[HasRight]] would be used as raw
  *            would be [[Common]].
  */
private[deepdiff] final case class DiffSchema[+LR[+T] <: LeftRight[T]] private[plan] (
    common: Seq[DiffSchemaField[Common]],
    left: Seq[DiffSchemaField[HasLeft]],
    right: Seq[DiffSchemaField[HasRight]],
    raw: LR[StructType]
)(implicit lr: LeftRightFunctor[LR]) {
  // Sanity checks
  require(common.nonEmpty || left.nonEmpty || right.nonEmpty)
  require(common.isEmpty || (common.nonEmpty && raw.isInstanceOf[Common[StructType]]))
  require(left.isEmpty || (left.nonEmpty && raw.isInstanceOf[HasLeft[StructType]]))
  require(right.isEmpty || (right.nonEmpty && raw.isInstanceOf[HasRight[StructType]]))

  override def toString: String =
    s"""DiffSchema(
       |  common = [
       |    ${common.map(_.toString).mkString("\n    ")}
       |  ],
       |  leftOnly = [
       |    ${left.map(_.toString).mkString("\n    ")}
       |  ],
       |  rightOnly = [
       |    ${right.map(_.toString).mkString("\n    ")}
       |  ],
       |  raw = $raw,
       |)""".stripMargin

  // Imitates a map with a pattern matching on the type parameter.
  def mapCompared[T](
      common: DiffSchemaField[Common] => T = matchError.common _,
      leftOnly: DiffSchemaField[HasLeft] => T = matchError.leftOnly _,
      rightOnly: DiffSchemaField[HasRight] => T = matchError.rightOnly _
  ): Seq[T] = {
    this.common.map(common) ++ this.left.map(leftOnly) ++ this.right.map(rightOnly)
  }

  val rawNumFields: LR[Int] = raw.map(_.length)

  lazy val fields: Seq[DiffSchemaField[LeftRight]] = {
    val builder = Seq.newBuilder[DiffSchemaField[LeftRight]]
    builder ++= common
    builder ++= left
    builder ++= right
    builder.result()
  }

  /** Used to generate an human-friendly example and its intermediary representation for the DiffExample. */
  lazy val recordExampleFactory: LR[StructExampleFactory] = {
    def createFieldExamples[TLR[+T] <: LeftRight[T]](
        fields: Seq[DiffSchemaField[TLR]]
    )(implicit tlr: LeftRightFunctor[TLR]) = {
      fields.map(f =>
        f.raw
          .zip(f.exampleFactory)
          .map({
            case (field, exampleFactory) =>
              StructExampleFactory.FieldExampleFactory(field.aliasedName, field.ordinal, exampleFactory)
          }))
    }

    raw
      .zip(lr.sequence(createFieldExamples(common), createFieldExamples(left), createFieldExamples(right)))
      .map({
        case (schema, fieldExamples) => ExampleFactory.structExampleFactory(schema, fieldExamples)
      })
  }

  /** Used for the binary comparison of a left and right record. */
  lazy val binaryEquivalence: BinaryEquivalence = {
    import BinaryEquivalence._
    raw match {
      case Common(ls: StructType, rs: StructType) if ls == rs =>
        // Sanity checks
        assert(left.isEmpty)
        assert(right.isEmpty)
        struct(ls, common.map(f => f.raw.left.name -> f.binaryEquivalence).toMap)
      case _ => Undetermined
    }
  }

  /** Pruned version of the schema, to re-arrange the fields for better binary comparison
    * and removing all ignored fields.
    */
  lazy val pruned: LR[StructType] =
    lr.sequence(fields.map(_.pruned))
      .map { fields => StructType(fields.map(forceNullable)) }

  /** Used only for the DatasetDiffs.metadata to present cleanly the schema of both sides that were compared. */
  def aliasedPruned: LR[StructType] = {
    lr.sequence(fields.map(_.aliasedPruned))
      .map { fields => StructType(fields.map(forceNullable)) }
  }

  private def forceNullable(field: StructField): StructField =
    field.dataType match {
      // Forcing nullable=true makes tests a lot easier to write. As we're considering all fields
      // to be potentially null, we're not lying neither.
      case m: MapType => field.copy(dataType = m.copy(valueContainsNull = true), nullable = true)
      case _          => field.copy(nullable = true)
    }

  /** Used to compare a left (right) record to another left (right) record to identify multiple identical matches. */
  lazy val multipleMatchesComparator: LR[StructComparator] = {
    def createFieldComparators[TLR[+T] <: LeftRight[T]](
        fields: Seq[DiffSchemaField[TLR]]
    )(implicit tlr: LeftRightFunctor[TLR]) = {
      fields.map(f =>
        f.raw
          .map(_.ordinal)
          .zip(f.multipleMatchesComparator)
          .map({
            case (ordinal, comparator) => StructComparator.FieldComparator(ordinal, comparator)
          }))
    }
    raw
      .zip(lr.sequence(createFieldComparators(common), createFieldComparators(left), createFieldComparators(right)))
      .zip(multipleMatchesBinaryEquivalence)
      .map({
        case ((schema, fields), binaryEquivalence) =>
          Comparator.recordComparator(schema, fields, binaryEquivalence)
      })
  }

  /** Used for the binary comparison of a left (right) record to another left (right) record
    * to identify multiple identical matches.
    */
  lazy val multipleMatchesBinaryEquivalence: LR[BinaryEquivalence] = {
    raw
      .zip(lr.sequence(fields.map(_.raw)))
      .zip(lr.sequence(fields.map(_.multipleMatchesBinaryEquivalence)))
      .map({
        case ((schema: StructType, comparedFields: Seq[RawField]), binaryEquivalences: Seq[BinaryEquivalence]) =>
          BinaryEquivalence.struct(
            schema,
            comparedFields
              .zip(binaryEquivalences)
              .map({ case (field, binaryEquivalence) => field.name -> binaryEquivalence })
              .toMap
          )
      })
  }
}
