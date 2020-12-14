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

/** Field hierarchy representing what should be present in the final
  * [[DatasetDiffs]]'s binaryFieldDiffs.
  * Those can only be created here, but allows the DiffAccumulators and DiffExample
  * de-serialization to access the same information without coupling tightly both of them.
  */
package com.criteo.deepdiff.plan

import com.criteo.deepdiff.plan.field.{FieldPath, RawField}
import com.criteo.deepdiff.plan.key._
import com.criteo.deepdiff.type_support.comparator.{Comparator, EqualityParams}
import com.criteo.deepdiff.type_support.example.{ArrayExampleFactory, ExampleFactory, StructExampleFactory}
import com.criteo.deepdiff.type_support.unsafe.BinaryEquivalence
import com.criteo.deepdiff.utils.{Common, LeftRight, LeftRightFunctor}
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

import scala.language.higherKinds

////////////////////
//// Interfaces ////
////////////////////

/** Represents a field in the BinaryRecordDiff's binaryFieldDiffs hierarchy */
private[deepdiff] sealed trait DiffField {

  /** Unique name within a [[com.criteo.deepdiff.RecordDiffs]] used to identify a field.
    *
    * This means that fields within an exploded array for example are in a different namespace that one from the root
    * record.
    */
  def fullName: String
}

/** DiffField which is actually present in the schema, contrary to virtual fields. The ROOT field itself
  * is typically not a real field.
  *
  * @tparam LR Whether the field is common to both schema or left/right only.
  */
private[deepdiff] sealed trait DiffSchemaField[+LR[+T] <: LeftRight[T]] extends DiffField {

  /** Raw information on the field in the left and/or right schemas. */
  def raw: LR[RawField]

  /** Used to generate raw and human friendly examples. */
  def exampleFactory: LR[ExampleFactory]

  /** Used for the binary comparison of a left and right record. */
  def binaryEquivalence: BinaryEquivalence

  /** Pruned version of the field. Its name may change, but most importantly its position. This allows to have
    * efficient binary comparison by ensuring both schema have the same fields and the same ordering. It also improves
    * Deep Diff efficiency by not reading the ignored fields at all.
    */
  private[plan] def pruned: LR[StructField]

  /** Similar as pruned, but with the alias if one exists. Used only for the DatasetDiffs schema. */
  private[plan] def aliasedPruned: LR[StructField]

  /** Used to compare a left (right) record to another left (right) record to identify multiple identical matches. */
  def multipleMatchesBinaryEquivalence: LR[BinaryEquivalence]

  /** Used for the binary comparison of a left (right) record to another left (right) record
    * to identify multiple identical matches.
    */
  def multipleMatchesComparator: LR[Comparator]
}

/** Interface used by all nested struct fields.
  *
  * This includes a typical nested struct and virtual fields such as the struct field generated for arrays of structs.
  */
private[deepdiff] sealed trait AnyStructDiffField[+LR[+T] <: LeftRight[T]] extends DiffField {

  /** Schema of the nested struct. */
  def schema: DiffSchema[LR]

  final def allNestedDiffFieldsFullName: Set[String] = recGetNames(schema)
  private def recGetNames(schema: DiffSchema[LeftRight]): Set[String] = {
    (schema.fields.map(_.fullName).toSet
      ++ schema.fields
        .flatMap({
          case s: StructDiffField[LeftRight] => Some(recGetNames(s.schema))
          case _                             => None
        })
        .flatten)
  }
}

/** Virtual field representing a single struct in an (exploded) array of structs. */
private[deepdiff] sealed trait VirtualNestedStructDiffField[+LR[+T] <: LeftRight[T]] extends AnyStructDiffField[LR] {
  def exampleFactory: LR[StructExampleFactory] = schema.recordExampleFactory
}

/////////////////////
//// Diff Fields ////
/////////////////////

/** Represents the record itself in the binaryFieldDiffs. */
private[deepdiff] object RootDiffField extends DiffField {
  val fullName: String = FieldPath.ROOT.fullName
}

/** Key used for either the grouping or the exploded arrays
  *
  * While it could be a [[DiffSchemaField]], as it's really in the schema, we never need all that information. It still
  * is a [[DiffField]] as we expect the fullName to be consistent the [[DiffSchemaField]] that will be generated for the
  * key field.
  *
  * @param relativeRawPath relative raw path, as present in the schema, from the root (same as before) of the key. Used
  *                        when generating the associated KeyMaker.
  */
private[deepdiff] final case class KeyDiffField[+LR[+T] <: LeftRight[T]] private[plan] (
    fullName: String,
    relativeRawPath: LR[FieldPath]
) extends DiffField

/** Nested struct within an array that is matched by its position. */
private[deepdiff] final case class PositionalNestedStructDiffField[+LR[+T] <: LeftRight[T]] private[plan] (
    fullName: String,
    schema: DiffSchema[LR]
) extends VirtualNestedStructDiffField[LR] {
  def getVirtualKey(parentKey: KeyMaker): NestedPositionKeyMaker = parentKey.nestedPositionKey()
}

/** Nested struct within an array exploded (matched) with the provided keys
  *
  * @param keyFields Key fields that must be used to match the nested structs.
  */
private[deepdiff] final case class ExplodedNestedStructDiffField[+LR[+T] <: LeftRight[T]] private[plan] (
    fullName: String,
    schema: DiffSchema[LR],
    keyFields: Seq[KeyDiffField[LR]],
    omitMultipleMatchesIfAllIdentical: Boolean
)(implicit lr: LeftRightFunctor[LR])
    extends VirtualNestedStructDiffField[LR] {
  def getKey(parentKey: KeyMaker): NestedStructKeyMaker[LR] =
    parentKey.nested(schema.raw, keyFields)
}

////////////////////////////
//// Diff Schema Fields ////
////////////////////////////

/** Fields with no nested [[DiffField]] such as Integers, Strings or Maps.
  *
  * Map of struct have no special treatment as they're barely exist in our ecosystem.
  *
  * @param leftRightEqualityParams Used to customize the equality, typically with tolerances for numerical fields.
  */
private[deepdiff] final case class AtomicDiffField[+LR[+T] <: LeftRight[T]] private[plan] (
    fullName: String,
    raw: LR[RawField],
    leftRightEqualityParams: EqualityParams
)(implicit lr: LeftRightFunctor[LR])
    extends DiffSchemaField[LR] {
  // Both dataTypes, if they exist, must be strictly equal.
  require(raw.map(_.dataType) match {
    case Common(l, r) => l == r
    case _            => true
  })

  private[plan] def pruned: LR[StructField] = raw.map(_.structField)
  private[plan] def aliasedPruned: LR[StructField] = raw.map { r => r.structField.copy(name = r.aliasedName) }

  // could eventually be changed to add more fine grained control over multiple matches
  private def equalityParams: LR[EqualityParams] = raw.map(_ => EqualityParams.strict)

  def comparator: Comparator = Comparator(raw.any.dataType, leftRightEqualityParams)
  def binaryEquivalence: BinaryEquivalence =
    BinaryEquivalence(raw.any.dataType, leftRightEqualityParams)
  lazy val exampleFactory: LR[ExampleFactory] =
    raw.map(f => ExampleFactory(f.dataType))
  lazy val multipleMatchesBinaryEquivalence: LR[BinaryEquivalence] =
    raw.zip(equalityParams).map({ case (f, equalityParam) => BinaryEquivalence(f.dataType, equalityParam) })
  lazy val multipleMatchesComparator: LR[Comparator] =
    raw.zip(equalityParams).map({ case (f, equalityParam) => Comparator(f.dataType, equalityParam) })

}

/** Nested struct field */
private[deepdiff] final case class StructDiffField[+LR[+T] <: LeftRight[T]] private[plan] (
    fullName: String,
    raw: LR[RawField],
    schema: DiffSchema[LR]
)(implicit lr: LeftRightFunctor[LR])
    extends DiffSchemaField[LR]
    with AnyStructDiffField[LR] {
  private[plan] def pruned: LR[StructField] =
    raw
      .zip(schema.pruned)
      .map({
        case (field: RawField, s: StructType) => StructField(field.name, s)
      })
  private[plan] def aliasedPruned: LR[StructField] =
    raw
      .zip(schema.aliasedPruned)
      .map({
        case (field: RawField, s: StructType) => StructField(field.aliasedName, s)
      })
  def binaryEquivalence: BinaryEquivalence = schema.binaryEquivalence
  def multipleMatchesBinaryEquivalence: LR[BinaryEquivalence] = schema.multipleMatchesBinaryEquivalence
  def multipleMatchesComparator: LR[Comparator] = schema.multipleMatchesComparator
  def exampleFactory: LR[StructExampleFactory] = schema.recordExampleFactory
}

/** Array of structs. The nested structs are simply matched by their position in the array. */
private[deepdiff] final case class ArrayStructDiffField[+LR[+T] <: LeftRight[T]] private[plan] (
    fullName: String,
    raw: LR[RawField],
    nestedStruct: VirtualNestedStructDiffField[LR]
)(implicit lr: LeftRightFunctor[LR])
    extends DiffSchemaField[LR] {
  protected def schema: DiffSchema[LR] = nestedStruct.schema

  private[plan] def pruned: LR[StructField] =
    raw
      .zip(schema.pruned)
      .map({
        case (field: RawField, s: StructType) => StructField(field.name, ArrayType(s))
      })
  private[plan] def aliasedPruned: LR[StructField] =
    raw
      .zip(schema.aliasedPruned)
      .map({
        case (field: RawField, s: StructType) => StructField(field.aliasedName, ArrayType(s))
      })
  def binaryEquivalence: BinaryEquivalence =
    BinaryEquivalence.array(schema.binaryEquivalence)

  def multipleMatchesBinaryEquivalence: LR[BinaryEquivalence] =
    schema.multipleMatchesBinaryEquivalence.map(BinaryEquivalence.array)

  def exampleFactory: LR[ArrayExampleFactory[StructExampleFactory]] =
    schema.recordExampleFactory.map(ExampleFactory.structArrayExampleFactory)

  def multipleMatchesComparator: LR[Comparator] =
    schema.multipleMatchesComparator
      .zip(multipleMatchesBinaryEquivalence)
      .map({
        case (recordComparator, binaryEquivalence) =>
          Comparator.recordArrayComparator(recordComparator, binaryEquivalence)
      })
}
