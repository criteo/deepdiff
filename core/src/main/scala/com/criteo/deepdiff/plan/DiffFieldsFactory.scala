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

import com.criteo.deepdiff.config.{DeepDiffConfig, ExplodedArrayConfig}
import com.criteo.deepdiff.plan.field.{FieldPath, _}
import com.criteo.deepdiff.type_support.comparator.EqualityParams
import com.criteo.deepdiff.utils._
import org.apache.spark.sql.types.{ArrayType, StructType}

import scala.collection.mutable
import scala.language.higherKinds

/** Used to create all the [[DiffField]] and the [[KeyDiffField]] for a schema. */
private[plan] final class DiffFieldsFactory(config: DeepDiffConfig) {
  private val aliases: Common[Map[FieldPath, String]] = Common(config.leftAliases, config.rightAliases).map(_.map({
    case (fullName, alias) => FieldPath(fullName) -> alias
  }))

  /** Entry point and should be used whenever a new root for the keys is used, for exploded arrays typically.
    *
    * @param rootField Root field for the keys and parent of all fields in the schema.
    * @param keys All the key fields that should be used to group identical records.
    * @param rawSchema Left and/or right raw schema.
    * @param defaultEqualityParams Default parameters, such as numerical tolerances, to be used when
    *                              comparing fields. Those are propagated to nested fields, so they're
    *                              not necessarily the global default ones.
    */
  def buildDiffFields[LR[+T] <: LeftRight[T]](
      rootField: Field[LR],
      keys: Set[FieldPath],
      rawSchema: LR[StructType],
      defaultEqualityParams: EqualityParams
  )(implicit lr: LeftRightFunctor[LR]): DiffFields = {
    // New root for the keys, so a new key collector is used.
    val keysCollector = new KeyDiffFieldsCollector(rootField, keys)
    new DiffFields(
      keysCollector,
      buildDiffSchema(parentField = rootField, rawSchema, defaultEqualityParams, rootField.path, keysCollector)
    )
  }

  // Returning the builder and not the DiffSchema, so that parent caller can use the matchLeftRight method
  // of the builder to imitate a pattern matching on the DiffSchema LeftRight type parameter.
  private def buildDiffSchema[LR[+T] <: LeftRight[T]](
      parentField: Field[LR],
      rawSchema: LR[StructType],
      defaultEqualityParams: EqualityParams,
      rootPath: FieldPath,
      keyCollector: KeyDiffFieldsCollector
  )(implicit lr: LeftRightFunctor[LR]): DiffSchemaBuilder = {
    // Used to handle both the KeyCollector and DiffSchemaBuilder at once. The KeyCollector is not
    // tied to the schema, as it does not depend on the parentField but on the rootField. The latter
    // being the level at which records are grouped.
    val collector = new DiffFieldsCollector(rootPath, keyCollector, schemaBuilder = new DiffSchemaBuilder(rawSchema))

    rawSchema
      .zipCommon(aliases)
      .zip[FieldPath](parentField.rawPath)
      .map[mutable.LinkedHashMap[String, RawField]]({
        // Using a LinkedMap to keep the same ordering as the left schema.
        // This improves readability by keep the left (usually the reference) schema
        // consistent (if pruned) and testing as the order of (diff)fields is deterministic.
        case ((rawSchema, aliases), rawPath) => buildRawFieldsLinkedMap(rawPath, rawSchema, aliases)
      }) match {
      case Common(leftRawFields: mutable.LinkedHashMap[String, RawField],
                  rightRawFields: mutable.LinkedHashMap[String, RawField]) =>
        leftRawFields
          .foreach({
            case (name, leftRawField) =>
              rightRawFields.get(name) match {
                case Some(rightRawField) =>
                  val field = parentField.child(name, Common(leftRawField, rightRawField))
                  collector.addDiffField(field, defaultEqualityParams)
                case _ =>
                  collector.addDiffField(parentField.child[HasLeft](name, OnlyLeft(leftRawField)),
                                         defaultEqualityParams)
              }
          })

        rightRawFields.foreach({
          case (name, rawField) =>
            if (!leftRawFields.contains(name)) {
              collector.addDiffField(parentField.child[HasRight](name, OnlyRight(rawField)), defaultEqualityParams)
            }
        })
      case OnlyLeft(leftRawFields: collection.Map[String, RawField]) =>
        leftRawFields.foreach({
          case (name, rawField) =>
            collector.addDiffField(parentField.child[HasLeft](name, OnlyLeft(rawField)), defaultEqualityParams)
        })
      case OnlyRight(rightRawFields: collection.Map[String, RawField]) =>
        rightRawFields.foreach({
          case (name, rawField) =>
            collector.addDiffField(parentField.child[HasRight](name, OnlyRight(rawField)), defaultEqualityParams)
        })
    }

    collector.schemaBuilder
  }

  private final class DiffFieldsCollector[LRS[+T] <: LeftRight[T]](
      val rootPath: FieldPath,
      val keyCollector: KeyDiffFieldsCollector,
      val schemaBuilder: DiffSchemaBuilder
  ) {
    def addDiffField[LR[+T] <: LeftRight[T]](field: MergedField[LR], defaultEqualityParams: EqualityParams)(implicit
        lr: LeftRightFunctor[LR]
    ): Unit = {
      if (!ConfigProxy.isIgnored(field)) {
        keyCollector.addIfKey(field)
        val equalityParams = ConfigProxy.getEqualityParams(field, defaultEqualityParams)
        val dataType = field.rawField.map(_.dataType)
        if (dataType.forall(_.isInstanceOf[StructType])) {
          addStructDiffField(field, dataType.asInstanceOf[LR[StructType]], equalityParams)
        } else if (dataType.forall(_.isInstanceOf[ArrayType])) {
          val elementDataType = dataType.map(_.asInstanceOf[ArrayType].elementType)
          if (elementDataType.forall(_.isInstanceOf[StructType])) {
            addArrayStructDiffField(field, elementDataType.asInstanceOf[LR[StructType]], equalityParams)
          } else {
            addAtomicDiffField(field, equalityParams)
          }
        } else {
          addAtomicDiffField(field, equalityParams)
        }
      }
    }

    private def addAtomicDiffField(field: MergedField[LeftRight], equalityParams: EqualityParams): Unit = {
      val relativePath = field.path.pathFrom(rootPath)
      field.rawField match {
        case rawField @ Common(leftRawField, rightRawField) =>
          if (leftRawField.dataType == rightRawField.dataType) {
            schemaBuilder.addCommon(
              AtomicDiffField(relativePath.fullName, rawField, equalityParams)
            )
          } else {
            // If the types are different they're represented as left-only/right-only fields.
            // config.ignore.leftOnly/rightOnly MUST NOT apply on those though as they aren't real
            // left/right-only fields.
            schemaBuilder.addLeft(
              AtomicDiffField[HasLeft](fullName = relativePath.virtual(leftRawField.dataType.simpleString),
                                       OnlyLeft(leftRawField),
                                       equalityParams)
            )
            schemaBuilder.addRight(
              AtomicDiffField[HasRight](fullName = relativePath.virtual(rightRawField.dataType.simpleString),
                                        OnlyRight(rightRawField),
                                        equalityParams)
            )
          }
        case rawField: OnlyLeft[RawField] =>
          schemaBuilder.addLeft(
            AtomicDiffField[HasLeft](relativePath.fullName, rawField, equalityParams)
          )
        case rawField: OnlyRight[RawField] =>
          schemaBuilder.addRight(
            AtomicDiffField[HasRight](relativePath.fullName, rawField, equalityParams)
          )
      }
    }

    private def addStructDiffField[LR[+T] <: LeftRight[T]](
        field: MergedField[LR],
        nestedSchema: LR[StructType],
        equalityParams: EqualityParams
    )(implicit lr: LeftRightFunctor[LR]): Unit = {
      val relativePath = field.path.pathFrom(rootPath)
      // Re-using the same KeyCollector, as the root field of the keys didn't change.
      buildDiffSchema(field, nestedSchema, equalityParams, rootPath, keyCollector).matchLeftRight(
        common = { nestedDiffSchema =>
          schemaBuilder.addCommon(
            StructDiffField[Common](relativePath.fullName,
                                    field.matchLeftRight[Common[RawField]](common = _.rawField),
                                    nestedDiffSchema)
          )
        },
        left = { nestedDiffSchema =>
          schemaBuilder.addLeft(
            StructDiffField[HasLeft](
              relativePath.fullName,
              field.matchLeftRight[HasLeft[RawField]](common = _.rawField, leftOnly = _.rawField),
              nestedDiffSchema)
          )
        },
        right = { nestedDiffSchema =>
          schemaBuilder.addRight(
            StructDiffField[HasRight](
              relativePath.fullName,
              field.matchLeftRight[HasRight[RawField]](common = _.rawField, rightOnly = _.rawField),
              nestedDiffSchema
            )
          )
        },
        // If no nested fields, we just ignore the current field
        empty = () => ()
      )
    }

    private def addArrayStructDiffField[LR[+T] <: LeftRight[T]](
        field: MergedField[LR],
        nestedSchema: LR[StructType],
        equalityParams: EqualityParams
    )(implicit lr: LeftRightFunctor[LR]): Unit = {
      val relativePath = field.path.pathFrom(rootPath)
      ConfigProxy.getExplodedArrayConfig(field) match {
        case Some(explodedArrayConfig) =>
          buildDiffFields(
            // Changing root field, which is the array field itself. From here on all keys are relative to the array.
            rootField = field,
            keys = explodedArrayConfig.keys.map(FieldPath(_)),
            nestedSchema,
            equalityParams
          ).matchLeftRight(
            common = { (nestedKeys: Seq[KeyDiffField[Common]], nestedDiffSchema: DiffSchema[Common]) =>
              schemaBuilder.addCommon(
                ArrayStructDiffField[Common](
                  relativePath.fullName,
                  field.matchLeftRight[Common[RawField]](common = _.rawField),
                  ExplodedNestedStructDiffField[Common](field.path.fullName,
                                                        nestedDiffSchema,
                                                        nestedKeys,
                                                        explodedArrayConfig.multipleMatches.omitIfAllIdentical)
                )
              )
            },
            left = { (nestedKeys: Seq[KeyDiffField[HasLeft]], nestedDiffSchema: DiffSchema[HasLeft]) =>
              schemaBuilder.addLeft(
                ArrayStructDiffField[HasLeft](
                  relativePath.fullName,
                  field.matchLeftRight[HasLeft[RawField]](common = _.rawField, leftOnly = _.rawField),
                  ExplodedNestedStructDiffField[HasLeft](field.path.fullName,
                                                         nestedDiffSchema,
                                                         nestedKeys,
                                                         explodedArrayConfig.multipleMatches.omitIfAllIdentical)
                )
              )
            },
            right = { (nestedKeys: Seq[KeyDiffField[HasRight]], nestedDiffSchema: DiffSchema[HasRight]) =>
              schemaBuilder.addRight(
                ArrayStructDiffField[HasRight](
                  relativePath.fullName,
                  field.matchLeftRight[HasRight[RawField]](common = _.rawField, rightOnly = _.rawField),
                  ExplodedNestedStructDiffField[HasRight](field.path.fullName,
                                                          nestedDiffSchema,
                                                          nestedKeys,
                                                          explodedArrayConfig.multipleMatches.omitIfAllIdentical)
                )
              )
            },
            // If no nested fields, we just ignore the current field
            empty = () => ()
          )
        case None =>
          val nestedFullName = relativePath.virtual("struct")
          // Re-using the same KeyCollector, as the root field of the keys didn't change.
          buildDiffSchema(field, nestedSchema, equalityParams, rootPath, keyCollector).matchLeftRight(
            common = { nestedDiffSchema =>
              schemaBuilder.addCommon(
                ArrayStructDiffField[Common](
                  relativePath.fullName,
                  field.matchLeftRight[Common[RawField]](common = _.rawField),
                  PositionalNestedStructDiffField[Common](nestedFullName, nestedDiffSchema)
                )
              )
            },
            left = { nestedDiffSchema =>
              schemaBuilder.addLeft(
                ArrayStructDiffField[HasLeft](
                  relativePath.fullName,
                  field.matchLeftRight[HasLeft[RawField]](common = _.rawField, leftOnly = _.rawField),
                  PositionalNestedStructDiffField[HasLeft](nestedFullName, nestedDiffSchema)
                )
              )
            },
            right = { nestedDiffSchema =>
              schemaBuilder.addRight(
                ArrayStructDiffField[HasRight](
                  relativePath.fullName,
                  field.matchLeftRight[HasRight[RawField]](common = _.rawField, rightOnly = _.rawField),
                  PositionalNestedStructDiffField[HasRight](nestedFullName, nestedDiffSchema)
                )
              )
            },
            // If no nested fields, we just ignore the current field
            empty = () => ()
          )
      }
    }
  }

  /** Utility wrapper */
  private object ConfigProxy {
    type Field = MergedField[LeftRight]

    def getExplodedArrayConfig(field: Field): Option[ExplodedArrayConfig] =
      config.explodedArrays.get(field.path.fullName)

    def getEqualityParams(field: Field, default: EqualityParams): EqualityParams =
      EqualityParams.fromConf(config.tolerances.get(field.path.fullName), default)

    def isIgnored(field: Field): Boolean =
      config.ignore.fields.contains(field.path.fullName) || (field.rawField match {
        case _: Common[_]    => false
        case _: OnlyLeft[_]  => config.ignore.leftOnly
        case _: OnlyRight[_] => config.ignore.rightOnly
      })
  }

  private def buildRawFieldsLinkedMap(
      rootRawPath: FieldPath,
      schema: StructType,
      aliases: Map[FieldPath, String]
  ): mutable.LinkedHashMap[String, RawField] = {
    val fields = new mutable.LinkedHashMap[String, RawField]()
    schema.fields
      .foreach(structField => {
        val maybeAlias = aliases.get(rootRawPath.child(structField.name))
        fields += maybeAlias.getOrElse(structField.name) -> RawField(ordinal = schema.fieldIndex(structField.name),
                                                                     structField,
                                                                     maybeAlias)
      })
    fields
  }
}

/** Simple wrapper class which provides matchLeftRight to imitate Scala's pattern matching.
  * It also ensures that the found key fields are consistent with the DiffSchema.
  */
private[plan] final class DiffFields(
    private val keysCollector: KeyDiffFieldsCollector,
    private val schemaFieldsCollector: DiffSchemaBuilder
) {
  def matchLeftRight[T](
      common: (Seq[KeyDiffField[Common]], DiffSchema[Common]) => T = matchError.common2 _,
      left: (Seq[KeyDiffField[HasLeft]], DiffSchema[HasLeft]) => T = matchError.leftOnly2 _,
      right: (Seq[KeyDiffField[HasRight]], DiffSchema[HasRight]) => T = matchError.rightOnly2 _,
      empty: () => T = matchError.noFields _
  ): T = {
    schemaFieldsCollector.matchLeftRight(
      common(keysCollector.resultAs[Common], _),
      left(keysCollector.resultAs[HasLeft], _),
      right(keysCollector.resultAs[HasRight], _),
      empty = { () =>
        assert(keysCollector.isEmpty, "Nothing to compare, but still found keys.")
        empty()
      }
    )
  }
}
