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

package com.criteo.deepdiff.type_support.unsafe

import com.criteo.deepdiff.type_support.comparator.EqualityParams
import org.apache.spark.sql.types._

/** Expresses what can be implied from the binary comparison. Used to generate the
  * appropriate [[UnsafeComparator]] for a field/record.
  *
  * There several "levels" : Undetermined, BinaryEqualImpliesEqualAndDifferentSizeImpliesNotEqual
  * and EqualIsEquivalentToBinaryEqual. Those have a strict ordering, which is used to merge them.
  *
  * @param strictness Having a higher strictness implies filling *all* the criteria of the previous
  *                   BinaryEquivalences.
  */
private[deepdiff] sealed abstract class BinaryEquivalence(private val strictness: Int) extends Serializable {
  override def toString: String = this.getClass.getSimpleName.dropRight(1)
}

private[deepdiff] object BinaryEquivalence {

  /** Nothing can be inferred from the binary comparison, hence it should not even be used.
    * This happens when structs have left/right-only fields or have different field ordering.
    */
  private[deepdiff] case object Undetermined extends BinaryEquivalence(strictness = 0)

  /** If two elements have the same binary representation they're equal, but not the opposite. This
    * is mostly used to represent ignored variable-length fields.
    */
  private[unsafe] case object BinaryEqualImpliesEqual extends BinaryEquivalence(strictness = 1)

  /** Similar to [[BinaryEqualImpliesEqual]]. However, they're still expected to have the same binary size,
    * only the content may be different. This concerns Map fields or numerical type with tolerances.
    */
  private[unsafe] case object BinaryEqualImpliesEqualAndDifferentSizeImpliesNotEqual
      extends BinaryEquivalence(strictness = 10)

  /** As the name implies, binary comparison tells us everything we need here. */
  private[unsafe] case object EqualEquivalentToBinaryEqual extends BinaryEquivalence(strictness = 100)

  def array(elementBinaryEquivalence: BinaryEquivalence): BinaryEquivalence = elementBinaryEquivalence

  def struct(schema: StructType, fieldsBinaryEquivalence: Map[String, BinaryEquivalence]): BinaryEquivalence = {
    val unspecifiedFieldDataTypes =
      schema.fields.filterNot(f => fieldsBinaryEquivalence.contains(f.name)).map(_.dataType)
    mostLenient(unspecifiedFieldDataTypes.map(ignoredEquivalence) ++ fieldsBinaryEquivalence.values: _*)
  }

  def apply(dataType: DataType, params: EqualityParams): BinaryEquivalence =
    dataType match {
      case BooleanType | ByteType | BinaryType | StringType | DateType | TimestampType => EqualEquivalentToBinaryEqual
      case ShortType | IntegerType | LongType | FloatType | DoubleType =>
        if (params.absoluteTolerance.forall(_ == 0) && params.relativeTolerance.forall(_ == 0)) {
          EqualEquivalentToBinaryEqual
        } else {
          BinaryEqualImpliesEqualAndDifferentSizeImpliesNotEqual
        }
      case ArrayType(elementType, _) =>
        array(BinaryEquivalence(elementType, params))
      case MapType(keyType, valueType, _) =>
        // Supporting other cases wouldn't be properly tested. Tests cases for those types are really
        // difficult to generate. There aren't even a lot of map in our datasets, and none with complex keys
        // that would not validate that condition (using a map as a key for example)
        require(BinaryEquivalence(keyType, EqualityParams.strict) == EqualEquivalentToBinaryEqual)
        mostLenient(BinaryEqualImpliesEqualAndDifferentSizeImpliesNotEqual, BinaryEquivalence(valueType, params))
      case schema: StructType =>
        struct(schema,
               fieldsBinaryEquivalence = schema.fields.map(f => f.name -> BinaryEquivalence(f.dataType, params)).toMap)
    }

  private def mostLenient(binaryEquivalences: BinaryEquivalence*): BinaryEquivalence =
    if (binaryEquivalences.isEmpty) Undetermined else binaryEquivalences.minBy(_.strictness)

  private def ignoredEquivalence(dataType: DataType): BinaryEquivalence =
    dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType =>
        BinaryEqualImpliesEqualAndDifferentSizeImpliesNotEqual
      case schema: StructType      => struct(schema, Map.empty)
      case StringType | BinaryType => BinaryEqualImpliesEqual
      case _: MapType              => BinaryEqualImpliesEqual
      case _: ArrayType            => BinaryEqualImpliesEqual
    }
}
