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

import com.criteo.deepdiff.type_support.comparator.{EqualityParams, defaultEqualityParams}
import com.criteo.deepdiff.type_support.unsafe.BinaryEquivalence._
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class BinaryEquivalenceSpec extends AnyFlatSpec with Matchers {
  "struct" should "not be stricter than its elements" in {
    for (
      (expected, binaryEquivalences) <- Seq(
        Undetermined -> Seq(Undetermined),
        Undetermined -> Seq(Undetermined, BinaryEqualImpliesEqual),
        Undetermined -> Seq(Undetermined,
                            BinaryEqualImpliesEqual,
                            BinaryEqualImpliesEqualAndDifferentSizeImpliesNotEqual),
        Undetermined -> Seq(Undetermined,
                            BinaryEqualImpliesEqual,
                            BinaryEqualImpliesEqualAndDifferentSizeImpliesNotEqual,
                            EqualEquivalentToBinaryEqual),
        BinaryEqualImpliesEqual -> Seq(BinaryEqualImpliesEqual),
        BinaryEqualImpliesEqual -> Seq(BinaryEqualImpliesEqual, BinaryEqualImpliesEqualAndDifferentSizeImpliesNotEqual),
        BinaryEqualImpliesEqual -> Seq(BinaryEqualImpliesEqual,
                                       BinaryEqualImpliesEqualAndDifferentSizeImpliesNotEqual,
                                       EqualEquivalentToBinaryEqual),
        BinaryEqualImpliesEqualAndDifferentSizeImpliesNotEqual -> Seq(
          BinaryEqualImpliesEqualAndDifferentSizeImpliesNotEqual),
        BinaryEqualImpliesEqualAndDifferentSizeImpliesNotEqual -> Seq(
          BinaryEqualImpliesEqualAndDifferentSizeImpliesNotEqual,
          EqualEquivalentToBinaryEqual),
        EqualEquivalentToBinaryEqual -> Seq(EqualEquivalentToBinaryEqual)
      )
    ) {
      val fieldsBinaryEquivalence = binaryEquivalences.zipWithIndex
        .map({
          case (binaryEquivalence, index) => s"field$index" -> binaryEquivalence
        })
        .toMap
      val schema = StructType(fieldsBinaryEquivalence.keys.map(StructField(_, StringType)).toArray)
      struct(schema, fieldsBinaryEquivalence) should be(expected)
    }
  }

  "array" should "not be stricter than its elements" in {
    for (
      binaryEq <- Seq[BinaryEquivalence](Undetermined,
                                         BinaryEqualImpliesEqual,
                                         BinaryEqualImpliesEqualAndDifferentSizeImpliesNotEqual,
                                         EqualEquivalentToBinaryEqual)
    ) {
      array(binaryEq) should be(binaryEq)
    }
  }

  "binary equivalent types" should "be treated as such" in {
    for (tpe <- Seq[DataType](BooleanType, ByteType, BinaryType, StringType)) {
      BinaryEquivalence(tpe, defaultEqualityParams) should be(EqualEquivalentToBinaryEqual)
    }
  }

  val toleranceEquality = EqualityParams(Some(1), None, true)
  "composite types" should "supported" in {
    BinaryEquivalence(ArrayType(IntegerType), EqualityParams.strict) should be(EqualEquivalentToBinaryEqual)
    BinaryEquivalence(ArrayType(IntegerType), toleranceEquality) should be(
      BinaryEqualImpliesEqualAndDifferentSizeImpliesNotEqual)
    BinaryEquivalence(MapType(StringType, IntegerType), EqualityParams.strict) should be(
      BinaryEqualImpliesEqualAndDifferentSizeImpliesNotEqual)
    BinaryEquivalence(MapType(StringType, IntegerType), toleranceEquality) should be(
      BinaryEqualImpliesEqualAndDifferentSizeImpliesNotEqual)
    BinaryEquivalence(StructType(Seq(StructField("a", IntegerType))), EqualityParams.strict) should be(
      EqualEquivalentToBinaryEqual)
    BinaryEquivalence(StructType(Seq(StructField("a", IntegerType))), toleranceEquality) should be(
      BinaryEqualImpliesEqualAndDifferentSizeImpliesNotEqual)
  }

  "numerical comparison tolerances" should "be taken into account" in {
    for (tpe <- Seq[DataType](IntegerType, LongType, FloatType, DoubleType, ShortType)) {
      BinaryEquivalence(tpe, EqualityParams.strict) should be(EqualEquivalentToBinaryEqual)
      BinaryEquivalence(tpe, toleranceEquality) should be(BinaryEqualImpliesEqualAndDifferentSizeImpliesNotEqual)
    }
  }
}
