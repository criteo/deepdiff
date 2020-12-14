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

import com.criteo.deepdiff.test_utils.SparkUtils
import com.criteo.deepdiff.type_support.unsafe.BinaryEquivalence._
import com.criteo.deepdiff.type_support.unsafe.UnsafeComparison.{NotEqual, Unknown}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.language.implicitConversions

object UnsafeComparatorSpec {
  final case class TestCase[T](left: T, right: T, expected: UnsafeComparison)
}

final class UnsafeComparatorSpec extends AnyFlatSpec with Matchers {
  import UnsafeComparatorSpec._

  val allBinEq = Seq(Undetermined,
                     BinaryEqualImpliesEqual,
                     BinaryEqualImpliesEqualAndDifferentSizeImpliesNotEqual,
                     EqualEquivalentToBinaryEqual)

  "array" should "be supported" in {
    implicit def asArrayData(x: Array[_]): ArrayData = SparkUtils.asSparkArray(x)
    for (
      binEq <- allBinEq;
      test <- Seq(
        TestCase(Array("a"), Array("b"), Unknown),
        TestCase(Array("a"), Array("a"), Unknown),
        TestCase(Array("a"), Array("b", "a"), NotEqual)
      )
    ) {
      UnsafeComparator.array(binEq).compare(test.left, test.right) should be(test.expected)
    }
  }

  "map" should "be supported" in {
    implicit def asMapData(x: Map[_, _]): MapData = SparkUtils.asSparkMap(x.toSeq)
    for (
      binEq <- allBinEq;
      test <- Seq(
        TestCase(Map("a" -> 1, "b" -> 2), Map("a" -> 1), NotEqual),
        TestCase(Map("a" -> 1), Map("b" -> 1), Unknown),
        TestCase(Map("a" -> 1), Map("a" -> 1), Unknown)
      )
    ) {
      UnsafeComparator.map(EqualEquivalentToBinaryEqual, binEq).compare(test.left, test.right) should be(test.expected)
    }
  }

  "row" should "be supported" in {
    implicit def asInternalRow(e: Product): InternalRow = SparkUtils.asSparkRow(e)
    for (
      binEq <- allBinEq;
      test <- Seq(
        TestCase((1, "test"), (1, "test", 2f), Unknown),
        TestCase((1, "test"), (1, "test"), Unknown)
      )
    ) {
      UnsafeComparator.struct(binEq).compare(test.left, test.right) should be(test.expected)
    }
  }
}
