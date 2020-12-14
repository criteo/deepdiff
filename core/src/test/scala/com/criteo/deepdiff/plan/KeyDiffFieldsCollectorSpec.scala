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

import com.criteo.deepdiff.plan.field.{FieldPath, RawField, RootField}
import com.criteo.deepdiff.utils.{Common, HasLeft, HasRight, OnlyLeft, OnlyRight}
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class KeyDiffFieldsCollectorSpec extends AnyFlatSpec with Matchers {
  import com.criteo.deepdiff.test_utils.SparkUtils._

  it should "support common keys" in {
    val keyCollector = new KeyDiffFieldsCollector(RootField, keys = Set("a", "b", "struct.d").map(FieldPath(_)))
    val structNode =
      RootField.child("struct", Common.twin(RawField("struct", 3, struct("d" -> BooleanType, "e" -> FloatType))))
    for (
      field <- Seq(
        RootField.child("a", Common.twin(RawField("a", 0, StringType))),
        RootField.child("b", Common.twin(RawField("b", 1, IntegerType))),
        RootField.child("c", Common.twin(RawField("c", 2, ByteType))),
        structNode.child("d", Common.twin(RawField("d", 0, BooleanType))),
        structNode.child("e", Common.twin(RawField("e", 0, FloatType)))
      )
    ) {
      keyCollector.addIfKey(field)
    }

    val expected = Seq(
      KeyDiffField[Common](
        fullName = "a",
        relativeRawPath = Common.twin(FieldPath("a"))
      ),
      KeyDiffField[Common](
        fullName = "b",
        relativeRawPath = Common.twin(FieldPath("b"))
      ),
      KeyDiffField[Common](
        fullName = "struct.d",
        relativeRawPath = Common.twin(FieldPath("struct.d"))
      )
    )

    keyCollector.resultAs[Common] should contain theSameElementsAs expected
    a[AssertionError] should be thrownBy {
      keyCollector.resultAs[HasLeft]
    }
    a[AssertionError] should be thrownBy {
      keyCollector.resultAs[HasRight]
    }
  }

  it should "support left keys" in {
    val keyCollector = new KeyDiffFieldsCollector(RootField, keys = Set("b", "struct.d").map(FieldPath(_)))
    val structNode =
      RootField.child("struct", HasLeft(RawField("struct", 3, struct("d" -> BooleanType, "e" -> FloatType))))
    keyCollector.addIfKey(RootField.child("a", Common.twin(RawField("a", 0, StringType))))
    for (
      field <- Seq(
        RootField.child("b", HasLeft(RawField("b", 1, IntegerType))),
        RootField.child("c", HasLeft(RawField("c", 2, ByteType))),
        structNode.child("d", HasLeft(RawField("d", 0, BooleanType))),
        structNode.child("e", HasLeft(RawField("e", 0, FloatType)))
      )
    ) {
      keyCollector.addIfKey(field)
    }

    val expected = Seq(
      KeyDiffField[HasLeft](
        fullName = "b",
        relativeRawPath = OnlyLeft(FieldPath("b"))
      ),
      KeyDiffField[HasLeft](
        fullName = "struct.d",
        relativeRawPath = OnlyLeft(FieldPath("struct.d"))
      )
    )

    keyCollector.resultAs[HasLeft] should contain theSameElementsAs expected
    a[AssertionError] should be thrownBy {
      keyCollector.resultAs[Common]
    }
    a[AssertionError] should be thrownBy {
      keyCollector.resultAs[HasRight]
    }
  }

  it should "support right keys" in {
    val keyCollector = new KeyDiffFieldsCollector(RootField, keys = Set("b", "struct.d").map(FieldPath(_)))
    val structNode =
      RootField.child("struct", HasRight(RawField("struct", 3, struct("d" -> BooleanType, "e" -> FloatType))))
    keyCollector.addIfKey(RootField.child("a", Common.twin(RawField("a", 0, StringType))))
    for (
      field <- Seq(
        RootField.child("b", HasRight(RawField("b", 1, IntegerType))),
        RootField.child("c", HasRight(RawField("c", 2, ByteType))),
        structNode.child("d", HasRight(RawField("d", 0, BooleanType))),
        structNode.child("e", HasRight(RawField("e", 0, FloatType)))
      )
    ) {
      keyCollector.addIfKey(field)
    }

    val expected = Seq(
      KeyDiffField[HasRight](
        fullName = "b",
        relativeRawPath = OnlyRight(FieldPath("b"))
      ),
      KeyDiffField[HasRight](
        fullName = "struct.d",
        relativeRawPath = OnlyRight(FieldPath("struct.d"))
      )
    )

    keyCollector.resultAs[HasRight] should contain theSameElementsAs expected
    a[AssertionError] should be thrownBy {
      keyCollector.resultAs[Common]
    }
    a[AssertionError] should be thrownBy {
      keyCollector.resultAs[HasLeft]
    }
  }

  it should "be empty only if no field has been added" in {
    new KeyDiffFieldsCollector(RootField, keys = Set("a").map(FieldPath(_))).isEmpty should be(true)

    val k1 = new KeyDiffFieldsCollector(RootField, keys = Set("a").map(FieldPath(_)))
    k1.addIfKey(RootField.child("a", Common.twin(RawField("a", 0, StringType))))
    k1.isEmpty should be(false)

    val k2 = new KeyDiffFieldsCollector(RootField, keys = Set("a").map(FieldPath(_)))
    k2.addIfKey(RootField.child("a", HasLeft(RawField("a", 0, StringType))))
    k2.isEmpty should be(false)

    val k3 = new KeyDiffFieldsCollector(RootField, keys = Set("a").map(FieldPath(_)))
    k3.addIfKey(RootField.child("a", HasRight(RawField("a", 0, StringType))))
    k3.isEmpty should be(false)
  }
}
