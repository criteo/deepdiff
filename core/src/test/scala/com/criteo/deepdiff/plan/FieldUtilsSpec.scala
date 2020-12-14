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

import com.criteo.deepdiff.plan.field.{FieldPath, RawField}
import com.criteo.deepdiff.test_utils.SparkUtils
import com.criteo.deepdiff.type_support.comparator.defaultEqualityParams
import com.criteo.deepdiff.utils._
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class FieldUtilsSpec extends AnyFlatSpec with Matchers {
  import FieldUtils._
  import SparkUtils.{dummyLeftSchema, dummyRightSchema}

  "commonKey" should "generate KeyDiffField" in {
    commonKey("name") should be(
      KeyDiffField(
        fullName = "name",
        relativeRawPath = Common(FieldPath("name"), FieldPath("name"))
      ))
    commonKey("name") should be(
      KeyDiffField(
        fullName = "name",
        relativeRawPath = Common(FieldPath("name"), FieldPath("name"))
      ))
  }

  "field" should "generate the appropriate field" in {
    field(dummyLeftSchema, "int") should be(RawField("int", 0, IntegerType))
    field(dummyLeftSchema, "boolean") should be(RawField("boolean", 5, BooleanType))
  }

  it should "support nested schemas" in {
    field(dummyLeftSchema, "struct.s2") should be(RawField("s2", 1, StringType))
  }

  "atomic" should "generate AtomicDiffField[Common]" in {
    atomic(dummyLeftSchema, "string", dummyRightSchema) should be(
      AtomicDiffField(
        fullName = "string",
        raw = Common(RawField("string", 4, StringType), RawField("string", 2, StringType)),
        leftRightEqualityParams = defaultEqualityParams
      ))
  }

  it should "generate AtomicDiffField[HasLeft]" in {
    atomic(dummyLeftSchema, "string") should be(
      AtomicDiffField[HasLeft](
        fullName = "string",
        raw = OnlyLeft(RawField("string", 4, StringType)),
        leftRightEqualityParams = defaultEqualityParams
      ))
  }

  it should "generate AtomicDiffField[HasRight]" in {
    atomic("string", dummyRightSchema) should be(
      AtomicDiffField[HasRight](
        fullName = "string",
        raw = OnlyRight(RawField("string", 2, StringType)),
        leftRightEqualityParams = defaultEqualityParams
      ))
  }
}
