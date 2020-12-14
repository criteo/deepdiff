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

package com.criteo.deepdiff.plan.key

import com.criteo.deepdiff.plan.KeyDiffField
import com.criteo.deepdiff.utils.Common
import com.criteo.deepdiff.{CompositeKeyExample, FieldsKeyExample}
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.types.StructType

import java.nio.ByteBuffer

class NestedKeyMakerSpec extends SchemaKeyMakerSpec {
  final class NestedKeyMakerWrapper(val nestedKey: NestedStructKeyMaker[Common]) extends SchemaKeyMaker {
    def buildLeft(input: SpecializedGetters): Array[Byte] = {
      nestedKey.builder.left(input).getBytes
    }
    def buildRight(input: SpecializedGetters): Array[Byte] = {
      nestedKey.builder.right(input).getBytes
    }
    def deserialize(buffer: ByteBuffer): FieldsKeyExample = {
      nestedKey
        .buildExampleFromBinaryKey(buffer)
        .asInstanceOf[CompositeKeyExample]
        .keyExamples
        .head match {
        case nestedKey: FieldsKeyExample => nestedKey
      }
    }
  }

  it should behave like reliableKeyExtractor { (schema: Common[StructType], keysPath: Seq[KeyDiffField[Common]]) =>
    new NestedKeyMakerWrapper(new NestedStructKeyMaker(EmptyKeyMaker, schema, keysPath))
  }
}
