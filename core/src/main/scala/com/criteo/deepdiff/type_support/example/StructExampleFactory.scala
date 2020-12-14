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

package com.criteo.deepdiff.type_support.example

import com.criteo.deepdiff.KeyExample
import com.criteo.deepdiff.diff.{K, Kx, Rx}
import com.criteo.deepdiff.plan.key.KeyMaker
import com.criteo.deepdiff.raw_part.{RawDiffExampleBuilder, RawToFriendlyDiffExample}
import com.criteo.deepdiff.utils.LeftRight
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters

import java.nio.ByteBuffer

private[deepdiff] object StructExampleFactory {
  object record {
    def rawExampleBuilder: RawDiffExampleBuilder[K, InternalRow, Kx, InternalRow] =
      new RawDiffExampleBuilder[K, InternalRow, Kx, InternalRow] {
        @inline def asRawKeyExample(key: K): Kx = KeyMaker.buildBinaryKey(key)
        @inline def asRawLeftExample(left: InternalRow): InternalRow = left.copy()
        @inline def asRawRightExample(right: InternalRow): InternalRow = right.copy()
      }

    def rawToFriendly(keyMaker: KeyMaker, structExample: LeftRight[StructExampleFactory])
        : RawToFriendlyDiffExample[Kx, Rx, KeyExample, Map[String, Any]] =
      new RawToFriendlyDiffExample[Kx, Rx, KeyExample, Map[String, Any]] {
        def rawToFriendlyKey(rawKey: Kx): KeyExample =
          keyMaker.buildExampleFromBinaryKey(ByteBuffer.wrap(rawKey.getBytes))
        def rawToFriendlyLeft(rawLeft: Rx): Map[String, Any] = structExample.maybeLeft.get.rawToFriendly(rawLeft)
        def rawToFriendlyRight(rawRight: Rx): Map[String, Any] = structExample.maybeRight.get.rawToFriendly(rawRight)
      }
  }

  object multipleMatches {
    def rawExampleBuilder: RawDiffExampleBuilder[K, Iterator[InternalRow], Kx, Seq[InternalRow]] =
      new RawDiffExampleBuilder[K, Iterator[InternalRow], Kx, Seq[InternalRow]] {
        @inline def asRawKeyExample(key: K): Kx = KeyMaker.buildBinaryKey(key)
        @inline def asRawLeftExample(left: Iterator[InternalRow]): Seq[InternalRow] = left.map(_.copy()).toList
        @inline def asRawRightExample(right: Iterator[InternalRow]): Seq[InternalRow] = right.map(_.copy()).toList
      }

    def rawToFriendly(keyMaker: KeyMaker, structExample: LeftRight[StructExampleFactory])
        : RawToFriendlyDiffExample[Kx, Seq[Rx], KeyExample, Seq[Map[String, Any]]] =
      new RawToFriendlyDiffExample[Kx, Seq[Rx], KeyExample, Seq[Map[String, Any]]] {
        def rawToFriendlyKey(rawKey: Kx): KeyExample =
          keyMaker.buildExampleFromBinaryKey(ByteBuffer.wrap(rawKey.getBytes))
        def rawToFriendlyLeft(rawLeft: Seq[Rx]): Seq[Map[String, Any]] = {
          val builder = structExample.maybeLeft.get
          rawLeft.map(builder.rawToFriendly)
        }
        def rawToFriendlyRight(rawRight: Seq[Rx]): Seq[Map[String, Any]] = {
          val builder = structExample.maybeRight.get
          rawRight.map(builder.rawToFriendly)
        }
      }

  }

  final case class FieldExampleFactory(name: String, ordinal: Int, exampleFactory: ExampleFactory)
}

private[deepdiff] final case class StructExampleFactory private[example] (
    numFields: Int,
    fieldExampleFactories: Seq[StructExampleFactory.FieldExampleFactory])
    extends ExampleFactory {
  type RawExample = InternalRow
  type FriendlyExample = Map[String, Any]

  protected def getNotNullRaw(input: SpecializedGetters, ordinal: Int): InternalRow =
    input.getStruct(ordinal, numFields).copy()

  override private[example] def getFriendly(input: SpecializedGetters, ordinal: Int): Map[String, Any] =
    rawToFriendly(input.getStruct(ordinal, numFields))

  protected def rawToFriendly(rawExample: InternalRow): Map[String, Any] =
    fieldExampleFactories
      .map({
        case StructExampleFactory.FieldExampleFactory(name, ordinal, exampleFactory) =>
          name -> (if (rawExample.isNullAt(ordinal)) null else exampleFactory.getFriendly(rawExample, ordinal))
      })
      .toMap
}
