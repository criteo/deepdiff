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

import com.criteo.deepdiff.diff.{K, Kx}
import com.criteo.deepdiff.plan.KeyDiffField
import com.criteo.deepdiff.utils._
import com.criteo.deepdiff.{DiffExample, FieldsKeyExample, KeyExample, PositionKeyExample}
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import yonik.util.MurmurHash3

import java.nio.ByteBuffer
import java.util
import scala.language.higherKinds

/** Base interface for objects making keys.
  *
  * Keys are expected to be wrapped in a [[UTF8String]]. A series of keys will be represented as a list of [[UTF8String]].
  * Making Spark believe that keys are String, which are represented by [[UTF8String]] internally, is important for
  * performance during the co-group. See [[GroupKeyMaker]] for more information on this. It also provides nice utility
  * functions and only wraps the binary data, no copies implied, hence it's a convenient wrapper. A series of keys is
  * only concatenated when a DiffExample must be created.
  *
  * If an example needs to be made, the raw data is concatenated to push back the cost of generating examples only after
  * the reduce stage. Hence instances of [[KeyMaker]] must work together: nested keys need to call buildExample of their
  * parent key themselves.
  */
private[deepdiff] sealed trait KeyMaker extends Serializable {

  /** Generate a human-friendly example from the binary key stored in a [[DiffExample]] */
  def buildExampleFromBinaryKey(buffer: ByteBuffer): KeyExample

  def keysFullName: Seq[String]

  // Used when generating the KeyMakers.
  private[plan] def nested[LR[+T] <: LeftRight[T]](rawSchema: LR[StructType], keysPath: Seq[KeyDiffField[LR]])(implicit
      lr: LeftRightFunctor[LR]
  ): NestedStructKeyMaker[LR] =
    new NestedStructKeyMaker(parentKey = this, rawSchema, keysPath)

  private[plan] def nestedPositionKey(): NestedPositionKeyMaker =
    new NestedPositionKeyMaker(parentKey = this)
}

private[deepdiff] object KeyMaker {

  /** Generates the binary key to be kept in a [[DiffExample]] */
  def buildBinaryKey(key: K): Kx =
    key match {
      case head :: Nil => head.copy()
      case _           => UTF8String.concat(key: _*)
    }

  /** Combines the nested key with its parent one to a single one. */
  @inline def nested(key: K, nestedKey: UTF8String): K = nestedKey :: key
}

/** Only used in tests */
private[key] object EmptyKeyMaker extends KeyMaker {
  def buildExampleFromBinaryKey(buffer: ByteBuffer): KeyExample = PositionKeyExample(0)
  def keysFullName: Seq[String] = Seq.empty
}

/** Key used by Spark to group records.
  *
  * It relies on [[KeyBuffer]] to generate the actual binary representation of the key. A hash of the whole key is put
  * at the beginning as Spark uses the initial 64 bit of String, named prefix, to compare cheaply keys first. If those
  * are equal, Spark will have to access the whole UnsafeRow directly. Hence we generate the best prefix possible by
  * prepending a 64bit hash of the whole binary key to avoid the costly key comparison. It improves the sort speed by
  * a factor of 3 compared to a constant prefix.
  *
  * The underlying ByteBuffer is returned to avoid memory copies. We could wrap it in another read-only
  * object, but returning a ByteBuffer makes it also very explicit that you have to be careful with it.
  *
  * @param rawSchema Schema of the input DataFrames
  * @param keyFields Fields to include in the group key.
  */
private[deepdiff] final class GroupKeyMaker private[plan] (
    rawSchema: Common[StructType],
    keyFields: Seq[KeyDiffField[Common]]
) extends KeyMaker {
  private val keyBuffers = KeyBuffer(rawSchema, keyFields, offsetBytes = 8)
  private val longPair = new MurmurHash3.LongPair()

  def buildLeft(row: SpecializedGetters): ByteBuffer = extract(keyBuffers.left, row)
  def buildRight(row: SpecializedGetters): ByteBuffer = extract(keyBuffers.right, row)
  def keysFullName: Seq[String] = keyFields.map(_.fullName)

  @inline private def extract(keyBuffer: KeyBuffer, input: SpecializedGetters): ByteBuffer = {
    val buffer = keyBuffer.putKey(input)
    // Spark's prefix for the sorting is 64bit long, so we're pushing all the entropy we can to avoid
    // collisions and therefore costly whole key comparisons (especially the memory access).
    // Yonik's implementation is the fastest 64bit variant I measured. It's 3 times faster than
    // Guava for 100 bytes. While the difference decrease, and is of only 20% at 10KB, the average
    // key size is probably closer to 100 bytes, which is already enough for 12 Long.
    MurmurHash3.murmurhash3_x64_128(buffer.array, 8, buffer.position - 8, 42, longPair)
    buffer.putLong(0, longPair.val1)
  }

  def buildExampleFromBinaryKey(buffer: ByteBuffer): FieldsKeyExample =
    FieldsKeyExample(keyBuffers.left.readKeyFrom(buffer).toMap)
}

/** Key used when exploding arrays.
  *
  * It relies on [[KeyBuffer]] to generate the actual binary representation of the key. The binary
  * is wrapped by Sparks [[UTF8String]] for two reasons:
  * - has a efficient hash/equality
  * - be consistent with the group key (an [[UTF8String]]) for simpler code.
  *
  * @param rawSchema Schema of the input DataFrames
  * @param keyFields Fields to include in the group key.
  */
private[deepdiff] final class NestedStructKeyMaker[+LR[+T] <: LeftRight[T]] private[plan] (
    parentKey: KeyMaker,
    rawSchema: LR[StructType],
    keyFields: Seq[KeyDiffField[LR]]
)(implicit lr: LeftRightFunctor[LR])
    extends KeyMaker {
  private val keyBuffers = KeyBuffer(rawSchema, keyFields)

  val builder: LR[SpecializedGetters => UTF8String] = keyBuffers.map(keyBuffer => extract(keyBuffer, _))
  def keysFullName: Seq[String] = keyFields.map(_.fullName)

  @inline private def extract(keyBuffer: KeyBuffer, input: SpecializedGetters): UTF8String = {
    val buffer = keyBuffer.putKey(input)
    // Copy the key from the buffer. The copy could be avoided by storing all the keys
    // in the buffer and keeping their offsets in memory. But would require a growable buffer
    // and non-negligible code change. However I didn't even see that copy in a single Babar
    // profile showing up, so it's probably not worth it.
    UTF8String.fromBytes(util.Arrays.copyOf(buffer.array, buffer.position))
  }

  def buildExampleFromBinaryKey(buffer: ByteBuffer): KeyExample = {
    val nestedKey = FieldsKeyExample(keyBuffers.any.readKeyFrom(buffer).toMap)
    parentKey.buildExampleFromBinaryKey(buffer).nested(nestedKey)
  }
}

/** Nested key used to represent the position of the struct when comparing struct arrays.
  *
  * @param parentKey Parent key.
  */
private[deepdiff] final class NestedPositionKeyMaker private[plan] (parentKey: KeyMaker) extends KeyMaker {
  @transient private lazy val buffer: ByteBuffer = ByteBuffer.allocate(4)
  def buildExampleFromBinaryKey(buffer: ByteBuffer): KeyExample = {
    val position = buffer.getInt()
    parentKey.buildExampleFromBinaryKey(buffer).nested(PositionKeyExample(position))
  }
  def keysFullName: Seq[String] = Seq.empty

  @inline def build(pos: Int): UTF8String = {
    buffer.putInt(0, pos)
    UTF8String.fromBytes(util.Arrays.copyOf(buffer.array(), 4))
  }
}
