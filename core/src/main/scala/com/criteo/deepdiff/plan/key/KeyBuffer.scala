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
import com.criteo.deepdiff.plan.field.FieldPath
import com.criteo.deepdiff.type_support.example.DateTimeExampleUtils
import com.criteo.deepdiff.utils.{LeftRight, LeftRightFunctor}
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.io.ObjectInputStream
import java.nio.{BufferOverflowException, ByteBuffer}
import scala.annotation.tailrec
import scala.language.higherKinds

/** Aggregates all the different values of a multi-field key in a binary representation.
  *
  * This has the advantage of being very fast to create, memory-efficient and is still easy to work
  * with as it can then be wrapped in whatever suits us best.
  *
  * @param offsetBytes Offset at the beginning of the ByteBuffer to ignore. This allows one add additional data
  *                    at the beginning.
  * @param keyReaders Used to read the binary key from a ByteBuffer when an human-friendly example needs to be generated.
  * @param keyWriters Functions to be used to write the keys to the buffer. Their ordering, which implies the ordering in
  *                   which keys will be written, must be the same as keys. Keys within the same nested struct, are written
  *                   by a single keyWriter, hence their number do not necessarily match the number of keys.
  * @param nullBitSetSize Size of the nullBitSet used by the keyWriters, which can be different than the number of keys,
  *                       as intermediate struct have their own null bit.
  */
private[key] final class KeyBuffer private (
    offsetBytes: Int,
    keyReaders: Array[KeyBuffer.KeyReader],
    keyWriters: Array[KeyBuffer.KeyWriter],
    nullBitSetSize: Int
) extends Serializable {

  @transient private lazy val blankNullBitSet: Array[Byte] = new Array[Byte](NullBitSet.bytesSize(nullBitSetSize))
  @transient private var buffer: ByteBuffer =
    ByteBuffer.allocate(offsetBytes + blankNullBitSet.length + keyReaders.length * 8) // 8 bytes, enough for int64 keys.

  /** The internal ByteBuffer is returned to avoid memory copies when Spark adds the key. */
  @inline @tailrec def putKey(input: SpecializedGetters): ByteBuffer = {
    try {
      buffer.position(offsetBytes)
      buffer.put(blankNullBitSet) // clean-up the previous nullBitSet
      var i = 0
      while (i < keyWriters.length) {
        keyWriters(i)(input, buffer)
        i += 1
      }
      buffer
    } catch {
      case _: BufferOverflowException =>
        // try has a low overhead and this exception won't occur often.
        buffer = ByteBuffer.allocate(buffer.capacity * 2)
        putKey(input)
    }
  }

  /** Called when a human-friendly example must be generated, hence performance is not critical. */
  def readKeyFrom(buffer: ByteBuffer): Seq[(String, Option[Any])] = {
    val nullBitSetOffset = buffer.position + offsetBytes
    buffer.position(nullBitSetOffset + blankNullBitSet.length)
    keyReaders.map(keyReader => {
      keyReader.name -> {
        if (NullBitSet.isNullAt(buffer, nullBitSetOffset, keyReader.nullBitIndex)) None
        else Some(keyReader.getFromBuffer(buffer))
      }
    })
  }

  // Custom Java de-serialization to re-create a buffer when de-serialized by Spark.
  private def readObject(aInputStream: ObjectInputStream): Unit = {
    aInputStream.defaultReadObject()
    buffer = ByteBuffer.allocate(offsetBytes + blankNullBitSet.length + keyReaders.length * 8)
  }
}

private[key] object KeyBuffer {
  type KeyWriter = (SpecializedGetters, ByteBuffer) => Unit
  // Byte value for booleans.
  private val TRUE: Byte = 1
  private val FALSE: Byte = 0

  /** Used by the KeyBuffer to read a ByteBuffer when a key needs to be read for a human-friendly example. */
  private final case class KeyReader(name: String, nullBitIndex: Int, getFromBuffer: ByteBuffer => Any)

  /** Parameters used during the generation of the KeyWriters.
    *
    * It is used also used to define the nullBit for each nullable field that needs to be tracked. Those are
    * generated on the fly, as intermediate nested struct are also tracked. For those we try to keep keys within the
    * same nested struct close to limit the number of bytes to be updated when the struct is null (keys are then
    * considered to be null).
    *
    * @param offsetBytes Offset to be kept intact at the beginning, used to determine the position of NullBitSet.
    * @param nullIntermediaryStructEqualsNullKeys Whether or not the intermediary struct being null is equivalent to all
    *                                             its nested keys being null or not. Currently not used as it would
    *                                             require changes in the difference accumulators to be consistent with
    *                                             it.
    */
  private final case class KeyGenParams(offsetBytes: Int, nullIntermediaryStructEqualsNullKeys: Boolean = false) {
    private val indexGenerator = Iterator.from(0)
    def nextAvailableNullBitIndex: Int = indexGenerator.next()
  }

  /** Definition given for a key. From those and the KeyGenParams we generate the KeyBuffer. */
  private final case class KeyBlueprint(name: String, path: List[String])

  def apply[LR[+T] <: LeftRight[T]](rawSchema: LR[StructType], keyFields: Seq[KeyDiffField[LR]], offsetBytes: Int = 0)(
      implicit lr: LeftRightFunctor[LR]
  ): LR[KeyBuffer] = {
    rawSchema
      // match left/right schema with its respective relativeRawPaths
      .zip[Seq[FieldPath]](lr.sequence(keyFields.map(_.relativeRawPath)))
      .map({
        case (schema, keysPath) =>
          val keyBlueprints = keysPath
            .map(_.pathFromRoot)
            .zip(keyFields.map(_.fullName))
            .map({ case (keyPath, name) => KeyBlueprint(name = name, path = keyPath) })
          val params = KeyGenParams(offsetBytes)
          val (keyWriters, keys) = generateNestedKeys(params, schema, keyBlueprints)
          new KeyBuffer(
            offsetBytes = offsetBytes,
            nullBitSetSize = params.nextAvailableNullBitIndex, // 0 indexed, so next value matches the size
            keyWriters = keyWriters.toArray,
            keyReaders = keys.toArray
          )
      })
  }

  /** Recursively generate the necessary KeyWriters and the Keys for reading it back. */
  private def generateNestedKeys(
      params: KeyGenParams,
      schema: StructType,
      blueprints: Seq[KeyBlueprint]
  ): (List[KeyWriter], List[KeyReader]) = {
    blueprints
      // For nested keys, the enclosing struct will only be retrieved once.
      .groupBy(_.path.head)
      .toSeq
      // This ensures deterministic key order (so that left and right have the same ordering)
      .sortBy(_._1)
      .map({
        case (parentField, keyBlueprints) =>
          val ordinal = schema.fieldIndex(parentField)
          schema(parentField).dataType match {
            case nestedSchema: StructType =>
              // Check if any key just point to the struct itself.
              if (keyBlueprints.exists(_.path.length == 1)) {
                throw new IllegalArgumentException(s"$parentField is a not valid key type (struct).")
              }
              val (nestedKeyWriters, nestedKeys) =
                generateNestedKeys(params, nestedSchema, keyBlueprints.map(k => k.copy(path = k.path.tail)))

              val keyWriter = {
                val nestedKeyWritersArray = nestedKeyWriters.toArray
                // If a null intermediary struct is equivalent to null keys, we just don't add a specific nullBit
                // for the struct, hence only the nullity of its keys is tracked.
                val maybeStructNullBitIndex =
                  if (params.nullIntermediaryStructEqualsNullKeys) None
                  else Some(params.nextAvailableNullBitIndex)
                // All keys are set to null in all cases, as this simplifies the reading of the key from the ByteBuffer.
                // We merge all generated positions and masks to limit the number of interaction with the ByteBuffer.
                val (positions, masks) = NullBitSet.combinedPosAndMak(
                  params.offsetBytes,
                  nestedKeys.map(_.nullBitIndex) ++ maybeStructNullBitIndex
                )
                val numFields = nestedSchema.length
                (input: SpecializedGetters, buffer: ByteBuffer) => {
                  val struct = input.getStruct(ordinal, numFields)
                  var i = 0
                  if (struct == null) {
                    while (i < positions.length) {
                      NullBitSet.setNullAt(buffer, positions(i), masks(i))
                      i += 1
                    }
                  } else {
                    while (i < nestedKeyWritersArray.length) {
                      nestedKeyWritersArray(i)(struct, buffer)
                      i += 1
                    }
                  }
                }
              }
              (keyWriter :: Nil, nestedKeys)
            case dataType =>
              if (keyBlueprints.exists(_.path.length > 1)) {
                throw new IllegalArgumentException(
                  s"Cannot retrieve nested key in $parentField (${dataType.simpleString})." +
                    s"Nested keys can only be retrieved in struct fields."
                )
              } else if (keyBlueprints.length > 1) {
                throw new IllegalArgumentException(s"Duplicate keys found for $parentField")
              }
              val rawKey = keyBlueprints.head
              val nullBitIndex = params.nextAvailableNullBitIndex
              (
                buildKeyWriter(params.offsetBytes, nullBitIndex, ordinal, dataType) :: Nil,
                buildKeyReader(rawKey.name, nullBitIndex, dataType) :: Nil
              )
          }
      })
      .foldRight[(List[KeyWriter], List[KeyReader])]((Nil, Nil))({
        // Flatten the keyWriters and keyReaders.
        case ((keyWriters, keyReaders), (allKeyWriters, allKeyReaders)) =>
          (keyWriters ::: allKeyWriters, keyReaders ::: allKeyReaders)
      })
  }

  private def buildKeyReader(name: String, nullBitIndex: Int, dataType: DataType): KeyReader = {
    KeyReader(
      name = name,
      nullBitIndex = nullBitIndex,
      getFromBuffer = dataType match {
        case ByteType    => _.get()
        case ShortType   => _.getShort()
        case IntegerType => _.getInt()
        case LongType    => _.getLong()
        case FloatType   => _.getFloat()
        case DoubleType  => _.getDouble()
        case DateType =>
          (buffer: ByteBuffer) => DateTimeExampleUtils.dateToString(buffer.getInt())
        case TimestampType =>
          (buffer: ByteBuffer) => DateTimeExampleUtils.timestampToString(buffer.getLong())
        case BooleanType =>
          (buffer: ByteBuffer) => buffer.get() == TRUE
        case StringType =>
          (buffer: ByteBuffer) =>
            val numBytes = buffer.getInt()
            val out = new Array[Byte](numBytes)
            buffer.get(out)
            UTF8String.fromBytes(out).toString
        case BinaryType =>
          (buffer: ByteBuffer) =>
            val numBytes = buffer.getInt()
            val out = new Array[Byte](numBytes)
            buffer.get(out)
            out
      }
    )
  }

  private def buildKeyWriter(offsetBytes: Int, nullBitIndex: Int, ordinal: Int, dataType: DataType): KeyWriter = {
    import NullBitSet.{posAndMask, setNullAt}
    val (pos, mask) = posAndMask(offsetBytes, nullBitIndex)
    dataType match {
      case ByteType =>
        (input: SpecializedGetters, buffer: ByteBuffer) =>
          if (input.isNullAt(ordinal)) setNullAt(buffer, pos, mask) else buffer.put(input.getByte(ordinal))
      case ShortType =>
        (input: SpecializedGetters, buffer: ByteBuffer) =>
          if (input.isNullAt(ordinal)) setNullAt(buffer, pos, mask) else buffer.putShort(input.getShort(ordinal))
      case IntegerType | DateType =>
        (input: SpecializedGetters, buffer: ByteBuffer) =>
          if (input.isNullAt(ordinal)) setNullAt(buffer, pos, mask) else buffer.putInt(input.getInt(ordinal))
      case LongType | TimestampType =>
        (input: SpecializedGetters, buffer: ByteBuffer) =>
          if (input.isNullAt(ordinal)) setNullAt(buffer, pos, mask) else buffer.putLong(input.getLong(ordinal))
      case FloatType =>
        (input: SpecializedGetters, buffer: ByteBuffer) =>
          if (input.isNullAt(ordinal)) setNullAt(buffer, pos, mask) else buffer.putFloat(input.getFloat(ordinal))
      case DoubleType =>
        (input: SpecializedGetters, buffer: ByteBuffer) =>
          if (input.isNullAt(ordinal)) setNullAt(buffer, pos, mask) else buffer.putDouble(input.getDouble(ordinal))
      case BooleanType =>
        (input: SpecializedGetters, buffer: ByteBuffer) => {
          if (input.isNullAt(ordinal)) setNullAt(buffer, pos, mask)
          else buffer.put(if (input.getBoolean(ordinal)) TRUE else FALSE)
        }
      case StringType =>
        (input: SpecializedGetters, buffer: ByteBuffer) => {
          val s = input.getUTF8String(ordinal)
          if (s == null) setNullAt(buffer, pos, mask)
          else {
            // s.writeTo won't raise a BufferOverflowException, it copies memory directly through
            // Unsafe, which implies segfault on error.
            if (buffer.remaining < (4 + s.numBytes)) throw new BufferOverflowException()
            buffer.putInt(s.numBytes)
            s.writeTo(buffer)
          }
        }
      case BinaryType =>
        (input: SpecializedGetters, buffer: ByteBuffer) => {
          val b = input.getBinary(ordinal)
          if (b == null) setNullAt(buffer, pos, mask)
          else {
            buffer.putInt(b.length)
            buffer.put(b)
          }
        }
      case _ =>
        throw new IllegalArgumentException("Only primitive types and Strings are supported for keys.")
    }
  }
}

/** All of the functions used to manipulate the null bit set. */
private object NullBitSet {
  def isNullAt(buffer: ByteBuffer, offsetBytes: Int, nullBitIndex: Int): Boolean = {
    val (pos, mask) = posAndMask(offsetBytes, nullBitIndex)
    (buffer.get(pos) & mask) == mask
  }

  @inline def setNullAt(buffer: ByteBuffer, pos: Int, mask: Byte): Unit = {
    buffer.put(pos, (mask | buffer.get(pos)).toByte)
  }

  /** Size in Bytes necessary for a NullBitSet of the given length. */
  def bytesSize(length: Int): Int = (length >> 3) + (if (length % 8 != 0) 1 else 0)

  /** Used to combine several positions and masks to avoid unnecessary calls as masks can overlap. */
  def combinedPosAndMak(offsetBytes: Int, nullBitIndices: Seq[Int]): (Array[Int], Array[Byte]) = {
    val (positions, masks) = nullBitIndices
      .map(posAndMask(offsetBytes, _))
      .groupBy(_._1)
      .map({
        case (pos, masks) => (pos, masks.map(_._2).reduce[Byte]({ case (a: Byte, b: Byte) => (a | b).toByte }))
      })
      .unzip
    (positions.toArray, masks.toArray)
  }

  def posAndMask(offsetBytes: Int, nullBitIndex: Int): (Int, Byte) = {
    val pos: Int = offsetBytes + (nullBitIndex >> 3)
    val mask: Byte = (1 << (nullBitIndex & 7)).toByte
    (pos, mask)
  }
}
