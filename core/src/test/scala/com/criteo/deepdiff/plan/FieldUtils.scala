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
import com.criteo.deepdiff.type_support.comparator.defaultEqualityParams
import com.criteo.deepdiff.utils._
import org.apache.spark.sql.types._

import scala.annotation.tailrec

private[plan] object FieldUtils {
  def explodedStruct(schema: StructType, name: String): StructType =
    schema(name).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]

  def commonKey(fullName: String): KeyDiffField[Common] = {
    val rawPath = FieldPath(fullName)
    KeyDiffField(fullName, Common(rawPath, rawPath))
  }

  def atomic(left: StructType, fullName: String, right: StructType): AtomicDiffField[Common] = {
    val leftField = field(left, fullName)
    val rightField = field(right, fullName)
    AtomicDiffField[Common](
      fullName,
      raw = Common(leftField, rightField),
      leftRightEqualityParams = defaultEqualityParams
    )
  }

  def atomic(left: StructType, fullName: String): AtomicDiffField[HasLeft] = {
    val leftField = field(left, fullName)
    AtomicDiffField[HasLeft](
      fullName,
      raw = OnlyLeft(leftField),
      leftRightEqualityParams = defaultEqualityParams
    )
  }

  def atomic(fullName: String, right: StructType): AtomicDiffField[HasRight] = {
    val rightField = field(right, fullName)
    AtomicDiffField[HasRight](
      fullName,
      raw = OnlyRight(rightField),
      leftRightEqualityParams = defaultEqualityParams
    )
  }

  def field(s: StructType, fullName: String): RawField = {
    val name = fullName.split('.').last.split(' ').head
    val parent = getDirectParent(s, fullName)
    RawField(parent.fieldIndex(name), parent(name))
  }

  def field(s: StructType, fullName: String, alias: String): RawField =
    field(s, fullName).copy(maybeAlias = Some(alias))

  private def getDirectParent(s: StructType, fullName: String): StructType =
    getDirectParent(s, fullName.split('.').toList)

  @tailrec private def getDirectParent(s: StructType, path: List[String]): StructType =
    path match {
      case _ :: Nil => s
      case head :: tail =>
        s(head).dataType match {
          case struct: StructType               => getDirectParent(struct, tail)
          case ArrayType(struct: StructType, _) => getDirectParent(struct, tail)
        }
      case _ => throw new IllegalStateException("path cannot be empty.")
    }
}
