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

import com.criteo.deepdiff.plan.field.{Field, FieldPath, MergedField}
import com.criteo.deepdiff.utils._

import scala.reflect.runtime.universe._

/** We cannot know whether the DiffSchema will be Common/HasLeft/HasRight before actually building it.
  * As we gather all the key fields during its construction, we accept any kind of fields. Only at the
  * end we check that everything we've seen so far is consistent with the DiffSchema.
  */
private[plan] final class KeyDiffFieldsCollector(root: Field[LeftRight], keys: Set[FieldPath]) {
  private var commonKeys: List[KeyDiffField[Common]] = Nil
  private var leftOnlyKeys: List[KeyDiffField[HasLeft]] = Nil
  private var rightOnlyKeys: List[KeyDiffField[HasRight]] = Nil
  private val keyFullNames = keys.map(_.fullName)

  def addIfKey[LR[+T] <: LeftRight[T]](node: MergedField[LR])(implicit lr: LeftRightFunctor[LR]): Unit = {
    val relativeFullName = node.path.pathFrom(root.path).fullName
    if (keyFullNames.contains(relativeFullName)) {
      node.matchLeftRight(
        common = { commonNode =>
          val Common(l, r) = commonNode.rawField.map(_.dataType)
          assert(l == r, s"Key '$relativeFullName' has two different types: $l and $r")
          commonKeys ::= KeyDiffField(
            relativeFullName,
            relativeRawPath = Common(
              left = commonNode.rawPath.left.pathFrom(root.rawPath.maybeLeft.get),
              right = commonNode.rawPath.right.pathFrom(root.rawPath.maybeRight.get)
            )
          )
        },
        leftOnly = { leftOnlyNode =>
          leftOnlyKeys ::= KeyDiffField(
            relativeFullName,
            relativeRawPath = leftOnlyNode.rawPath.map(_.pathFrom(root.rawPath.maybeLeft.get))
          )
        },
        rightOnly = { rightOnlyNode =>
          rightOnlyKeys ::= KeyDiffField(
            relativeFullName,
            relativeRawPath = rightOnlyNode.rawPath.map(_.pathFrom(root.rawPath.maybeRight.get))
          )
        }
      )
    }
  }

  def resultAs[LR[+T] <: LeftRight[T]](implicit tag: WeakTypeTag[LR[_]]): Seq[KeyDiffField[LR]] = {
    def assertAllKeysPresent(keyFields: Seq[KeyDiffField[LeftRight]], message: String): Unit = {
      val foundKeyFullNames = keyFields.map(_.fullName).toSet
      assert(foundKeyFullNames == keyFullNames, s"$message (${keyFullNames.diff(foundKeyFullNames).mkString(", ")})")
    }
    val foundKeys = (tag.tpe match {
      case t if t <:< weakTypeOf[Common[_]] =>
        assert(leftOnlyKeys.isEmpty)
        assert(rightOnlyKeys.isEmpty)
        assertAllKeysPresent(commonKeys, "Missing common keys.")
        commonKeys
      case t if t <:< weakTypeOf[HasLeft[_]] =>
        assert(commonKeys.isEmpty)
        assert(rightOnlyKeys.isEmpty)
        assertAllKeysPresent(leftOnlyKeys, "Missing left keys.")
        leftOnlyKeys
      case t if t <:< weakTypeOf[HasRight[_]] =>
        assert(commonKeys.isEmpty)
        assert(leftOnlyKeys.isEmpty)
        assertAllKeysPresent(rightOnlyKeys, "Missing right keys.")
        rightOnlyKeys
      case _ => throw new IllegalStateException(s"${tag.tpe} is not supported")
    }).asInstanceOf[Seq[KeyDiffField[LR]]]
    foundKeys
  }

  /** Used for sanity checks */
  def isEmpty: Boolean = commonKeys.isEmpty && leftOnlyKeys.isEmpty && rightOnlyKeys.isEmpty
}
