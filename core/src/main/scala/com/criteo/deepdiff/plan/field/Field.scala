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

package com.criteo.deepdiff.plan.field

import com.criteo.deepdiff.plan.matchError
import com.criteo.deepdiff.utils._

import scala.language.higherKinds

/** Used to represent a field when traversing the left and right schemas. */
private[plan] sealed trait Field[+LR[+T] <: LeftRight[T]] {

  /** Path of a field, as seen in the final report. */
  def path: FieldPath

  /** Raw path of the field in left and/or right schemas. */
  def rawPath: LR[FieldPath]
}

private[plan] object Field {
  implicit class FieldNodeChild[LR[+T] <: LeftRight[T]](parent: Field[LR])(implicit lr: LeftRightFunctor[LR]) {

    /** Used this method is the only way to create another FieldNode. */
    def child[LRB[+T] <: LeftRight[T]](name: String, field: LRB[RawField])(implicit
        lrb: LeftRightFunctor[LRB]
    ): MergedField[LRB] =
      MergedField(
        parent.path.child(name),
        field
          .map(_.structField.name)
          .mapSeparately({
            case Left(name)  => parent.rawPath.maybeLeft.get.child(name)
            case Right(name) => parent.rawPath.maybeRight.get.child(name)
          }),
        field
      )
  }
}

/** Root Node, from which all others should be created. */
private[plan] object RootField extends Field[Common] {
  val path: FieldPath = FieldPath.ROOT
  val rawPath: Common[FieldPath] = Common(FieldPath.ROOT, FieldPath.ROOT)

  val fullName: String = path.fullName
  val name: String = fullName
}

/** Can only be created by calling child() */
private[plan] final case class MergedField[+LR[+T] <: LeftRight[T]] private[field] (
    path: FieldPath,
    rawPath: LR[FieldPath],
    rawField: LR[RawField]
) extends Field[LR] {

  /** Imitates Scala's pattern matching as it can't be done directly. */
  def matchLeftRight[T](
      common: MergedField[Common] => T = matchError.common _,
      leftOnly: MergedField[HasLeft] => T = matchError.leftOnly _,
      rightOnly: MergedField[HasRight] => T = matchError.rightOnly _
  ): T = {
    rawPath match {
      case _: Common[FieldPath]    => common(this.asInstanceOf[MergedField[Common]])
      case _: OnlyLeft[FieldPath]  => leftOnly(this.asInstanceOf[MergedField[HasLeft]])
      case _: OnlyRight[FieldPath] => rightOnly(this.asInstanceOf[MergedField[HasRight]])
    }
  }
}
