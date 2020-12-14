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

package com.criteo.deepdiff.utils

import scala.language.higherKinds

/** Represents something that can have either a left and/or right value. Used typically
  * for field attributes which can have one those three states. With the type class
  * [[LeftRightFunctor]] we ensure when working with those attributes that we stay consistent.
  * A left-only field cannot have right-only example builder.
  *
  * This avoids a lot of boilerplate.
  */
private[deepdiff] sealed trait LeftRight[+A] extends Serializable {
  def maybeLeft: Option[A]
  def maybeRight: Option[A]
  def any: A = maybeLeft.orElse(maybeRight).get
  def forall(f: A => Boolean): Boolean
  def exists(f: A => Boolean): Boolean
}

private[deepdiff] sealed trait HasLeft[+A] extends LeftRight[A] {
  def left: A
  def maybeLeft: Option[A] = Some(left)
}

private[deepdiff] sealed trait HasRight[+A] extends LeftRight[A] {
  def right: A
  def maybeRight: Option[A] = Some(right)
}

private[deepdiff] object HasRight {
  def apply[T](right: T): HasRight[T] = OnlyRight(right)
}

private[deepdiff] object HasLeft {
  def apply[T](left: T): HasLeft[T] = OnlyLeft(left)
}

private[deepdiff] final case class OnlyLeft[+A](left: A) extends HasLeft[A] {
  def maybeRight: Option[A] = None
  def forall(f: A => Boolean): Boolean = f(left)
  def exists(f: A => Boolean): Boolean = f(left)
}

private[deepdiff] final case class OnlyRight[+A](right: A) extends HasRight[A] {
  def maybeLeft: Option[A] = None
  def forall(f: A => Boolean): Boolean = f(right)
  def exists(f: A => Boolean): Boolean = f(right)
}

private[deepdiff] final case class Common[+A](left: A, right: A) extends HasLeft[A] with HasRight[A] {
  def forall(f: A => Boolean): Boolean = f(left) && f(right)
  def exists(f: A => Boolean): Boolean = f(left) || f(right)
}

private[deepdiff] object Common {
  def twin[A](x: A): Common[A] = Common(x, x)
}

private[deepdiff] trait LeftRightFunctor[LR[+T] <: LeftRight[T]] extends Serializable {
  def map[A, B](x: LR[A])(f: A => B): LR[B]
  def mapSeparately[A, B](x: LR[A])(f: Either[A, A] => B): LR[B]
  def zip[A, B](x: LR[A])(y: LR[B]): LR[(A, B)]
  def zipCommon[A, B](x: LR[A])(y: Common[B]): LR[(A, B)]
  def sequence[A, B](x: Seq[LeftRight[A]]*): LR[Seq[A]]
}

private[deepdiff] object LeftRightFunctor {
  implicit val leftFunctor: LeftRightFunctor[HasLeft] = new LeftRightFunctor[HasLeft] {
    def map[A, B](x: HasLeft[A])(f: A => B): HasLeft[B] = HasLeft(f(x.left))
    def mapSeparately[A, B](x: HasLeft[A])(f: Either[A, A] => B): HasLeft[B] = HasLeft(f(Left(x.left)))
    def zip[A, B](x: HasLeft[A])(y: HasLeft[B]): HasLeft[(A, B)] = HasLeft((x.left, y.left))
    def zipCommon[A, B](x: HasLeft[A])(y: Common[B]): HasLeft[(A, B)] = zip(x)(y)
    def sequence[A, B](x: Seq[LeftRight[A]]*): HasLeft[Seq[A]] = HasLeft(x.flatten.flatMap(_.maybeLeft))
  }

  implicit val rightFunctor: LeftRightFunctor[HasRight] = new LeftRightFunctor[HasRight] {
    def map[A, B](x: HasRight[A])(f: A => B): HasRight[B] = HasRight(f(x.right))
    def mapSeparately[A, B](x: HasRight[A])(f: Either[A, A] => B): HasRight[B] = HasRight(f(Right(x.right)))
    def zip[A, B](x: HasRight[A])(y: HasRight[B]): HasRight[(A, B)] = HasRight((x.right, y.right))
    def zipCommon[A, B](x: HasRight[A])(y: Common[B]): HasRight[(A, B)] = zip(x)(y)
    def sequence[A, B](x: Seq[LeftRight[A]]*): HasRight[Seq[A]] = HasRight(x.flatten.flatMap(_.maybeRight))
  }

  implicit val commonFunctor: LeftRightFunctor[Common] = new LeftRightFunctor[Common] {
    def map[A, B](x: Common[A])(f: A => B): Common[B] = Common[B](f(x.left), f(x.right))
    def mapSeparately[A, B](x: Common[A])(f: Either[A, A] => B): Common[B] =
      Common(f(Left(x.left)), f(Right(x.right)))
    def zip[A, B](x: Common[A])(y: Common[B]): Common[(A, B)] =
      Common((x.left, y.left), (x.right, y.right))
    def zipCommon[A, B](x: Common[A])(y: Common[B]): Common[(A, B)] = zip(x)(y)
    def sequence[A, B](x: Seq[LeftRight[A]]*): Common[Seq[A]] = {
      val flatX = x.flatten
      Common(flatX.flatMap(_.maybeLeft), flatX.flatMap(_.maybeRight))
    }
  }
}

private[deepdiff] object LeftRight {
  def apply[T](maybeLeft: Option[T], maybeRight: Option[T]): LeftRight[T] =
    (maybeLeft, maybeRight) match {
      case (Some(l), Some(r)) => Common(l, r)
      case (Some(l), None)    => OnlyLeft(l)
      case (None, Some(r))    => OnlyRight(r)
      case (None, None) =>
        throw new IllegalArgumentException("Either left or right must be not None.")
    }

  implicit class InnerLeftRightFunctor[A, LR[+T] <: LeftRight[T]](x: LR[A])(implicit functor: LeftRightFunctor[LR]) {
    def map[B](f: A => B): LR[B] = functor.map(x)(f)
    def zip[B](y: LR[B]): LR[(A, B)] = functor.zip[A, B](x)(y)
    def zipCommon[B](y: Common[B]): LR[(A, B)] = functor.zipCommon[A, B](x)(y)
    def mapSeparately[B](f: Either[A, A] => B): LR[B] = functor.mapSeparately(x)(f)
  }
}
