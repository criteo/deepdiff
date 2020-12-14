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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class LeftRightSpec extends AnyFlatSpec with Matchers {
  trait LeftRightBuilder[+LR[+T] <: LeftRight[T]] {
    def expects[T](a: T, b: T): LR[T] = apply(a, b)
    def apply[T](a: T, b: T): LR[T]
  }

  def coherentLeftRight[LR[+T] <: LeftRight[T]](f: LeftRightBuilder[LR])(implicit lr: LeftRightFunctor[LR]): Unit = {
    it should "support several transformations while keep the same type" in {
      f(3, 7).map(_ * 5) should be(f.expects(15, 35))
      f(3, 7).mapSeparately({
        case Left(l)  => l * 5
        case Right(r) => r * 11
      }) should be(f.expects(15, 77))
      f(3, 7).zip(f(11, 13)) should be(f.expects((3, 11), (7, 13)))
    }

    it should "support zipping with common" in {
      f(3, 7).zipCommon(Common(11, 13)) should be(f.expects((3, 11), (7, 13)))
    }

    it should "support forall/exists" in {
      f(3, 7).forall(_ < 10) should be(true)
      f(3, 7).forall(_ < 5) && f(7, 3).forall(_ < 5) should be(false)
      f(3, 7).exists(_ < 5) || f(7, 3).exists(_ < 5) should be(true)
      f(3, 7).exists(_ < 2) should be(false)
    }
  }

  "OnlyLeft" should behave like coherentLeftRight[HasLeft](new LeftRightBuilder[OnlyLeft] {
    override def apply[T](a: T, b: T): OnlyLeft[T] = OnlyLeft(a)
  })

  "Common/HasLeft" should behave like coherentLeftRight[HasLeft](new LeftRightBuilder[HasLeft] {
    override def expects[T](a: T, b: T): HasLeft[T] = HasLeft(a)
    override def apply[T](a: T, b: T): Common[T] = Common(a, b)
  })

  "Common" should behave like coherentLeftRight[Common](new LeftRightBuilder[Common] {
    override def apply[T](a: T, b: T): Common[T] = Common(a, b)
  })

  "Common/HasRight" should behave like coherentLeftRight[HasRight](new LeftRightBuilder[HasRight] {
    override def expects[T](a: T, b: T): HasRight[T] = HasRight(b)
    override def apply[T](a: T, b: T): Common[T] = Common(a, b)
  })

  "OnlyRight" should behave like coherentLeftRight[HasRight](new LeftRightBuilder[OnlyRight] {
    override def apply[T](a: T, b: T): OnlyRight[T] = OnlyRight(b)
  })

  "LeftRightFunctor" should "be able to extract all similar LeftRight from a sequence" in {
    val s = Seq(
      Common(3, 5),
      OnlyLeft(7),
      OnlyRight(11)
    )
    LeftRightFunctor.commonFunctor.sequence(s) should be(Common(Seq(3, 7), Seq(5, 11)))
    LeftRightFunctor.leftFunctor.sequence(s) should be(OnlyLeft(Seq(3, 7)))
    LeftRightFunctor.rightFunctor.sequence(s) should be(OnlyRight(Seq(5, 11)))
  }

  "LeftRight" should "have a coherent any/maybeLeft/maybeRight" in {
    Common(3, 5).maybeLeft should be(Some(3))
    Common(3, 5).maybeRight should be(Some(5))
    Set(Common(3, 5).any).subsetOf(Set(3, 5)) should be(true)
    OnlyLeft(3).maybeLeft should be(Some(3))
    OnlyLeft(3).maybeRight should be(None)
    OnlyLeft(3).any should be(3)
    OnlyRight(5).maybeLeft should be(None)
    OnlyRight(5).maybeRight should be(Some(5))
    OnlyRight(5).any should be(5)
  }
}
