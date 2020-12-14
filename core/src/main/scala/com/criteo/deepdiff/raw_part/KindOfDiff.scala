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

package com.criteo.deepdiff.raw_part

/** Simple Integer flag used to return the kind differences if any to the caller. */
private[deepdiff] final case class KindOfDiff private (value: Int) extends AnyVal {
  def |(other: KindOfDiff): KindOfDiff = KindOfDiff(value | other.value)
  def &(other: KindOfDiff): KindOfDiff = KindOfDiff(value & other.value)
  def unary_~ : KindOfDiff = KindOfDiff(~value)

  override def toString: String = {
    import KindOfDiff._
    if (this == NEUTRAL) "NEUTRAL"
    else {
      Map(
        "IDENTICAL" -> IDENTICAL,
        "DIFFERENT" -> DIFFERENT,
        "LEFT_ONLY" -> LEFT_ONLY,
        "RIGHT_ONLY" -> RIGHT_ONLY,
        "MULTIPLE_MATCHES" -> MULTIPLE_MATCHES
      ).flatMap({
        case (name, flag) => if ((flag & this) == flag) Some(name) else None
      }).mkString("KindOfDiff(", " | ", ")")
    }
  }
}

private[deepdiff] object KindOfDiff {
  // used when initializing a mutable flag.
  val NEUTRAL: KindOfDiff = KindOfDiff(0)
  val IDENTICAL: KindOfDiff = KindOfDiff(1)
  val DIFFERENT: KindOfDiff = KindOfDiff(2)
  val LEFT_ONLY: KindOfDiff = KindOfDiff(4)
  val RIGHT_ONLY: KindOfDiff = KindOfDiff(8)
  // Used to avoid considering exploded arrays with only multiple matches as identical.
  val MULTIPLE_MATCHES: KindOfDiff = KindOfDiff(16)
}
