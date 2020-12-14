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

private[deepdiff] final class FieldPath private (private val raw: List[String]) extends Serializable {
  lazy val fullName: String = raw.mkString(".")

  override def hashCode(): Int = fullName.hashCode

  override def equals(obj: Any): Boolean =
    obj match {
      case other: FieldPath => other.fullName == fullName
      case _                => false
    }

  def child(name: String): FieldPath = new FieldPath(raw :+ name)

  def virtual(suffix: String): String = s"$fullName ($suffix)"

  def pathFromRoot: List[String] = raw
  def pathFrom(parent: FieldPath): FieldPath = {
    require(parent.raw.length < raw.length)
    var parentPath = parent.raw
    var newPath = raw
    while (parentPath.nonEmpty) {
      assert(parentPath.head == newPath.head, s"$parentPath != $newPath")
      newPath = newPath.tail
      parentPath = parentPath.tail
    }
    new FieldPath(newPath)
  }
}

private[deepdiff] object FieldPath {
  val ROOT: FieldPath = new FieldPath(Nil)
  def apply(fullName: String) = new FieldPath(fullName.split('.').toList)
}
