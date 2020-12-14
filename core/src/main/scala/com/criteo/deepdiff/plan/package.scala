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

package com.criteo.deepdiff

package object plan {
  // Utilities used when matching LeftRight to imitate Scala's MatchError as we can't use the
  // usual match { case } for this.
  private[plan] object matchError {
    def noFields[T](): T = throw new IllegalStateException(s"Empty pruned schema case is not handled")
    def common[T](x: Any): T = throw new IllegalStateException(s"Common case is not handled")
    def leftOnly[T](x: Any): T = throw new IllegalStateException(s"LeftOnly case is not handled")
    def rightOnly[T](x: Any): T = throw new IllegalStateException(s"RightOnly case is not handled")
    def common2[T](x: Any, y: Any): T = throw new IllegalStateException(s"Common case is not handled")
    def leftOnly2[T](x: Any, y: Any): T = throw new IllegalStateException(s"LeftOnly case is not handled")
    def rightOnly2[T](x: Any, y: Any): T = throw new IllegalStateException(s"RightOnly case is not handled")
  }
}
