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

package com.criteo.deepdiff.test_cases

import com.criteo.deepdiff.KeyExample
import com.criteo.deepdiff.test_utils.{DatasetDiffsBuilder, DeepDiffSpec}

object ExplodedArraySpec {
  final case class Record(key: String, array: Seq[Nested])
  final case class Nested(key: String, value: Int)
}

/** Specific test related to exploded arrays */
final class ExplodedArraySpec extends DeepDiffSpec {
  import DeepDiffSpec._
  import ExplodedArraySpec._
  import com.criteo.deepdiff.test_utils.EasyKeyExample._

  val explodedConf = // language=HOCON
    """keys = ["key"]
      |exploded-arrays {
      |  array = { keys = ["key"] }
      |}
      |""".stripMargin

  "DeepDiff" should {

    "be order agnostic for exploded arrays" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .conf(explodedConf)
        .compare(
          left = Seq(
            Record("A", array = Seq(null, Nested("a", 1), null)),
            Record("B", array = Seq(Nested("a", 2), Nested("b", 3), Nested("c", 4)))
          ),
          right = Seq(
            Record("A", array = Seq(null, null, Nested("a", 1))),
            Record("B", array = Seq(Nested("c", 4), Nested("a", 2), Nested("b", 3)))
          )
        )
        .expect(
          DatasetDiffsBuilder[KeyExample, Product]()
            .record(identical = 2)
            .common("key", identical = 2)
            .common("array", identical = 2)
            .explodedArray(
              "array",
              DatasetDiffsBuilder[KeyExample, Product]()
                .record(identical = 4)
                .common("key", identical = 4)
                .common("value", identical = 4)
            )
        )
    }

    "count the number of nulls in exploded arrays" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .conf(explodedConf)
        .compare(
          left = Seq(
            Record("identical", array = Seq(null, null)),
            Record("less-null", array = Seq(null, null)),
            Record("more-null", array = Seq(null, null))
          ),
          right = Seq(
            Record("identical", array = Seq(null, null)),
            Record("less-null", array = Seq(null)),
            Record("more-null", array = Seq(null, null, null))
          )
        )
        .expect(
          DatasetDiffsBuilder[KeyExample, Product]()
            .kindOfDifferent(content = 2)
            .record(
              identical = 1,
              diffExamples = Seq(
                (Key("less-null"), Record("less-null", Seq(null, null)), Record("less-null", Seq(null))),
                (Key("more-null"), Record("more-null", Seq(null, null)), Record("more-null", Seq(null, null, null)))
              )
            )
            .common("key", identical = 3)
            .common(
              "array",
              identical = 1,
              diffExamples = Seq(
                (Key("less-null"), Seq(null, null), Seq(null)),
                (Key("more-null"), Seq(null, null), Seq(null, null, null))
              )
            )
            .explodedArray("array",
                           DatasetDiffsBuilder[KeyExample, Product]()
                             .record()
                             .common("key")
                             .common("value"))
        )
    }
  }
}
