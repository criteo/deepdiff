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

object EmptySpec {
  final case class Record(key: String, value: java.lang.Float = null)
}

final class EmptySpec extends DeepDiffSpec {
  import DeepDiffSpec._
  import EmptySpec._
  import com.criteo.deepdiff.test_utils.EasyKeyExample._

  "Deep Diff" should {
    "support not reporting any examples at all" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .conf( // language=HOCON
          """keys = ["key"]
            |max-examples = 0
            |""".stripMargin)
        .compare(
          left = Seq(
            Record("identical"),
            Record("different", value = 2f),
            Record("left-only", value = 2f),
            Record("right-only"),
            Record("right")
          ),
          right = Seq(
            Record("identical"),
            Record("different", value = 1f),
            Record("left-only"),
            Record("right-only", value = 1f),
            Record("left")
          )
        )
        .expect(
          DatasetDiffsBuilder[KeyExample, Product]()
            .kindOfDifferent(content = 1, leftOnly = 1, rightOnly = 1)
            .record(identical = 1, different = 3, leftOnly = 1, rightOnly = 1)
            .common("key", identical = 4)
            .common("value", identical = 1, different = 1, leftOnly = 1, rightOnly = 1)
        )
    }

    "support comparing with an empty side" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .testReversedLeftAndRight()
        .defaultConf()
        .compare(
          left = Seq(
            Record("identical")
          ),
          right = Seq.empty[Record]
        )
        .expect(
          DatasetDiffsBuilder[KeyExample, Product]()
            .record(leftExamples = Seq((Key("identical"), Record("identical"))))
            .common("key")
            .common("value")
        )
    }

    "support comparing two empty datasets" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .defaultConf()
        .compare(
          left = Seq.empty[Record],
          right = Seq.empty[Record]
        )
        .expect(
          DatasetDiffsBuilder[KeyExample, Product]()
            .common("key")
            .common("value")
        )
    }
  }
}
