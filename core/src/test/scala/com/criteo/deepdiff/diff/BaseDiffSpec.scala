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

package com.criteo.deepdiff.diff

import com.criteo.deepdiff.KeyExample
import com.criteo.deepdiff.config.DeepDiffConfig
import com.criteo.deepdiff.test_utils.{Checker, DatasetDiffsBuilder, EasyKeyExample}

final class BaseDiffSpec extends DFDatasetDiffsBuilderSpec {
  import EasyKeyExample._
  import com.criteo.deepdiff.test_utils.SparkUtils._

  it should "work with simple types" in {
    val leftA = LeftRow(1, 2L, 3f, 4d, "A", true, Seq(11, 12), Map("x" -> 10), Seq(null), (32, "xy", 47d))
    val rightA = RightRow(1, 21L, 3f, 4d, "A", true, Seq(111, 12), Map("x2" -> 10), Seq.empty, (32, "xy", 47d))

    val leftB = LeftRow(10,
                        20L,
                        30f,
                        40d,
                        "B",
                        true,
                        Seq(110, 120, 130, null),
                        Map("z" -> null, "x" -> 30, "y" -> 100),
                        Seq((27f, "27"), (49f, "49")),
                        (100, "s4", 12d))
    val rightB = RightRow(10,
                          20L,
                          30f,
                          40d,
                          "B",
                          true,
                          Seq(110, 120, 130, null),
                          Map("y" -> 100, "x" -> 30, "z" -> null),
                          Seq((27f, "27"), (49f, "49")),
                          (100, "s3", null))

    val leftC = LeftRow(10,
                        11L,
                        12f,
                        null,
                        "C",
                        false,
                        Seq.empty,
                        Map.empty,
                        Seq((13f, "13"), (39f, "39"), (23f, "23"), null, null, (null, null)),
                        (1, "", 2d))
    val rightC = RightRow(null,
                          11L,
                          12f,
                          13d,
                          "C",
                          false,
                          Seq.empty,
                          Map.empty,
                          Seq((130f, "13"), (39f, "390"), null, (56f, "56"), null, (2f, "2")),
                          null)
    val (leftD, rightD) =
      DummyRow.leftAndRight(37, 28L, 39f, 49d, "D", false, Seq(13), Map("h" -> 17), Seq.empty, (2, "yx", 3d))

    Checker.check(
      result = buildDiff(
        DeepDiffConfig(keys = Set("string")),
        dummyLeftSchema,
        dummyRightSchema,
        rows = Seq(
          (leftA, rightA),
          (leftB, rightB),
          (leftC, rightC),
          (leftD, rightD)
        ).map({
          case (l, r) => (l.asSpark, r.asSpark)
        })
      ),
      expected = DatasetDiffsBuilder[KeyExample, Map[String, Any]]()
        .schema(dummyLeftSchema, dummyLeftSchema)
        .kindOfDifferent(content = 3)
        .record(
          identical = 1,
          diffExamples = Seq(
            (Key("string" -> "A"), leftA.asExample, rightA.asExample),
            (Key("string" -> "B"), leftB.asExample, rightB.asExample),
            (Key("string" -> "C"), leftC.asExample, rightC.asExample)
          )
        )
        .common("string", identical = 4)
        .common("int", identical = 3, leftExamples = Seq((Key("string" -> "C"), 10)))
        .common("float", identical = 4)
        .common("double", identical = 3, rightExamples = Seq((Key("string" -> "C"), 13d)))
        .common("boolean", identical = 4)
        .common("array", identical = 3, diffExamples = Seq((Key("string" -> "A"), Seq(11, 12), Seq(111, 12))))
        .common("map", identical = 3, diffExamples = Seq((Key("string" -> "A"), Map("x" -> 10), Map("x2" -> 10))))
        .common(
          "exploded_array",
          identical = 2,
          diffExamples = Seq(
            (Key("string" -> "A"), Seq(null), Nil),
            (
              Key("string" -> "C"),
              Seq(Map("a1" -> 13f, "a2" -> "13"),
                  Map("a1" -> 39f, "a2" -> "39"),
                  Map("a1" -> 23f, "a2" -> "23"),
                  null,
                  null,
                  Map("a1" -> null, "a2" -> null)),
              Seq(Map("a1" -> 130f, "a2" -> "13"),
                  Map("a1" -> 39f, "a2" -> "390"),
                  null,
                  Map("a1" -> 56f, "a2" -> "56"),
                  null,
                  Map("a1" -> 2f, "a2" -> "2"))
            )
          )
        )
        .common(
          "exploded_array (struct)",
          identical = 3,
          diffExamples = Seq(
            (Key("string" -> "C") >> Key(0), Map("a1" -> 13f, "a2" -> "13"), Map("a1" -> 130f, "a2" -> "13")),
            (Key("string" -> "C") >> Key(1), Map("a1" -> 39f, "a2" -> "39"), Map("a1" -> 39f, "a2" -> "390")),
            (Key("string" -> "C") >> Key(5), Map("a1" -> null, "a2" -> null), Map("a1" -> 2f, "a2" -> "2"))
          ),
          leftExamples = Seq((Key("string" -> "C") >> Key(2), Map("a1" -> 23f, "a2" -> "23"))),
          rightExamples = Seq((Key("string" -> "C") >> Key(3), Map("a1" -> 56f, "a2" -> "56")))
        )
        .common(
          "exploded_array.a1",
          identical = 3,
          diffExamples = Seq((Key("string" -> "C") >> Key(0), 13f, 130f)),
          rightExamples = Seq((Key("string" -> "C") >> Key(5), 2f))
        )
        .common(
          "exploded_array.a2",
          identical = 3,
          diffExamples = Seq((Key("string" -> "C") >> Key(1), "39", "390")),
          rightExamples = Seq((Key("string" -> "C") >> Key(5), "2"))
        )
        .common(
          "struct",
          identical = 2,
          leftExamples = Seq((Key("string" -> "C"), leftC.asExample("struct"))),
          diffExamples = Seq((Key("string" -> "B"), leftB.asExample("struct"), rightB.asExample("struct")))
        )
        .common("struct.s1", identical = 3)
        .common("struct.s2", identical = 2, diffExamples = Seq((Key("string" -> "B"), "s4", "s3")))
        .common("struct.s3", identical = 2, leftExamples = Seq((Key("string" -> "B"), 12d)))
        .common("long", identical = 3, diffExamples = Seq((Key("string" -> "A"), 2L, 21L)))
    )
  }
}
