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

object DeeplyNestedSpec {
  final case class BigRecord(key: String, record: Record, others: Seq[Record])
  final case class Record(key: String, nested: Nested, array: Seq[Nested])
  final case class Nested(key: String, value: Int)

  final case class BigRecord2(key: String, record: Record2, others: Seq[Record2])
  final case class Record2(key: String, nested: Nested2, array: Seq[Nested2])
  final case class Nested2(key: String, value: Int, second: java.lang.Double = null)
}

final class DeeplyNestedSpec extends DeepDiffSpec {
  import DeepDiffSpec._
  import DeeplyNestedSpec._
  import com.criteo.deepdiff.test_utils.EasyKeyExample._

  "DeepDiff" should {
    val left = Seq(
      BigRecord(
        "identical",
        record = Record("A", Nested("Aa", 1), Seq(Nested("Ab", 2), Nested("Ac", 3))),
        others = Seq(
          Record("B", null, Nil),
          Record("C", Nested("Ca", 4), Seq(Nested("Cb", 5)))
        )
      ),
      BigRecord(
        "different",
        record = Record("A", Nested("Aa", 1), Seq(Nested("Ab", 2), Nested("Ac", 3))),
        others = Seq(
          Record("B", null, Nil),
          Record("C", Nested("Ca", 4), Seq(Nested("Cb", 5)))
        )
      ),
      BigRecord(
        "left-only",
        record = Record("A", Nested("Aa", 1), Seq(Nested("Ab", 2), Nested("Ac", 3))),
        others = Seq(
          Record("B", null, Nil),
          Record("C", Nested("Ca", 4), Seq(Nested("Cb", 5)))
        )
      ),
      BigRecord(
        "right-only",
        record = Record("A", Nested("Aa", 1), Seq(Nested("Ab", 2), Nested("Ac", 3))),
        others = Seq(
          Record("B", null, Nil),
          Record("C", Nested("Ca", 4), Seq(Nested("Cb", 5)))
        )
      ),
      BigRecord(
        "right-only-second-field",
        record = Record("A", Nested("Aa", 1), Seq(Nested("Ab", 2), Nested("Ac", 3))),
        others = Seq(
          Record("B", null, Nil),
          Record("C", Nested("Ca", 4), Seq(Nested("Cb", 5)))
        )
      )
    )
    val right = Seq(
      BigRecord2(
        "identical",
        record = Record2("A", Nested2("Aa", 1), Seq(Nested2("Ab", 2), Nested2("Ac", 3))),
        others = Seq(
          Record2("B", null, Nil),
          Record2("C", Nested2("Ca", 4), Seq(Nested2("Cb", 5)))
        )
      ),
      BigRecord2(
        "different",
        record = Record2("A",
                         Nested2("Aa", 10), // <-- 1 vs 10
                         Seq(
                           Nested2("Ab", 2),
                           Nested2("Ac", 30) // <- 3 vs 30
                         )),
        others = Seq(
          Record2("B", null, Nil),
          Record2("C",
                  Nested2("Ca", 40), // <- 4 vs 40
                  Seq(Nested2("Cb", 50)) // <- 5 vs 50
          )
        )
      ),
      BigRecord2(
        "left-only",
        record = Record2("A", null, Seq(Nested2("Ac", 3))), // missing nested2 & 1st of array
        others = Seq(
          // missing B
          Record2("C", null, Seq(Nested2("Cb", 5))) // missing nested2
        )
      ),
      BigRecord2(
        "right-only",
        record =
          Record2("A", Nested2("Aa", 1), Seq(Nested2("Ab", 2), Nested2("Ac", 3), Nested2("Ad", 6))), // added in array
        others = Seq(
          Record2("B", Nested2("Ba", 7), Seq(Nested2("Bb", 8))), // added nested2 & array
          Record2("C", Nested2("Ca", 4), Seq(Nested2("Cb", 5)))
        )
      ),
      BigRecord2(
        "right-only-second-field",
        record = Record2("A", Nested2("Aa", 1, second = 1d), Seq(Nested2("Ab", 2), Nested2("Ac", 3))),
        others = Seq(
          Record2("B", null, Nil),
          Record2("C", Nested2("Ca", 4), Seq(Nested2("Cb", 5, second = 5d)))
        )
      )
    )
    val baseExpected =
      DatasetDiffsBuilder[KeyExample, Product]()
        .record(identical = 1, diffExamples = left.zip(right).drop(1).map({ case (l, r) => (Key(l.key), l, r) }))
        .common("key", identical = 5)
        .common("record",
                identical = 1,
                diffExamples = left.zip(right).drop(1).map({ case (l, r) => (Key(l.key), l.record, r.record) }))
        .common("others",
                identical = 1,
                diffExamples = left.zip(right).drop(1).map({ case (l, r) => (Key(l.key), l.others, r.others) }))
        .common("record.key", identical = 5)
        .common(
          "record.nested",
          identical = 2,
          diffExamples = Seq(
            (Key("different"), Nested("Aa", 1), Nested2("Aa", 10)),
            (Key("right-only-second-field"), Nested("Aa", 1), Nested2("Aa", 1, second = 1d))
          ),
          leftExamples = Seq(
            (Key("left-only"), Nested("Aa", 1))
          )
        )
        .common("record.nested.key", identical = 4)
        .common("record.nested.value", identical = 3, diffExamples = Seq((Key("different"), 1, 10)))
        .rightOnly("record.nested.second", identical = 3, rightExamples = Seq((Key("right-only-second-field"), 1d)))
        .common(
          "record.array",
          identical = 2,
          diffExamples = Seq(
            (Key("different"), Seq(Nested("Ab", 2), Nested("Ac", 3)), Seq(Nested2("Ab", 2), Nested2("Ac", 30))),
            (Key("left-only"), Seq(Nested("Ab", 2), Nested("Ac", 3)), Seq(Nested2("Ac", 3))),
            (Key("right-only"),
             Seq(Nested("Ab", 2), Nested("Ac", 3)),
             Seq(Nested2("Ab", 2), Nested2("Ac", 3), Nested2("Ad", 6)))
          )
        )

    "support deeply nested structures" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .testReversedLeftAndRight()
        .testPruned()
        .defaultConf()
        .compare(left, right)
        .expect(
          baseExpected
            .kindOfDifferent(content = 2, rightOnly = 2)
            .common(
              "record.array (struct)",
              identical = 7,
              diffExamples = Seq(
                (Key("different") >> Key(1), Nested("Ac", 3), Nested2("Ac", 30)),
                (Key("left-only") >> Key(0), Nested("Ab", 2), Nested2("Ac", 3))
              ),
              leftExamples = Seq((Key("left-only") >> Key(1), Nested("Ac", 3))),
              rightExamples = Seq((Key("right-only") >> Key(2), Nested2("Ad", 6)))
            )
            .common("record.array.key", identical = 8, diffExamples = Seq((Key("left-only") >> Key(0), "Ab", "Ac")))
            .common("record.array.value",
                    identical = 7,
                    diffExamples = Seq(
                      (Key("different") >> Key(1), 3, 30),
                      (Key("left-only") >> Key(0), 2, 3)
                    ))
            .rightOnly("record.array.second", identical = 9)
            .common(
              "others (struct)",
              identical = 5,
              diffExamples = Seq(
                (Key("different") >> Key(1),
                 Record("C", Nested("Ca", 4), Seq(Nested("Cb", 5))),
                 Record2("C", Nested2("Ca", 40), Seq(Nested2("Cb", 50)))),
                (Key("left-only") >> Key(0), Record2("B", null, Nil), Record2("C", null, Seq(Nested2("Cb", 5)))),
                (Key("right-only") >> Key(0),
                 Record2("B", null, Nil),
                 Record2("B", Nested2("Ba", 7), Seq(Nested2("Bb", 8)))),
                (Key("right-only-second-field") >> Key(1),
                 Record("C", Nested("Ca", 4), Seq(Nested("Cb", 5))),
                 Record2("C", Nested2("Ca", 4), Seq(Nested2("Cb", 5, second = 5d))))
              ),
              leftExamples = Seq(
                (Key("left-only") >> Key(1), Record("C", Nested("Ca", 4), Seq(Nested("Cb", 5))))
              )
            )
            .common("others.key", identical = 8, diffExamples = Seq((Key("left-only") >> Key(0), "B", "C")))
            .common(
              "others.nested",
              identical = 7,
              diffExamples = Seq(
                (Key("different") >> Key(1), Nested("Ca", 4), Nested2("Ca", 40))
              ),
              rightExamples = Seq(
                (Key("right-only") >> Key(0), Nested2("Ba", 7))
              )
            )
            .common("others.nested.key", identical = 4)
            .common("others.nested.value",
                    identical = 3,
                    diffExamples = Seq(
                      (Key("different") >> Key(1), 4, 40)
                    ))
            .rightOnly("others.nested.second", identical = 4)
            .common(
              "others.array",
              identical = 5,
              diffExamples = Seq(
                (Key("different") >> Key(1), Seq(Nested("Cb", 5)), Seq(Nested2("Cb", 50))),
                (Key("left-only") >> Key(0), Nil, Seq(Nested2("Cb", 5))),
                (Key("right-only") >> Key(0), Nil, Seq(Nested2("Bb", 8))),
                (Key("right-only-second-field") >> Key(1), Seq(Nested("Cb", 5)), Seq(Nested2("Cb", 5, second = 5d)))
              )
            )
            .common(
              "others.array (struct)",
              identical = 2,
              diffExamples = Seq(
                (Key("different") >> Key(1) >> Key(0), Nested("Cb", 5), Nested2("Cb", 50)),
                (Key("right-only-second-field") >> Key(1) >> Key(0), Nested("Cb", 5), Nested2("Cb", 5, second = 5d))
              ),
              rightExamples = Seq(
                (Key("left-only") >> Key(0) >> Key(0), Nested2("Cb", 5)),
                (Key("right-only") >> Key(0) >> Key(0), Nested2("Bb", 8))
              )
            )
            .common("others.array.key", identical = 4)
            .common("others.array.value",
                    identical = 3,
                    diffExamples = Seq(
                      (Key("different") >> Key(1) >> Key(0), 5, 50)
                    ))
            .rightOnly("others.array.second",
                       identical = 3,
                       rightExamples = Seq(
                         (Key("right-only-second-field") >> Key(1) >> Key(0), 5d)
                       )))
    }

    "support deeply nested structures within exploded arrays" when runningDeepDiffTestCase {
      DeepDiffTestCase
        .testReversedLeftAndRight()
        .testPruned()
        .conf( // language=HOCON
          """keys = ["key"]
            |exploded-arrays {
            |  others = { keys = ["key"] }
            |  "record.array" = { keys = ["key"] }
            |  "others.array" = { keys = ["key"] } 
            |}""".stripMargin)
        .compare(left, right)
        .expect(
          baseExpected
            .kindOfDifferent(content = 1, leftOnly = 1, rightOnly = 2)
            .explodedArray(
              "record.array",
              DatasetDiffsBuilder[KeyExample, Product]()
                .kindOfDifferent(content = 1)
                .record(
                  identical = 8,
                  diffExamples = Seq(
                    (Key("different") >> Key("Ac"), Nested("Ac", 3), Nested2("Ac", 30))
                  ),
                  leftExamples = Seq((Key("left-only") >> Key("Ab"), Nested("Ab", 2))),
                  rightExamples = Seq((Key("right-only") >> Key("Ad"), Nested2("Ad", 6)))
                )
                .common("key", identical = 9)
                .common("value", identical = 8, diffExamples = Seq((Key("different") >> Key("Ac"), 3, 30)))
                .rightOnly("second", identical = 9)
            )
            .explodedArray(
              "others",
              DatasetDiffsBuilder[KeyExample, Product]()
                .kindOfDifferent(content = 1, leftOnly = 1, rightOnly = 2)
                .record(
                  identical = 5,
                  diffExamples = Seq(
                    (Key("different") >> Key("C"),
                     Record("C", Nested("Ca", 4), Seq(Nested("Cb", 5))),
                     Record2("C", Nested2("Ca", 40), Seq(Nested2("Cb", 50)))),
                    (Key("left-only") >> Key("C"),
                     Record("C", Nested("Ca", 4), Seq(Nested("Cb", 5))),
                     Record2("C", null, Seq(Nested2("Cb", 5)))),
                    (Key("right-only") >> Key("B"),
                     Record2("B", null, Nil),
                     Record2("B", Nested2("Ba", 7), Seq(Nested2("Bb", 8)))),
                    (Key("right-only-second-field") >> Key("C"),
                     Record("C", Nested("Ca", 4), Seq(Nested("Cb", 5))),
                     Record2("C", Nested2("Ca", 4), Seq(Nested2("Cb", 5, second = 5d))))
                  ),
                  leftExamples = Seq(
                    (Key("left-only") >> Key("B"), Record("B", null, Nil))
                  )
                )
                .common("key", identical = 9)
                .common(
                  "nested",
                  identical = 6,
                  diffExamples = Seq(
                    (Key("different") >> Key("C"), Nested("Ca", 4), Nested2("Ca", 40))
                  ),
                  leftExamples = Seq(
                    (Key("left-only") >> Key("C"), Nested("Ca", 4))
                  ),
                  rightExamples = Seq(
                    (Key("right-only") >> Key("B"), Nested2("Ba", 7))
                  )
                )
                .common("nested.key", identical = 4)
                .common("nested.value",
                        identical = 3,
                        diffExamples = Seq(
                          (Key("different") >> Key("C"), 4, 40)
                        ))
                .rightOnly("nested.second", identical = 4)
                .common(
                  "array",
                  identical = 6,
                  diffExamples = Seq(
                    (Key("different") >> Key("C"), Seq(Nested("Cb", 5)), Seq(Nested2("Cb", 50))),
                    (Key("right-only") >> Key("B"), Nil, Seq(Nested2("Bb", 8))),
                    (Key("right-only-second-field") >> Key("C"),
                     Seq(Nested("Cb", 5)),
                     Seq(Nested2("Cb", 5, second = 5d)))
                  )
                )
            )
            .explodedArray(
              "others.array",
              DatasetDiffsBuilder[KeyExample, Product]()
                .kindOfDifferent(content = 1, rightOnly = 1)
                .record(
                  identical = 3,
                  diffExamples = Seq(
                    (Key("different") >> Key("C") >> Key("Cb"), Nested("Cb", 5), Nested2("Cb", 50)),
                    (Key("right-only-second-field") >> Key("C") >> Key("Cb"),
                     Nested("Cb", 5),
                     Nested2("Cb", 5, second = 5d))
                  ),
                  rightExamples = Seq(
                    (Key("right-only") >> Key("B") >> Key("Bb"), Nested2("Bb", 8))
                  )
                )
                .common("key", identical = 5)
                .common("value",
                        identical = 4,
                        diffExamples = Seq(
                          (Key("different") >> Key("C") >> Key("Cb"), 5, 50)
                        ))
                .rightOnly("second",
                           identical = 4,
                           rightExamples = Seq(
                             (Key("right-only-second-field") >> Key("C") >> Key("Cb"), 5d)
                           ))
            ))
    }
  }

}
