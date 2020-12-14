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

import com.criteo.deepdiff.{DifferentExample, KindOfDifferentRecords, LeftOnlyExample, RightOnlyExample}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class RawDiffPartMergerSpec extends AnyFlatSpec with Matchers {
  val record = Map.empty[String, Any]

  it should "be able to merge two empty DatasetRawDiffPart" in {
    new RawDiffPartMerger(maxExamples = 1000000)
      .merge(DatasetRawDiffsPart.empty, DatasetRawDiffsPart.empty) should be(DatasetRawDiffsPart.empty)
  }

  it should "merge with an empty DatasetRawDiffPart" in {
    val x = DatasetRawDiffsPart(
      root = MatchedRecordsRawDiffsPart[String, Map[String, Any]](
        multipleMatches = RawDiffPart(5, List("5" -> DifferentExample(Seq(Map("x" -> 5), Map("x" -> 5)), Nil))),
        kindOfDifferentRecords = KindOfDifferentRecords(1, 2, 3, 4),
        recordDiffs = CommonRawDiffsPart(
          identical = 6,
          different = RawDiffPart(7, List("7" -> DifferentExample(Map("x" -> 7), Map("x" -> 77)))),
          leftOnly = RawDiffPart(8, List("8" -> LeftOnlyExample(Map("x" -> 8)))),
          rightOnly = RawDiffPart(9, List("9" -> RightOnlyExample(Map("x" -> 9))))
        ),
        fieldsDiffs = Map(
          "common" -> CommonRawDiffsPart(identical = 10,
                                         different = RawDiffPart(11, Nil),
                                         leftOnly = RawDiffPart(12, Nil),
                                         rightOnly = RawDiffPart(13, Nil)),
          "left" -> LeftOnlyRawDiffsPart(nulls = 14, leftOnly = RawDiffPart(15, Nil)),
          "right" -> RightOnlyRawDiffsPart(nulls = 16, rightOnly = RawDiffPart(17, Nil))
        )
      ),
      explodedArrays = Map(
        "left" -> LeftOnlyRecordRawDiffsPart(
          records = RawDiffPart(19, List("19" -> LeftOnlyExample(Map("y" -> 19)))),
          fields = Set("fieldL")
        )
      )
    )
    val merger = new RawDiffPartMerger(maxExamples = 1000000)
    merger.merge(DatasetRawDiffsPart.empty, x) should be(x)
    merger.merge(x, DatasetRawDiffsPart.empty) should be(x)
  }

  it should "merge root.multipleMatches" in {
    new RawDiffPartMerger(maxExamples = 1000000)
      .merge(
        a = DatasetRawDiffsPart(
          root = MatchedRecordsRawDiffsPart[String, Map[String, Any]](
            multipleMatches = RawDiffPart(
              count = 10,
              examples = List(
                "a" -> DifferentExample(Nil, Seq(Map("key" -> "a"))),
                "a2" -> DifferentExample(Seq(Map("key" -> "a2")), Nil)
              )
            ),
            kindOfDifferentRecords = KindOfDifferentRecords(0, 0, 0, 0),
            recordDiffs = CommonRawDiffsPart.empty,
            fieldsDiffs = Map.empty
          ),
          explodedArrays = Map.empty
        ),
        b = DatasetRawDiffsPart(
          root = MatchedRecordsRawDiffsPart[String, Map[String, Any]](
            multipleMatches = RawDiffPart(
              count = 1000,
              examples = List("a3" -> DifferentExample(Seq(Map("key" -> "a")), Seq(Map("key" -> "a2"))))
            ),
            kindOfDifferentRecords = KindOfDifferentRecords(0, 0, 0, 0),
            recordDiffs = CommonRawDiffsPart.empty,
            fieldsDiffs = Map.empty
          ),
          explodedArrays = Map.empty
        )
      ) should be(
      DatasetRawDiffsPart(
        root = MatchedRecordsRawDiffsPart[String, Map[String, Any]](
          multipleMatches = RawDiffPart(
            count = 1010,
            examples = List(
              "a2" -> DifferentExample(Seq(Map("key" -> "a2")), Nil),
              "a" -> DifferentExample(Nil, Seq(Map("key" -> "a"))),
              "a3" -> DifferentExample(Seq(Map("key" -> "a")), Seq(Map("key" -> "a2")))
            )
          ),
          kindOfDifferentRecords = KindOfDifferentRecords(0, 0, 0, 0),
          recordDiffs = CommonRawDiffsPart.empty,
          fieldsDiffs = Map.empty
        ),
        explodedArrays = Map.empty
      ))
  }
  it should "merge root.kindOfDifferentRecords" in {
    new RawDiffPartMerger(maxExamples = 1000000)
      .merge(
        a = DatasetRawDiffsPart(
          root = MatchedRecordsRawDiffsPart[String, Map[String, Any]](
            multipleMatches = RawDiffPart.empty,
            kindOfDifferentRecords = KindOfDifferentRecords(1, 2, 3, 4),
            recordDiffs = CommonRawDiffsPart.empty,
            fieldsDiffs = Map.empty
          ),
          explodedArrays = Map.empty
        ),
        b = DatasetRawDiffsPart(
          root = MatchedRecordsRawDiffsPart[String, Map[String, Any]](
            multipleMatches = RawDiffPart.empty,
            kindOfDifferentRecords = KindOfDifferentRecords(40, 30, 20, 10),
            recordDiffs = CommonRawDiffsPart.empty,
            fieldsDiffs = Map.empty
          ),
          explodedArrays = Map.empty
        )
      ) should be(
      DatasetRawDiffsPart(
        root = MatchedRecordsRawDiffsPart[String, Map[String, Any]](
          multipleMatches = RawDiffPart.empty,
          kindOfDifferentRecords = KindOfDifferentRecords(41, 32, 23, 14),
          recordDiffs = CommonRawDiffsPart.empty,
          fieldsDiffs = Map.empty
        ),
        explodedArrays = Map.empty
      ))
  }

  it should "merge root.recordDiff" in {
    new RawDiffPartMerger(maxExamples = 1000000)
      .merge(
        a = DatasetRawDiffsPart(
          root = MatchedRecordsRawDiffsPart[String, Map[String, Any]](
            multipleMatches = RawDiffPart.empty,
            kindOfDifferentRecords = KindOfDifferentRecords(0, 0, 0, 0),
            recordDiffs = CommonRawDiffsPart(
              identical = 11,
              different = RawDiffPart(
                count = 12,
                examples =
                  List("b" -> DifferentExample(Map("key" -> "b", "field" -> 2), Map("key" -> "b", "field" -> 20)))
              ),
              leftOnly = RawDiffPart(
                count = 13,
                examples = List(
                  "c" -> LeftOnlyExample(Map("key" -> "c")),
                  "c2" -> LeftOnlyExample(record),
                  "c3" -> LeftOnlyExample(record)
                )
              ),
              rightOnly = RawDiffPart(
                count = 14,
                examples = List("d" -> RightOnlyExample(record), "d2" -> RightOnlyExample(Map("key" -> "d2")))
              )
            ),
            fieldsDiffs = Map.empty
          ),
          explodedArrays = Map.empty
        ),
        b = DatasetRawDiffsPart(
          root = MatchedRecordsRawDiffsPart[String, Map[String, Any]](
            multipleMatches = RawDiffPart.empty,
            kindOfDifferentRecords = KindOfDifferentRecords(0, 0, 0, 0),
            recordDiffs = CommonRawDiffsPart(
              identical = 1100,
              different = RawDiffPart(
                count = 1200,
                examples =
                  List("bb" -> DifferentExample(Map("key" -> "bb", "field" -> 2), Map("key" -> "bb", "field" -> 20)))
              ),
              leftOnly = RawDiffPart(
                count = 1300,
                examples = List(
                  "cc" -> LeftOnlyExample(Map("key" -> "c")),
                  "cc2" -> LeftOnlyExample(record),
                  "cc3" -> LeftOnlyExample(record)
                )
              ),
              rightOnly = RawDiffPart(
                count = 1400,
                examples = List("dd" -> RightOnlyExample(record), "dd2" -> RightOnlyExample(Map("key" -> "d2")))
              )
            ),
            fieldsDiffs = Map.empty
          ),
          explodedArrays = Map.empty
        )
      ) should be(
      DatasetRawDiffsPart(
        root = MatchedRecordsRawDiffsPart[String, Map[String, Any]](
          multipleMatches = RawDiffPart.empty,
          kindOfDifferentRecords = KindOfDifferentRecords(0, 0, 0, 0),
          recordDiffs = CommonRawDiffsPart(
            identical = 1111,
            different = RawDiffPart(
              count = 1212,
              examples = List(
                "b" -> DifferentExample(Map("key" -> "b", "field" -> 2), Map("key" -> "b", "field" -> 20)),
                "bb" -> DifferentExample(Map("key" -> "bb", "field" -> 2), Map("key" -> "bb", "field" -> 20))
              )
            ),
            leftOnly = RawDiffPart(
              count = 1313,
              examples = List(
                "c3" -> LeftOnlyExample(record),
                "c2" -> LeftOnlyExample(record),
                "c" -> LeftOnlyExample(Map("key" -> "c")),
                "cc" -> LeftOnlyExample(Map("key" -> "c")),
                "cc2" -> LeftOnlyExample(record),
                "cc3" -> LeftOnlyExample(record)
              )
            ),
            rightOnly = RawDiffPart(
              count = 1414,
              examples = List(
                "d2" -> RightOnlyExample(Map("key" -> "d2")),
                "d" -> RightOnlyExample(record),
                "dd" -> RightOnlyExample(record),
                "dd2" -> RightOnlyExample(Map("key" -> "d2"))
              )
            )
          ),
          fieldsDiffs = Map.empty
        ),
        explodedArrays = Map.empty
      ))
  }

  it should "merge root.fieldDiffs" in {
    new RawDiffPartMerger(maxExamples = 1000000)
      .merge(
        a = DatasetRawDiffsPart(
          root = MatchedRecordsRawDiffsPart[String, Map[String, Any]](
            multipleMatches = RawDiffPart.empty,
            kindOfDifferentRecords = KindOfDifferentRecords(0, 0, 0, 0),
            recordDiffs = CommonRawDiffsPart.empty,
            fieldsDiffs = Map(
              "field" -> CommonRawDiffsPart(
                identical = 21,
                different = RawDiffPart(
                  count = 22,
                  examples = List("e" -> DifferentExample(1, 2), "e2" -> DifferentExample(3, 4))
                ),
                leftOnly = RawDiffPart(
                  count = 23,
                  examples = List("f" -> LeftOnlyExample(3), "f2" -> LeftOnlyExample(7), "f3" -> LeftOnlyExample(11))
                ),
                rightOnly = RawDiffPart(count = 24, examples = List("g" -> RightOnlyExample(4)))
              ),
              "left-only-field" -> LeftOnlyRawDiffsPart(
                nulls = 31,
                leftOnly = RawDiffPart(32, List("h" -> LeftOnlyExample(33)))
              ),
              "right-only-field" -> RightOnlyRawDiffsPart(
                nulls = 41,
                rightOnly = RawDiffPart(42, List("i" -> RightOnlyExample(43)))
              )
            )
          ),
          explodedArrays = Map.empty
        ),
        b = DatasetRawDiffsPart(
          root = MatchedRecordsRawDiffsPart[String, Map[String, Any]](
            multipleMatches = RawDiffPart.empty,
            kindOfDifferentRecords = KindOfDifferentRecords(0, 0, 0, 0),
            recordDiffs = CommonRawDiffsPart.empty,
            fieldsDiffs = Map(
              "field" -> CommonRawDiffsPart(
                identical = 2100,
                different = RawDiffPart(
                  count = 2200,
                  examples = List("ee" -> DifferentExample(1, 2), "ee2" -> DifferentExample(3, 4))
                ),
                leftOnly = RawDiffPart(
                  count = 2300,
                  examples = List("ff" -> LeftOnlyExample(3), "ff2" -> LeftOnlyExample(7), "ff3" -> LeftOnlyExample(11))
                ),
                rightOnly = RawDiffPart(count = 2400, examples = List("gg" -> RightOnlyExample(4)))
              ),
              "left-only-field" -> LeftOnlyRawDiffsPart(
                nulls = 3100,
                leftOnly = RawDiffPart(3200, List("hh" -> LeftOnlyExample(33)))
              ),
              "right-only-field" -> RightOnlyRawDiffsPart(
                nulls = 4100,
                rightOnly = RawDiffPart(4200, List("ii" -> RightOnlyExample(43)))
              )
            )
          ),
          explodedArrays = Map.empty
        )
      ) should be(
      DatasetRawDiffsPart(
        root = MatchedRecordsRawDiffsPart[String, Map[String, Any]](
          multipleMatches = RawDiffPart.empty,
          kindOfDifferentRecords = KindOfDifferentRecords(0, 0, 0, 0),
          recordDiffs = CommonRawDiffsPart.empty,
          fieldsDiffs = Map(
            "field" -> CommonRawDiffsPart(
              identical = 2121,
              different = RawDiffPart(
                count = 2222,
                examples = List(
                  "e2" -> DifferentExample(3, 4),
                  "e" -> DifferentExample(1, 2),
                  "ee" -> DifferentExample(1, 2),
                  "ee2" -> DifferentExample(3, 4)
                )
              ),
              leftOnly = RawDiffPart(
                count = 2323,
                examples = List(
                  "f3" -> LeftOnlyExample(11),
                  "f2" -> LeftOnlyExample(7),
                  "f" -> LeftOnlyExample(3),
                  "ff" -> LeftOnlyExample(3),
                  "ff2" -> LeftOnlyExample(7),
                  "ff3" -> LeftOnlyExample(11)
                )
              ),
              rightOnly = RawDiffPart(
                count = 2424,
                examples = List(
                  "g" -> RightOnlyExample(4),
                  "gg" -> RightOnlyExample(4)
                )
              )
            ),
            "left-only-field" -> LeftOnlyRawDiffsPart(
              nulls = 3131,
              leftOnly = RawDiffPart(3232, List("h" -> LeftOnlyExample(33), "hh" -> LeftOnlyExample(33)))
            ),
            "right-only-field" -> RightOnlyRawDiffsPart(
              nulls = 4141,
              rightOnly = RawDiffPart(4242, List("i" -> RightOnlyExample(43), "ii" -> RightOnlyExample(43)))
            )
          )
        ),
        explodedArrays = Map.empty
      )
    )
  }

  it should "merge explodedArrays" in {
    new RawDiffPartMerger(maxExamples = 1000)
      .merge(
        a = DatasetRawDiffsPart(
          root = MatchedRecordsRawDiffsPart[String, Map[String, Any]](
            multipleMatches = RawDiffPart.empty,
            kindOfDifferentRecords = KindOfDifferentRecords(0, 0, 0, 0),
            recordDiffs = CommonRawDiffsPart.empty,
            fieldsDiffs = Map.empty
          ),
          explodedArrays = Map(
            "common" -> MatchedRecordsRawDiffsPart[String, Map[String, Any]](
              multipleMatches = RawDiffPart(5, List("5" -> DifferentExample(Seq(Map("x" -> 5), Map("x" -> 5)), Nil))),
              kindOfDifferentRecords = KindOfDifferentRecords(1, 2, 3, 4),
              recordDiffs = CommonRawDiffsPart(
                identical = 6,
                different = RawDiffPart(7, List("7" -> DifferentExample(Map("x" -> 7), Map("x" -> 77)))),
                leftOnly = RawDiffPart(8, List("8" -> LeftOnlyExample(Map("x" -> 8)))),
                rightOnly = RawDiffPart(9, List("9" -> RightOnlyExample(Map("x" -> 9))))
              ),
              fieldsDiffs = Map(
                "common" -> CommonRawDiffsPart(identical = 10,
                                               different = RawDiffPart(11, Nil),
                                               leftOnly = RawDiffPart(12, Nil),
                                               rightOnly = RawDiffPart(13, Nil)),
                "left" -> LeftOnlyRawDiffsPart(nulls = 14, leftOnly = RawDiffPart(15, Nil)),
                "right" -> RightOnlyRawDiffsPart(nulls = 16, rightOnly = RawDiffPart(17, Nil))
              )
            ),
            "left" -> LeftOnlyRecordRawDiffsPart(
              records = RawDiffPart(19, List("19" -> LeftOnlyExample(Map("y" -> 19)))),
              fields = Set("fieldL")
            ),
            "right" -> RightOnlyRecordRawDiffsPart(
              records = RawDiffPart(21, List("21" -> RightOnlyExample(Map("z" -> 21)))),
              fields = Set("fieldR")
            )
          )
        ),
        b = DatasetRawDiffsPart(
          root = MatchedRecordsRawDiffsPart[String, Map[String, Any]](
            multipleMatches = RawDiffPart.empty,
            kindOfDifferentRecords = KindOfDifferentRecords(0, 0, 0, 0),
            recordDiffs = CommonRawDiffsPart.empty,
            fieldsDiffs = Map.empty
          ),
          explodedArrays = Map(
            "common" -> MatchedRecordsRawDiffsPart[String, Map[String, Any]](
              multipleMatches =
                RawDiffPart(50, List("50" -> DifferentExample(Seq(Map("x" -> 50), Map("x" -> 50)), Nil))),
              kindOfDifferentRecords = KindOfDifferentRecords(10, 20, 30, 40),
              recordDiffs = CommonRawDiffsPart(
                identical = 60,
                different = RawDiffPart(70, List("70" -> DifferentExample(Map("x" -> 70), Map("x" -> 770)))),
                leftOnly = RawDiffPart(80, List("80" -> LeftOnlyExample(Map("x" -> 80)))),
                rightOnly = RawDiffPart(90, List("90" -> RightOnlyExample(Map("x" -> 90))))
              ),
              fieldsDiffs = Map(
                "common" -> CommonRawDiffsPart(identical = 100,
                                               different = RawDiffPart(110, Nil),
                                               leftOnly = RawDiffPart(120, Nil),
                                               rightOnly = RawDiffPart(130, Nil)),
                "left" -> LeftOnlyRawDiffsPart(nulls = 140, leftOnly = RawDiffPart(150, Nil)),
                "right" -> RightOnlyRawDiffsPart(nulls = 160, rightOnly = RawDiffPart(170, Nil))
              )
            ),
            "left" -> LeftOnlyRecordRawDiffsPart(
              records = RawDiffPart(190, List("190" -> LeftOnlyExample(Map("y" -> 190)))),
              fields = Set("fieldL")
            ),
            "right" -> RightOnlyRecordRawDiffsPart(
              records = RawDiffPart(210, List("210" -> RightOnlyExample(Map("z" -> 210)))),
              fields = Set("fieldR")
            )
          )
        )
      ) should be(
      DatasetRawDiffsPart(
        root = MatchedRecordsRawDiffsPart[String, Map[String, Any]](
          multipleMatches = RawDiffPart.empty,
          kindOfDifferentRecords = KindOfDifferentRecords(0, 0, 0, 0),
          recordDiffs = CommonRawDiffsPart.empty,
          fieldsDiffs = Map.empty
        ),
        explodedArrays = Map(
          "common" -> MatchedRecordsRawDiffsPart[String, Map[String, Any]](
            multipleMatches = RawDiffPart(55,
                                          List(
                                            "5" -> DifferentExample(Seq(Map("x" -> 5), Map("x" -> 5)), Nil),
                                            "50" -> DifferentExample(Seq(Map("x" -> 50), Map("x" -> 50)), Nil)
                                          )),
            kindOfDifferentRecords = KindOfDifferentRecords(11, 22, 33, 44),
            recordDiffs = CommonRawDiffsPart(
              identical = 66,
              different = RawDiffPart(77,
                                      List(
                                        "7" -> DifferentExample(Map("x" -> 7), Map("x" -> 77)),
                                        "70" -> DifferentExample(Map("x" -> 70), Map("x" -> 770))
                                      )),
              leftOnly = RawDiffPart(88,
                                     List(
                                       "8" -> LeftOnlyExample(Map("x" -> 8)),
                                       "80" -> LeftOnlyExample(Map("x" -> 80))
                                     )),
              rightOnly = RawDiffPart(99,
                                      List(
                                        "9" -> RightOnlyExample(Map("x" -> 9)),
                                        "90" -> RightOnlyExample(Map("x" -> 90))
                                      ))
            ),
            fieldsDiffs = Map(
              "common" -> CommonRawDiffsPart(identical = 110,
                                             different = RawDiffPart(121, Nil),
                                             leftOnly = RawDiffPart(132, Nil),
                                             rightOnly = RawDiffPart(143, Nil)),
              "left" -> LeftOnlyRawDiffsPart(nulls = 154, leftOnly = RawDiffPart(165, Nil)),
              "right" -> RightOnlyRawDiffsPart(nulls = 176, rightOnly = RawDiffPart(187, Nil))
            )
          ),
          "left" -> LeftOnlyRecordRawDiffsPart(
            records = RawDiffPart(209,
                                  List(
                                    "19" -> LeftOnlyExample(Map("y" -> 19)),
                                    "190" -> LeftOnlyExample(Map("y" -> 190))
                                  )),
            fields = Set("fieldL")
          ),
          "right" -> RightOnlyRecordRawDiffsPart(
            records = RawDiffPart(231,
                                  List(
                                    "21" -> RightOnlyExample(Map("z" -> 21)),
                                    "210" -> RightOnlyExample(Map("z" -> 210))
                                  )),
            fields = Set("fieldR")
          )
        )
      ))
  }

  it should "keep only max examples" in {
    new RawDiffPartMerger(maxExamples = 2)
      .merge(
        a = DatasetRawDiffsPart(
          root = MatchedRecordsRawDiffsPart[String, Map[String, Any]](
            multipleMatches = RawDiffPart(
              count = 10,
              examples = List(
                "a" -> DifferentExample(Nil, Seq(Map("key" -> "a"))),
                "a2" -> DifferentExample(Seq(Map("key" -> "a2")), Nil)
              )
            ),
            kindOfDifferentRecords = KindOfDifferentRecords(
              hasDifferentNotNullField = 1,
              hasOnlyLeftOnlyFields = 2,
              hasOnlyRightOnlyFields = 3,
              hasOnlyLeftAndRightOnlyFields = 4
            ),
            recordDiffs = CommonRawDiffsPart(
              identical = 11,
              different = RawDiffPart(
                count = 12,
                examples = List(
                  "b" -> DifferentExample(Map("key" -> "b", "field" -> 2), Map("key" -> "b", "field" -> 20)),
                  "b2" -> DifferentExample(Map("key" -> "b2", "field" -> 2), Map("key" -> "b2", "field" -> 20))
                )
              ),
              leftOnly = RawDiffPart(
                count = 13,
                examples = List("c" -> LeftOnlyExample(Map("key" -> "c")), "c2" -> LeftOnlyExample(record))
              ),
              rightOnly = RawDiffPart(
                count = 1400,
                examples = List("d" -> RightOnlyExample(record), "d2" -> RightOnlyExample(Map("key" -> "d2")))
              )
            ),
            fieldsDiffs = Map(
              "field" -> CommonRawDiffsPart(
                identical = 21,
                different = RawDiffPart(
                  count = 22,
                  examples = List("e" -> DifferentExample(1, 2), "e2" -> DifferentExample(3, 4))
                ),
                leftOnly =
                  RawDiffPart(count = 2300, examples = List("f" -> LeftOnlyExample(3), "f2" -> LeftOnlyExample(7))),
                rightOnly = RawDiffPart(count = 1, examples = List("g" -> RightOnlyExample(4)))
              )
            )
          ),
          explodedArrays = Map.empty
        ),
        b = DatasetRawDiffsPart(
          root = MatchedRecordsRawDiffsPart[String, Map[String, Any]](
            multipleMatches = RawDiffPart(
              count = 1,
              examples = List("a3" -> DifferentExample(Seq(Map("key" -> "a")), Seq(Map("key" -> "a2"))))
            ),
            kindOfDifferentRecords = KindOfDifferentRecords(
              hasDifferentNotNullField = 100,
              hasOnlyLeftOnlyFields = 200,
              hasOnlyRightOnlyFields = 300,
              hasOnlyLeftAndRightOnlyFields = 400
            ),
            recordDiffs = CommonRawDiffsPart(
              identical = 1100,
              different = RawDiffPart(
                count = 1,
                examples =
                  List("bb" -> DifferentExample(Map("key" -> "bb", "field" -> 2), Map("key" -> "bb", "field" -> 20)))
              ),
              leftOnly = RawDiffPart(
                count = 1300,
                examples = List("cc" -> LeftOnlyExample(Map("key" -> "c")), "cc2" -> LeftOnlyExample(record))
              ),
              rightOnly = RawDiffPart(
                count = 14,
                examples = List("dd" -> RightOnlyExample(record), "dd2" -> RightOnlyExample(Map("key" -> "d2")))
              )
            ),
            fieldsDiffs = Map(
              "field" -> CommonRawDiffsPart(
                identical = 2100,
                different = RawDiffPart(
                  count = 2200,
                  examples = List("ee" -> DifferentExample(1, 2), "ee2" -> DifferentExample(3, 4))
                ),
                leftOnly =
                  RawDiffPart(count = 23, examples = List("ff" -> LeftOnlyExample(3), "ff2" -> LeftOnlyExample(7))),
                rightOnly = RawDiffPart(count = 1, examples = List("gg" -> RightOnlyExample(4)))
              )
            )
          ),
          explodedArrays = Map.empty
        )
      ) should be(
      DatasetRawDiffsPart(
        root = MatchedRecordsRawDiffsPart[String, Map[String, Any]](
          multipleMatches = RawDiffPart(
            count = 11,
            examples = List(
              "a" -> DifferentExample(Nil, Seq(Map("key" -> "a"))),
              "a2" -> DifferentExample(Seq(Map("key" -> "a2")), Nil)
            )
          ),
          kindOfDifferentRecords = KindOfDifferentRecords(
            hasDifferentNotNullField = 101,
            hasOnlyLeftOnlyFields = 202,
            hasOnlyRightOnlyFields = 303,
            hasOnlyLeftAndRightOnlyFields = 404
          ),
          recordDiffs = CommonRawDiffsPart(
            identical = 1111,
            different = RawDiffPart(
              count = 13,
              examples = List(
                "b" -> DifferentExample(Map("key" -> "b", "field" -> 2), Map("key" -> "b", "field" -> 20)),
                "b2" -> DifferentExample(Map("key" -> "b2", "field" -> 2), Map("key" -> "b2", "field" -> 20))
              )
            ),
            leftOnly = RawDiffPart(
              count = 1313,
              examples = List("cc" -> LeftOnlyExample(Map("key" -> "c")), "cc2" -> LeftOnlyExample(record))
            ),
            rightOnly = RawDiffPart(
              count = 1414,
              examples = List(
                "d" -> RightOnlyExample(record),
                "d2" -> RightOnlyExample(Map("key" -> "d2"))
              )
            )
          ),
          fieldsDiffs = Map(
            "field" -> CommonRawDiffsPart(
              identical = 2121,
              different = RawDiffPart(
                count = 2222,
                examples = List("ee" -> DifferentExample(1, 2), "ee2" -> DifferentExample(3, 4))
              ),
              leftOnly = RawDiffPart(
                count = 2323,
                examples = List(
                  "f" -> LeftOnlyExample(3),
                  "f2" -> LeftOnlyExample(7)
                )
              ),
              rightOnly = RawDiffPart(
                count = 2,
                examples = List(
                  "gg" -> RightOnlyExample(4),
                  "g" -> RightOnlyExample(4)
                )
              )
            )
          )
        ),
        explodedArrays = Map.empty
      )
    )
  }
}
