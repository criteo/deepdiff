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

import com.criteo.deepdiff._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class RawDiffPartSpec extends AnyWordSpec with Matchers {
  def identity[K, F]: RawToFriendlyDiffExample[K, F, K, F] =
    new RawToFriendlyDiffExample[K, F, K, F] {
      def rawToFriendlyKey(rawKey: K): K = rawKey
      def rawToFriendlyLeft(rawLeft: F): F = rawLeft
      def rawToFriendlyRight(rawRight: F): F = rawRight
    }

  def doubleRawToFriendlyExample: RawToFriendlyDiffExample[String, Int, String, String] =
    new RawToFriendlyDiffExample[String, Int, String, String] {
      def rawToFriendlyKey(rawKey: String): String = rawKey * 2
      def rawToFriendlyLeft(rawLeft: Int): String = (rawLeft * 2).toString
      def rawToFriendlyRight(rawRight: Int): String = (rawRight * 2).toString
    }

  "LeftRightRawDiffsPart" should {
    "build a LeftRightDiffs" when {
      "common" in {
        val x = CommonRawDiffsPart(
          identical = 111,
          different = RawDiffPart(222, List("a" -> DifferentExample(2, 22))),
          leftOnly = RawDiffPart(333, List("b" -> LeftOnlyExample(3))),
          rightOnly = RawDiffPart(444, List("c" -> RightOnlyExample(5)))
        )
        x.buildCommonDiff(identity[String, Int]) should be(
          CommonDiffs(
            identical = 111,
            different = Diff(222, Map("a" -> DifferentExample(2, 22))),
            leftOnly = Diff(333, Map("b" -> LeftOnlyExample(3))),
            rightOnly = Diff(444, Map("c" -> RightOnlyExample(5)))
          )
        )
        x.buildCommonDiff(doubleRawToFriendlyExample) should be(
          CommonDiffs(
            identical = 111,
            different = Diff(222, Map("aa" -> DifferentExample("4", "44"))),
            leftOnly = Diff(333, Map("bb" -> LeftOnlyExample("6"))),
            rightOnly = Diff(444, Map("cc" -> RightOnlyExample("10")))
          )
        )
      }
      "leftOnly" in {
        val x = LeftOnlyRawDiffsPart(
          nulls = 111,
          leftOnly = RawDiffPart(333, List("b" -> LeftOnlyExample(3)))
        )
        x.buildLeftOnlyDiff(identity[String, Int]) should be(
          LeftOnlyDiffs(
            nulls = 111,
            leftOnly = Diff(333, Map("b" -> LeftOnlyExample(3)))
          )
        )
        x.buildLeftOnlyDiff(doubleRawToFriendlyExample) should be(
          LeftOnlyDiffs(
            nulls = 111,
            leftOnly = Diff(333, Map("bb" -> LeftOnlyExample("6")))
          )
        )
      }
      "rightOnly" in {
        val x = RightOnlyRawDiffsPart(
          nulls = 111,
          rightOnly = RawDiffPart(444, List("c" -> RightOnlyExample(5)))
        )
        x.buildRightOnlyDiff(identity[String, Int]) should be(
          RightOnlyDiffs(
            nulls = 111,
            rightOnly = Diff(444, Map("c" -> RightOnlyExample(5)))
          )
        )
        x.buildRightOnlyDiff(doubleRawToFriendlyExample) should be(
          RightOnlyDiffs(
            nulls = 111,
            rightOnly = Diff(444, Map("cc" -> RightOnlyExample("10")))
          )
        )
      }
    }
  }

  "RecordRawDiffsPart" should {
    "build a RecordDiffs correctly" when {
      val recordRawToFriendly = new RawToFriendlyDiffExample[String, Map[String, Any], String, Map[String, Any]] {
        def rawToFriendlyKey(rawKey: String): String = rawKey * 3
        def rawToFriendlyLeft(rawLeft: Map[String, Any]): Map[String, Any] = rawLeft ++ Map("left" -> 2)
        def rawToFriendlyRight(rawRight: Map[String, Any]): Map[String, Any] = rawRight ++ Map("right" -> 2)
      }

      "common" in {
        val x = MatchedRecordsRawDiffsPart[String, Map[String, Any]](
          multipleMatches = RawDiffPart(
            101,
            List(
              "a" -> DifferentExample(Seq(Map("key" -> "a")), Seq(Map("key" -> "a"), Map("key" -> "a"))),
              "b" -> DifferentExample(Nil, Seq(Map("key" -> "b"), Map("key" -> "b"))),
              "c" -> DifferentExample(Seq(Map("key" -> "c"), Map("key" -> "c")), Nil)
            )
          ),
          recordDiffs = CommonRawDiffsPart(
            identical = 102,
            different =
              RawDiffPart(103, List("d" -> DifferentExample(Map("key" -> "d"), Map("key" -> "d", "field" -> 1)))),
            leftOnly = RawDiffPart(104, List("e" -> LeftOnlyExample(Map("key" -> "e", "field" -> 1)))),
            rightOnly = RawDiffPart(105, List("f" -> RightOnlyExample(Map("key" -> "f"))))
          ),
          kindOfDifferentRecords = KindOfDifferentRecords(106, 107, 108, 109),
          fieldsDiffs = Map(
            "field" -> CommonRawDiffsPart(
              identical = 111,
              different = RawDiffPart(222, List("a" -> DifferentExample(2, 22))),
              leftOnly = RawDiffPart(333, List("b" -> LeftOnlyExample(3))),
              rightOnly = RawDiffPart(444, List("c" -> RightOnlyExample(5)))
            ),
            "field2" -> CommonRawDiffsPart(
              identical = 1110,
              different = RawDiffPart(2220, List("a" -> DifferentExample(2, 22))),
              leftOnly = RawDiffPart(3330, List("b" -> LeftOnlyExample(3))),
              rightOnly = RawDiffPart(4440, List("c" -> RightOnlyExample(5)))
            ),
            "left" -> LeftOnlyRawDiffsPart(
              nulls = 11100,
              leftOnly = RawDiffPart(22200, List("a" -> LeftOnlyExample(77)))
            ),
            "right" -> RightOnlyRawDiffsPart(
              nulls = 111000,
              rightOnly = RawDiffPart(222000, List("a" -> RightOnlyExample(777)))
            )
          )
        )
        x.buildMatchedRecordsDiffs(
          multipleMatchesRawToFriendly = identity[String, Seq[Map[String, Any]]],
          recordRawToFriendly = identity[String, Map[String, Any]],
          rawToFriendlyByFullName = Map(
            "field" -> identity[String, Any],
            "field2" -> identity[String, Any],
            "left" -> identity[String, Any],
            "right" -> identity[String, Any]
          )
        ) should be(
          MatchedRecordsDiffs(
            multipleMatches = Diff(
              101,
              Map(
                "a" -> DifferentExample(Seq(Map("key" -> "a")), Seq(Map("key" -> "a"), Map("key" -> "a"))),
                "b" -> DifferentExample(Nil, Seq(Map("key" -> "b"), Map("key" -> "b"))),
                "c" -> DifferentExample(Seq(Map("key" -> "c"), Map("key" -> "c")), Nil)
              )
            ),
            recordDiffs = CommonDiffs(
              identical = 102,
              different = Diff(103, Map("d" -> DifferentExample(Map("key" -> "d"), Map("key" -> "d", "field" -> 1)))),
              leftOnly = Diff(104, Map("e" -> LeftOnlyExample(Map("key" -> "e", "field" -> 1)))),
              rightOnly = Diff(105, Map("f" -> RightOnlyExample(Map("key" -> "f"))))
            ),
            kindOfDifferentRecords = KindOfDifferentRecords(106, 107, 108, 109),
            fieldsDiffs = Map(
              "field" -> CommonDiffs(
                identical = 111,
                different = Diff(222, Map("a" -> DifferentExample(2, 22))),
                leftOnly = Diff(333, Map("b" -> LeftOnlyExample(3))),
                rightOnly = Diff(444, Map("c" -> RightOnlyExample(5)))
              ),
              "field2" -> CommonDiffs(
                identical = 1110,
                different = Diff(2220, Map("a" -> DifferentExample(2, 22))),
                leftOnly = Diff(3330, Map("b" -> LeftOnlyExample(3))),
                rightOnly = Diff(4440, Map("c" -> RightOnlyExample(5)))
              ),
              "left" -> LeftOnlyDiffs(
                nulls = 11100,
                leftOnly = Diff(22200, Map("a" -> LeftOnlyExample(77)))
              ),
              "right" -> RightOnlyDiffs(
                nulls = 111000,
                rightOnly = Diff(222000, Map("a" -> RightOnlyExample(777)))
              )
            )
          )
        )
        x.buildMatchedRecordsDiffs(
          multipleMatchesRawToFriendly =
            new RawToFriendlyDiffExample[String, Seq[Map[String, Any]], String, Seq[Map[String, Any]]] {
              def rawToFriendlyKey(rawKey: String): String = rawKey * 2
              def rawToFriendlyLeft(rawLeft: Seq[Map[String, Any]]): Seq[Map[String, Any]] =
                rawLeft.map(_ ++ Map("left" -> 1))
              def rawToFriendlyRight(rawRight: Seq[Map[String, Any]]): Seq[Map[String, Any]] =
                rawRight.map(_ ++ Map("right" -> 1))
            },
          recordRawToFriendly,
          rawToFriendlyByFullName = Map[String, RawToFriendlyDiffExample[String, Any, String, Any]](
            "field" -> identity[String, Any],
            "field2" -> doubleRawToFriendlyExample.asInstanceOf[RawToFriendlyDiffExample[String, Any, String, Any]],
            "left" -> identity[String, Any],
            "right" -> identity[String, Any]
          )
        ) should be(
          MatchedRecordsDiffs(
            multipleMatches = Diff(
              101,
              Map(
                "aa" -> DifferentExample(
                  Seq(Map("key" -> "a", "left" -> 1)),
                  Seq(Map("key" -> "a", "right" -> 1), Map("key" -> "a", "right" -> 1))
                ),
                "bb" -> DifferentExample(Nil, Seq(Map("key" -> "b", "right" -> 1), Map("key" -> "b", "right" -> 1))),
                "cc" -> DifferentExample(Seq(Map("key" -> "c", "left" -> 1), Map("key" -> "c", "left" -> 1)), Nil)
              )
            ),
            recordDiffs = CommonDiffs(
              identical = 102,
              different = Diff(
                103,
                Map(
                  "ddd" -> DifferentExample(
                    Map("key" -> "d", "left" -> 2),
                    Map("key" -> "d", "field" -> 1, "right" -> 2)
                  )
                )
              ),
              leftOnly = Diff(104, Map("eee" -> LeftOnlyExample(Map("key" -> "e", "field" -> 1, "left" -> 2)))),
              rightOnly = Diff(105, Map("fff" -> RightOnlyExample(Map("key" -> "f", "right" -> 2))))
            ),
            kindOfDifferentRecords = KindOfDifferentRecords(106, 107, 108, 109),
            fieldsDiffs = Map(
              "field" -> CommonDiffs(
                identical = 111,
                different = Diff(222, Map("a" -> DifferentExample(2, 22))),
                leftOnly = Diff(333, Map("b" -> LeftOnlyExample(3))),
                rightOnly = Diff(444, Map("c" -> RightOnlyExample(5)))
              ),
              "field2" -> CommonDiffs(
                identical = 1110,
                different = Diff(2220, Map("aa" -> DifferentExample("4", "44"))),
                leftOnly = Diff(3330, Map("bb" -> LeftOnlyExample("6"))),
                rightOnly = Diff(4440, Map("cc" -> RightOnlyExample("10")))
              ),
              "left" -> LeftOnlyDiffs(
                nulls = 11100,
                leftOnly = Diff(22200, Map("a" -> LeftOnlyExample(77)))
              ),
              "right" -> RightOnlyDiffs(
                nulls = 111000,
                rightOnly = Diff(222000, Map("a" -> RightOnlyExample(777)))
              )
            )
          )
        )
      }
      "leftOnly" in {
        val x = LeftOnlyRecordRawDiffsPart[String, Map[String, Any]](
          records = RawDiffPart(104, List("e" -> LeftOnlyExample(Map("key" -> "e", "field" -> 1)))),
          fields = Set("field")
        )
        x.buildLeftOnlyRecordDiffs(identity[String, Map[String, Any]]) should be(
          LeftOnlyRecordDiffs(
            records = Diff(104, Map("e" -> LeftOnlyExample(Map("key" -> "e", "field" -> 1)))),
            fields = Set("field")
          ))
        x.buildLeftOnlyRecordDiffs(recordRawToFriendly) should be(
          LeftOnlyRecordDiffs(
            records = Diff(104, Map("eee" -> LeftOnlyExample(Map("key" -> "e", "field" -> 1, "left" -> 2)))),
            fields = Set("field")
          ))
      }
      "rightOnly" in {
        val x = RightOnlyRecordRawDiffsPart[String, Map[String, Any]](
          records = RawDiffPart(105, List("f" -> RightOnlyExample(Map("key" -> "f")))),
          fields = Set("field")
        )
        x.buildRightOnlyRecordDiffs(identity[String, Map[String, Any]]) should be(
          RightOnlyRecordDiffs(
            records = Diff(105, Map("f" -> RightOnlyExample(Map("key" -> "f")))),
            fields = Set("field")
          ))
        x.buildRightOnlyRecordDiffs(recordRawToFriendly) should be(
          RightOnlyRecordDiffs(
            records = Diff(105, Map("fff" -> RightOnlyExample(Map("key" -> "f", "right" -> 2)))),
            fields = Set("field")
          ))

      }
    }
  }
}
