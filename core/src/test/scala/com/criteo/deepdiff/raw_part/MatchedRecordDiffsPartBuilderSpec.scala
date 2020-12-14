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

import com.criteo.deepdiff.test_utils.{Checker, DatasetDiffsBuilder}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object MatchedRecordDiffsPartBuilderSpec {
  sealed trait Pointer[T] {
    def get: T
  }

  final case class FixedPointer[T](obj: T) extends Pointer[T] {
    def get: T = obj
  }

  /** Mimicking Spark UnsafeRow iterators.
    * Iterator which elements can only be retrieved once and are only valid as long as the iterator
    * is not changed. So one has to be extremely careful when manipulating it. Spark's UnsafeRow
    * Iterators have the same behavior as they re-use the same UnsafeRow. Whenever hasNext is called
    * this UnsafeRow points to new data. So you can only call hasNext when you don't need the previous
    * element anymore.
    */
  final case class UnsafeIterator[T](private val elements: Seq[T]) extends Iterator[Pointer[T]] {
    private val nullPointer = new NullPointer[T]
    private var cursor = -1
    private var current: Pointer[T] = nullPointer

    override def hasNext: Boolean = {
      if (cursor < elements.length) {
        if (current != nullPointer) {
          true
        } else if (cursor < elements.length - 1) {
          cursor += 1
          current = new ElementPointer(cursor)
          true
        } else {
          false
        }
      } else {
        false
      }
    }

    override def next(): Pointer[T] = {
      if (current == nullPointer) {
        throw new IllegalStateException("Element has already been retrieved.")
      } else {
        val res = current
        current = nullPointer
        res
      }
    }

    private final class NullPointer[T] extends Pointer[T] {
      def get: T = {
        throw new IllegalStateException("")
      }
    }

    private final class ElementPointer(private val position: Int) extends Pointer[T] {
      def get: T = {
        if (cursor != position) {
          throw new IllegalStateException("Cursor has already moved. You cannot access this element anymore.")
        }
        elements(position)
      }
    }
  }

  final case class Record(name: String = null, value: java.lang.Integer = null)

  final case class DummyRecordRawDiffsAccumulator(maxDiffExamples: Int)
      extends RecordRawDiffsAccumulator[String, Pointer[Record], String, Record] {
    import KindOfDiff._
    type R = Pointer[Record]

    private val nameDiffs = new CommonRawDiffsPartBuilder[String, R, String, Any](
      maxDiffExamples = maxDiffExamples,
      rawDiffExampleBuilder = new RawDiffExampleBuilder[String, R, String, Any] {
        def asRawKeyExample(key: String): String = key
        def asRawLeftExample(left: R): Any = left.get.name
        def asRawRightExample(right: R): Any = right.get.name
      }
    )

    private val valueDiffs = new CommonRawDiffsPartBuilder[String, R, String, Any](
      maxDiffExamples = maxDiffExamples,
      rawDiffExampleBuilder = new RawDiffExampleBuilder[String, R, String, Any] {
        def asRawKeyExample(key: String): String = key
        def asRawLeftExample(left: R): Any = left.get.value
        def asRawRightExample(right: R): Any = right.get.value
      }
    )

    def compareRecords(key: String, left: R, right: R): KindOfDiff = {
      val l = left.get
      val r = right.get
      var flags = NEUTRAL
      def compareField(f: Record => AnyRef, diffs: CommonRawDiffsPartBuilder[String, R, String, Any]): KindOfDiff = {
        val fieldL = f(l)
        val fieldR = f(r)
        if ((fieldL == null) == (fieldR == null)) {
          if (fieldL == null) {
            diffs.identical()
          } else {
            if (fieldL == fieldR) diffs.identical() else diffs.different(key, left, right)
          }
        } else if (fieldL == null) {
          diffs.rightOnly(key, right)
        } else {
          diffs.leftOnly(key, left)
        }
      }
      flags |= compareField(_.name, nameDiffs)
      flags |= compareField(_.value, valueDiffs)
      flags
    }

    var recordExampleMakerCallCount: Int = 0
    val recordExampleBuilder = new RawDiffExampleBuilder[String, R, String, Record] {
      def asRawKeyExample(key: String): String = {
        recordExampleMakerCallCount += 1
        key
      }
      def asRawLeftExample(left: R): Record = left.get
      def asRawRightExample(right: R): Record = {
        val r = right.get
        r.copy(
          name = if (r.name != null) r.name * 2 else null,
          value = if (r.value != null) r.value * 2 else null
        )
      }
    }
    var multipleMatchesExampleMakerCallCount: Int = 0
    val multipleMatchesExampleBuilder = new RawDiffExampleBuilder[String, Iterator[R], String, Seq[Record]] {
      def asRawKeyExample(key: String): String = {
        multipleMatchesExampleMakerCallCount += 1
        key
      }
      def asRawLeftExample(left: Iterator[R]): Seq[Record] = left.map(_.get).toList
      def asRawRightExample(right: Iterator[R]): Seq[Record] =
        right
          .map(record => {
            val r = record.get
            r.copy(
              name = if (r.name != null) r.name * 3 else null,
              value = if (r.value != null) r.value * 3 else null
            )
          })
          .toList
    }

    def isLeftEqual(a: R, b: R): Boolean = a.get == b.get
    def isRightEqual(a: R, b: R): Boolean = a.get == b.get

    def results: List[(String, LeftRightRawDiffsPart[String, Any])] =
      ("name" -> nameDiffs.result()) :: ("value" -> valueDiffs.result()) :: Nil

    override def binaryIdenticalRecord(record: Pointer[Record]): Unit = ???
  }
}

final class MatchedRecordDiffsPartBuilderSpec extends AnyFlatSpec with Matchers {
  import KindOfDiff._
  import MatchedRecordDiffsPartBuilderSpec._

  def buildBinaryRecordDiffBuilder(maxDiffExamples: Int, omitMultipleMatchesIfAllIdentical: Boolean) =
    new MatchedRecordDiffsPartBuilder[String, Pointer[Record], String, Record](
      recordRawDiffsAccumulator = DummyRecordRawDiffsAccumulator(maxDiffExamples),
      omitMultipleMatchesIfAllIdentical = omitMultipleMatchesIfAllIdentical,
      maxDiffExamples = maxDiffExamples
    )

  private def head(name: String = null, value: java.lang.Integer = null) = FixedPointer(Record(name, value))
  private def tail(records: Record*) = UnsafeIterator(records)

  it should "support all kind of differences" in {
    val b = buildBinaryRecordDiffBuilder(maxDiffExamples = 10, omitMultipleMatchesIfAllIdentical = false)
    b.compare(
      "identical",
      head("a", 1),
      tail(),
      head("a", 1),
      tail()
    ) should be(IDENTICAL)
    b.compare(
      "different-name",
      head("cc", 3),
      tail(),
      head("c", 3),
      tail()
    ) should be(IDENTICAL | DIFFERENT)
    b.compare(
      "different-value",
      head("d", 4),
      tail(),
      head("d", 44),
      tail()
    ) should be(IDENTICAL | DIFFERENT)
    b.leftOnly("left-only", head("e", 5), tail()) should be(LEFT_ONLY)
    b.rightOnly("right-only", head("f", 6), tail()) should be(RIGHT_ONLY)
    b.compare(
      "missing-value",
      head(name = "g"),
      tail(),
      head("g", 7),
      tail()
    ) should be(IDENTICAL | RIGHT_ONLY)
    b.compare(
      "missing-name",
      head("h", 8),
      tail(),
      head(value = 8),
      tail()
    ) should be(IDENTICAL | LEFT_ONLY)
    b.compare(
      "different-name-and-missing-value",
      head(name = "i"),
      tail(),
      head("ii", 9),
      tail()
    ) should be(DIFFERENT | RIGHT_ONLY)
    b.compare(
      "different-value-and-missing-name",
      head("j", 1010),
      tail(),
      head(value = 10),
      tail()
    ) should be(DIFFERENT | LEFT_ONLY)
    b.compare(
      "left-and-right-only",
      head(value = 11),
      tail(),
      head(name = "k"),
      tail()
    ) should be(LEFT_ONLY | RIGHT_ONLY)

    Checker.check(
      result = b.result,
      expected = DatasetDiffsBuilder[String, Record]()
        .kindOfDifferent(
          content = 4,
          leftOnly = 1,
          rightOnly = 1,
          leftAndRightOnly = 1
        )
        .record(
          identical = 1,
          diffExamples = Seq(
            ("different-name", Record("cc", 3), Record("cc", 6)),
            ("different-value", Record("d", 4), Record("dd", 88)),
            ("missing-value", Record(name = "g"), Record("gg", 14)),
            ("missing-name", Record("h", 8), Record(value = 16)),
            ("different-name-and-missing-value", Record(name = "i"), Record("iiii", 18)),
            ("different-value-and-missing-name", Record("j", 1010), Record(value = 20)),
            ("left-and-right-only", Record(value = 11), Record(name = "kk"))
          ),
          leftExamples = Seq(("left-only", Record("e", 5))),
          rightExamples = Seq(("right-only", Record("ff", 12)))
        )
        .common(
          "name",
          identical = 3,
          diffExamples = Seq(
            ("different-name", "cc", "c"),
            ("different-name-and-missing-value", "i", "ii")
          ),
          leftExamples = Seq(
            ("missing-name", "h"),
            ("different-value-and-missing-name", "j")
          ),
          rightExamples = Seq(
            ("left-and-right-only", "k")
          )
        )
        .common(
          "value",
          identical = 3,
          diffExamples = Seq(
            ("different-value", 4, 44),
            ("different-value-and-missing-name", 1010, 10)
          ),
          leftExamples = Seq(
            ("left-and-right-only", 11)
          ),
          rightExamples = Seq(
            ("missing-value", 7),
            ("different-name-and-missing-value", 9)
          )
        )
    )
  }

  it should "not store more examples than necessary" in {
    val rawDiffsAccumulator = DummyRecordRawDiffsAccumulator(1)
    val b = new MatchedRecordDiffsPartBuilder[String, Pointer[Record], String, Record](
      recordRawDiffsAccumulator = rawDiffsAccumulator,
      omitMultipleMatchesIfAllIdentical = false,
      maxDiffExamples = 1
    )
    b.compare(
      "multiple",
      head("a", 1),
      tail(Record("a", 1)),
      head("a", 1),
      tail()
    ) should be(MULTIPLE_MATCHES)
    b.compare(
      "multiple2",
      head("b", 2),
      tail(Record("b", 2)),
      head("b", 2),
      tail()
    ) should be(MULTIPLE_MATCHES)
    b.compare(
      "different",
      head("cc", 3),
      tail(),
      head("c", 3),
      tail()
    ) should be(IDENTICAL | DIFFERENT)
    b.compare(
      "different2",
      head("ccc", 3),
      tail(),
      head("c", 3),
      tail()
    ) should be(IDENTICAL | DIFFERENT)
    b.leftOnly("left-only", head("d", 4), tail()) should be(LEFT_ONLY)
    b.leftOnly("left-only2", head("e", 5), tail()) should be(LEFT_ONLY)
    b.rightOnly("right-only", head("f", 6), tail()) should be(RIGHT_ONLY)
    b.rightOnly("right-only2", head("g", 7), tail()) should be(RIGHT_ONLY)
    b.compare(
      "identical",
      head("h", 8),
      tail(),
      head("h", 8),
      tail()
    ) should be(IDENTICAL)
    b.compare(
      "left-only-name",
      head("i", 9),
      tail(),
      head(value = 9),
      tail()
    ) should be(IDENTICAL | LEFT_ONLY)
    b.compare(
      "left-only-name2",
      head("j", 10),
      tail(),
      head(value = 10),
      tail()
    ) should be(IDENTICAL | LEFT_ONLY)
    b.compare(
      "right-only-name",
      head(value = 11),
      tail(),
      head("k", 11),
      tail()
    ) should be(IDENTICAL | RIGHT_ONLY)
    b.compare(
      "right-only-name2",
      head(value = 12),
      tail(),
      head("l", 12),
      tail()
    ) should be(IDENTICAL | RIGHT_ONLY)

    Checker.check(
      result = b.result,
      expected = DatasetDiffsBuilder[String, Record]()
        .multipleMatches(
          count = 2,
          examples = Seq(("multiple", Seq(Record("a", 1), Record("a", 1)), Seq(Record("aaa", 3))))
        )
        .kindOfDifferent(
          content = 2,
          leftOnly = 2,
          rightOnly = 2
        )
        .record(
          identical = 1,
          different = 6,
          leftOnly = 2,
          rightOnly = 2,
          diffExamples = Seq(("different", Record("cc", 3), Record("cc", 6))),
          leftExamples = Seq(("left-only", Record("d", 4))),
          rightExamples = Seq(("right-only", Record("ff", 12)))
        )
        .common(
          "name",
          identical = 1,
          different = 2,
          leftOnly = 2,
          rightOnly = 2,
          diffExamples = Seq(("different", "cc", "c")),
          leftExamples = Seq(("left-only-name", "i")),
          rightExamples = Seq(("right-only-name", "k"))
        )
        .common("value", identical = 7)
    )

    rawDiffsAccumulator.recordExampleMakerCallCount should be(3)
    rawDiffsAccumulator.multipleMatchesExampleMakerCallCount should be(1)
  }

  it should "handle correctly multiple records" in {
    for (ignoreMultipleMatchesIfIdentical <- Seq(true, false)) {
      withClue(s"ignore multiple matches ? $ignoreMultipleMatchesIfIdentical") {
        val b = buildBinaryRecordDiffBuilder(maxDiffExamples = 10, ignoreMultipleMatchesIfIdentical)
        b.compare(
          "A",
          head("a", 1),
          tail(Record("a", 2), Record("a", 3)),
          head("a", 10),
          tail()
        ) should be(MULTIPLE_MATCHES)
        b.compare(
          "B",
          head("b", 1),
          tail(),
          head("b", 10),
          tail(Record("b", 11), Record("b", 12))
        ) should be(MULTIPLE_MATCHES)
        b.compare(
          "C",
          head("c", 1),
          tail(Record("c", 2), Record("c", 3)),
          head("c", 10),
          tail(Record("c", 11), Record("c", 12))
        ) should be(MULTIPLE_MATCHES)

        b.leftOnly("D", head("d", 1), tail(Record("d", 2), Record("d", 3))) should be(MULTIPLE_MATCHES)
        b.rightOnly("E", head("e", 10), tail(Record("e", 11), Record("e", 12))) should be(MULTIPLE_MATCHES)

        Checker.check(
          result = b.result,
          expected = DatasetDiffsBuilder[String, Record]()
            .multipleMatches(
              examples = Seq(
                (
                  "A",
                  Seq(Record("a", 1), Record("a", 2), Record("a", 3)),
                  Seq(Record("aaa", 30))
                ),
                (
                  "B",
                  Seq(Record("b", 1)),
                  Seq(Record("bbb", 30), Record("bbb", 33), Record("bbb", 36))
                ),
                (
                  "C",
                  Seq(Record("c", 1), Record("c", 2), Record("c", 3)),
                  Seq(Record("ccc", 30), Record("ccc", 33), Record("ccc", 36))
                ),
                (
                  "D",
                  Seq(Record("d", 1), Record("d", 2), Record("d", 3)),
                  Nil
                ),
                (
                  "E",
                  Nil,
                  Seq(Record("eee", 30), Record("eee", 33), Record("eee", 36))
                )
              )
            )
            .common("name")
            .common("value")
        )
      }
    }
  }

  it should "ignore multiple identical records" in {
    val builder = buildBinaryRecordDiffBuilder(maxDiffExamples = 10, omitMultipleMatchesIfAllIdentical = true)
    builder.compare(
      "A",
      head("a", 1),
      tail(Record("a", 1), Record("a", 1)),
      head("a", 1),
      tail()
    ) should be(IDENTICAL)
    builder.compare(
      "B",
      head("b", 10),
      tail(),
      head("b", 10),
      tail(Record("b", 10), Record("b", 10))
    ) should be(IDENTICAL)
    builder.compare(
      "C",
      head("c", 1),
      tail(Record("c", 1), Record("c", 1)),
      head("c", 10),
      tail(Record("c", 10), Record("c", 10))
    ) should be(IDENTICAL | DIFFERENT)
    builder.leftOnly("D", head("d", 1), tail(Record("d", 1), Record("d", 1))) should be(LEFT_ONLY)
    builder.rightOnly("E", head("e", 10), tail(Record("e", 10), Record("e", 10))) should be(RIGHT_ONLY)

    Checker.check(
      result = builder.result,
      expected = DatasetDiffsBuilder[String, Record]()
        .kindOfDifferent(content = 1)
        .record(identical = 2,
                diffExamples = Seq(("C", Record("c", 1), Record("cc", 20))),
                leftExamples = Seq(("D", Record("d", 1))),
                rightExamples = Seq(("E", Record("ee", 20))))
        .common("name", identical = 3)
        .common("value", identical = 2, diffExamples = Seq(("C", 1, 10)))
    )
  }
}
