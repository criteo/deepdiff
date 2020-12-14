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

package com.criteo.deepdiff.test_utils

import com.criteo.deepdiff._
import com.criteo.deepdiff.raw_part.{
  DatasetRawDiffsPart,
  LeftOnlyRecordRawDiffsPart,
  MatchedRecordsRawDiffsPart,
  RawToFriendlyDiffExample,
  RightOnlyRecordRawDiffsPart
}
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.scalatest.AppendedClues
import org.scalatest.matchers.should.Matchers

import java.nio.ByteBuffer
import java.util
import scala.util.Try

/** Debug friendly DataSetRawDiffsPart checker. It checks result in a specific order to provide the
  * most information possible for debugging. Moreover examples are nicely formatted to help find all
  * differences.
  */
object Checker extends Matchers with AppendedClues {
  private def rawToFriendly[K, F]: RawToFriendlyDiffExample[K, F, K, F] =
    new RawToFriendlyDiffExample[K, F, K, F] {
      def rawToFriendlyKey(rawKey: K): K = rawKey
      def rawToFriendlyLeft(rawLeft: F): F = rawLeft
      def rawToFriendlyRight(rawRight: F): F = rawRight
    }

  private def matchedRecordDiffIdentity[K, R](m: MatchedRecordsRawDiffsPart[K, R]): MatchedRecordsDiffs[K, R] =
    m.buildMatchedRecordsDiffs(
      multipleMatchesRawToFriendly = rawToFriendly[K, Seq[R]],
      recordRawToFriendly = rawToFriendly[K, R],
      rawToFriendlyByFullName = Map
        .empty[String, RawToFriendlyDiffExample[K, Any, K, Any]]
        .withDefaultValue(rawToFriendly[K, Any])
    )

  def check[K, R](result: MatchedRecordsRawDiffsPart[K, R], expected: DatasetDiffsBuilder[K, R]): Unit = {
    check(
      DatasetDiffs(
        root = matchedRecordDiffIdentity(result),
        explodedArrays = Map.empty,
        schema = DatasetDiffsBuilder.emptySchema
      ),
      expected.result
    )
  }

  def check[K, R](result: DatasetRawDiffsPart[K, R], expected: DatasetDiffsBuilder[K, R]): Unit = {
    check(
      DatasetDiffs(
        root = matchedRecordDiffIdentity(result.root),
        explodedArrays = result.explodedArrays.mapValues({
          case m: MatchedRecordsRawDiffsPart[K, R]  => matchedRecordDiffIdentity(m)
          case m: LeftOnlyRecordRawDiffsPart[K, R]  => m.buildLeftOnlyRecordDiffs(rawToFriendly[K, R])
          case m: RightOnlyRecordRawDiffsPart[K, R] => m.buildRightOnlyRecordDiffs(rawToFriendly[K, R])
        }),
        schema = DatasetDiffsBuilder.emptySchema
      ),
      expected.result
    )
  }

  def check[K, R](result: DatasetDiffs[K, R], expected: DatasetDiffsBuilder[K, R]): Unit =
    check(result, expected.result)

  def check[K, R](result: DatasetDiffs[K, R], expected: DatasetDiffs[K, R]): Unit = {
    withClue("Schema > ") {
      recursiveCheckSchema(result.schema.left, expected.schema.left)
      recursiveCheckSchema(result.schema.right, expected.schema.right)
    } withClue PrettyDiff(result.schema, expected.schema)
    withClue("Exploded Arrays > ") {
      val expectedArrays = expected.explodedArrays.keySet
      val resultArrays = result.explodedArrays.keySet
      resultArrays should contain theSameElementsAs expectedArrays withClue PrettyDiff(resultArrays, expectedArrays)
      // Nested fields should be at the end as they provide less information
      for (name <- resultArrays.toSeq.sortBy(name => (name.split('.').length, name))) {
        withClue(s"$name > ") { check(result.explodedArrays(name), expected.explodedArrays(name)) }
      }
    }
    withClue("Root >") { check(result.root, expected.root) }
  }

  private def recursiveCheckSchema(result: StructType, expected: StructType): Unit = {
    val r = result.fields.map(f => f.name -> f).toMap
    val e = expected.fields.map(f => f.name -> f).toMap
    r.keySet should contain theSameElementsAs e.keySet
    r.foreach {
      case (name, field) =>
        val expectedField = e(name)
        (field.dataType, expectedField.dataType) match {
          case (rs: StructType, es: StructType) => recursiveCheckSchema(rs, es)
          case (ra: ArrayType, ea: ArrayType) =>
            (ra.elementType, ea.elementType) match {
              case (ras: StructType, eas: StructType) =>
                recursiveCheckSchema(ras, eas)
              case _ => field should be(expectedField)
            }
          case _ => field should be(expectedField)
        }
    }
  }

  private def check(result: RecordDiffs[_, _], expected: RecordDiffs[_, _]): Unit = {
    (result, expected) match {
      case (r: MatchedRecordsDiffs[_, _], e: MatchedRecordsDiffs[_, _]) =>
        withClue("MultipleMatches > ") { check(r.multipleMatches, e.multipleMatches) }
        // Check different row types after root for easier debugging with root's examples.
        check(r.kindOfDifferentRecords, e.kindOfDifferentRecords) withClue PrettyDiff.global(r, e)
        withClue("Record > ") { check(r.recordDiffs, e.recordDiffs) } withClue PrettyDiff.global(r, e)

        val expectedFields = e.fieldsDiffs.keySet
        val resultFields = r.fieldsDiffs.keySet
        withClue("Fields > ") {
          resultFields should contain theSameElementsAs expectedFields withClue PrettyDiff(resultFields, expectedFields)
        }
        // Nested fields should be at the end as they provide less information
        for (name <- resultFields.toSeq.sortBy(name => (name.split('.').length, name))) {
          withClue(s"$name > ") {
            check(r.fieldsDiffs(name), e.fieldsDiffs(name))
          } withClue PrettyDiff.global(r, e)
        }
      case (r: LeftOnlyRecordDiffs[_, _], e: LeftOnlyRecordDiffs[_, _]) =>
        withClue("Record > ") { check(r.records, e.records) } withClue PrettyDiff(r.records, e.records)
        withClue("Fields > ") {
          r.fields should contain theSameElementsAs (e.fields)
        }
      case (r: RightOnlyRecordDiffs[_, _], e: RightOnlyRecordDiffs[_, _]) =>
        withClue("Record > ") { check(r.records, e.records) } withClue PrettyDiff(r.records, e.records)
        withClue("Fields > ") {
          r.fields should contain theSameElementsAs (e.fields)
        }
      case (r, e) =>
        r should be(e)

    }
  }

  private def check(result: KindOfDifferentRecords, expected: KindOfDifferentRecords): Unit = {
    result should be(expected)
  }

  private def check(result: LeftRightDiffs[_, _], expected: LeftRightDiffs[_, _]): Unit = {
    def countHint[T](r: T, e: T)(f: T => Long): String = {
      val fr = f(r)
      val fe = f(e)
      if (fr == fe) s"$fr" else s"$fr|$fe"
    }

    (result, expected) match {
      case (r: CommonDiffs[_, _], e: CommonDiffs[_, _]) =>
        withClue(
          Seq(
            s"identical: ${countHint(r, e)(_.identical)}",
            s"different: ${countHint(r, e)(_.different.count)}",
            s"leftOnly: ${countHint(r, e)(_.leftOnly.count)}",
            s"rightOnly: ${countHint(r, e)(_.rightOnly.count)}"
          ).mkString("", ", ", " > ")) {
          withClue("different\n") { check(r.different, e.different) }
          withClue("leftOnly\n") { check(r.leftOnly, e.leftOnly) }
          withClue("rightOnly\n") { check(r.rightOnly, e.rightOnly) }
          // Check identical at the end as it has no examples
          withClue("identical\n") { r.identical should be(e.identical) }
        }
      case (r: LeftOnlyDiffs[_, _], e: LeftOnlyDiffs[_, _]) =>
        withClue(
          Seq(
            s"identical: ${countHint(r, e)(_.nulls)}",
            s"leftOnly: ${countHint(r, e)(_.leftOnly.count)}"
          ).mkString("> ", ", ", " > ")) {
          withClue("leftOnly\n") { check(r.leftOnly, e.leftOnly) }
          // Check identical at the end as it has no examples
          withClue("identical\n") { r.nulls should be(e.nulls) }
        }
      case (r: RightOnlyDiffs[_, _], e: RightOnlyDiffs[_, _]) =>
        withClue(
          Seq(
            s"identical: ${countHint(r, e)(_.nulls)}",
            s"rightOnly: ${countHint(r, e)(_.rightOnly.count)}"
          ).mkString("> ", ", ", " > ")) {
          withClue("rightOnly\n") { check(r.rightOnly, e.rightOnly) }
          // Check identical at the end as it has no examples
          withClue("identical\n") { r.nulls should be(e.nulls) }
        }
      case (r, e) =>
        r should be(e)

    }
  }

  private def check(result: Diff[_, DiffExample[_]], expected: Diff[_, DiffExample[_]]): Unit = {
    val r = wrapBinary(result.examples).asInstanceOf[Map[_, _]]
    val e = wrapBinary(expected.examples).asInstanceOf[Map[_, _]]
    r should contain theSameElementsAs e withClue PrettyDiff(r, e)
    // Check count after the examples as it helps less.
    result.count should be(expected.count)
  }

  private def wrapBinary(x: Any): Any =
    x match {
      case binary: Array[Byte]      => ByteBuffer.wrap(binary)
      case s: Seq[_]                => s.map(wrapBinary)
      case m: Map[_, _]             => m.map({ case (k, v) => (wrapBinary(k), wrapBinary(v)) })
      case FieldsKeyExample(fields) => FieldsKeyExample(wrapBinary(fields).asInstanceOf[Map[String, Option[_]]])
      case CompositeKeyExample(keyExamples) =>
        CompositeKeyExample(wrapBinary(keyExamples).asInstanceOf[List[SimpleKeyExample]])
      case DifferentExample(left, right) => DifferentExample(wrapBinary(left), wrapBinary(right))
      case LeftOnlyExample(left)         => LeftOnlyExample(wrapBinary(left))
      case RightOnlyExample(right)       => RightOnlyExample(wrapBinary(right))
      case _                             => x
    }

  // Utility to generate human readable diff of records. Produces something like:
  // key: 'common_leftAndRightOnly_field' <Map3>
  //   right <HashTrieMap>
  //     total <Double>
  //       -5.0 is not equal -50.0
  private object PrettyDiff {
    def global(result: MatchedRecordsDiffs[_, _], expected: MatchedRecordsDiffs[_, _]): Any =
      create(result.fieldsDiffs, expected.fieldsDiffs, "=== RECORD DIFF HINTS ===")

    def apply(result: Any, expected: Any): Any =
      create(result, expected, "=== DIFF HINTS ===")

    private def create(result: Any, expected: Any, title: String): Any = {
      new Object {
        private lazy val message: String = {
          Try(
            recursiveDifferences(result, expected)
              .getOrElse(PDiff("No Diff found ?"))
              .asString(title))
            .recover({ case t => s"Could not generate diff. Raised error:\n$t" })
            .get
        }

        override def toString: String = message
      }
    }

    sealed trait PrettyDifference {
      val indentStep = " "

      def asString(title: String): String = {
        val b = s"\n$title\n"
        b + prettyString().getOrElse("∅") + b
      }

      def prettyString(indentLevel: Int = 0): Option[String]
    }

    object PrettyDifference {
      def merge(differences: Option[PrettyDifference]*): Option[PrettyDifference] = merge(differences)

      def merge(differences: Iterable[Option[PrettyDifference]]): Option[PrettyDifference] = {
        val d = differences.flatten
        if (d.nonEmpty) Some(PDiffSeq(d.toSeq)) else None
      }
    }

    case object PDiffAlreadyShown extends PrettyDifference {
      override def prettyString(indentLevel: Int): Option[String] = None
    }

    final case class PDiff(text: String) extends PrettyDifference {
      def prettyString(indentLevel: Int): Option[String] = Some(indentStep * indentLevel + text)
    }

    final case class PDiffSeq(differences: Seq[PrettyDifference]) extends PrettyDifference {
      def prettyString(indentLevel: Int): Option[String] = {
        val diff = differences.flatMap(_.prettyString(indentLevel))
        if (diff.nonEmpty) Some(diff.mkString("\n")) else None
      }
    }

    final case class PDiffSection(symbol: Char, title: String, difference: PrettyDifference) extends PrettyDifference {
      def prettyString(indentLevel: Int): Option[String] = {
        Some(
          Seq(
            indentStep * indentLevel,
            symbol,
            indentStep,
            title,
            difference.prettyString(indentLevel + 2).map("\n" + _).getOrElse("")
          ).mkString(""))
      }
    }

    object PDiffSection {
      def element(id: String, difference: PrettyDifference): PDiffSection = PDiffSection('*', id, difference)
      def part(title: String, difference: PrettyDifference): PDiffSection = PDiffSection('>', title, difference)
    }

    private def recursiveDifferences(a: Any, b: Any): Option[PrettyDifference] =
      (a, b) match {
        case (result: DatasetsSchema, expected: DatasetsSchema) =>
          PrettyDifference.merge(
            recursiveDifferences(result.left, expected.left).map(PDiffSection.part("left", _)),
            recursiveDifferences(result.right, expected.right).map(PDiffSection.part("right", _))
          )
        case (result: StructType, expected: StructType) =>
          SomeIf(result != expected)(
            PDiffSeq(
              result.treeString.split('\n').map(PDiff)
                ++ Seq(PDiff("is not equal to"))
                ++ expected.treeString.split('\n').map(PDiff)))
        case (result: LeftRightDiffs[_, _], expected: LeftRightDiffs[_, _]) =>
          (result, expected) match {
            case (r: CommonDiffs[_, _], e: CommonDiffs[_, _]) =>
              PrettyDifference.merge(
                SomeIf(r.identical != e.identical)(
                  PDiff(s"Identical: ${r.identical} is not equal ${e.identical}")
                ),
                recursiveDifferences(r.different, e.different).map(PDiffSection.part("different", _)),
                recursiveDifferences(r.leftOnly, e.leftOnly).map(PDiffSection.part("leftOnly", _)),
                recursiveDifferences(r.rightOnly, e.rightOnly).map(PDiffSection.part("rightOnly", _))
              )
            case (r: LeftOnlyDiffs[_, _], e: LeftOnlyDiffs[_, _]) =>
              PrettyDifference.merge(
                SomeIf(r.nulls != e.nulls)(
                  PDiff(s"Identical: ${r.nulls} is not equal ${e.nulls}")
                ),
                recursiveDifferences(r.leftOnly, e.leftOnly).map(PDiffSection.part("leftOnly", _))
              )
            case (r: RightOnlyDiffs[_, _], e: RightOnlyDiffs[_, _]) =>
              PrettyDifference.merge(
                SomeIf(r.nulls != e.nulls)(
                  PDiff(s"Identical: ${r.nulls} is not equal ${e.nulls}")
                ),
                recursiveDifferences(r.rightOnly, e.rightOnly).map(PDiffSection.part("rightOnly", _))
              )
            case _ =>
              Some(PDiffAlreadyShown) // type info already displayed as fields are within a map
          }
        case (result: Diff[_, DiffExample[_]], expected: Diff[_, DiffExample[_]]) =>
          PrettyDifference.merge(
            SomeIf(result.count != expected.count)(PDiff(s"Count: ${result.count} is not equal ${expected.count}")),
            recursiveDifferences(result.examples, expected.examples).map(
              PDiffSection.part("Examples", _)
            )
          )
        case (result: DiffExample[_], expected: DiffExample[_]) =>
          (result, expected) match {
            case (DifferentExample(leftResult, rightResult), DifferentExample(leftExpected, rightExpected)) =>
              PrettyDifference.merge(
                recursiveDifferences(leftResult, leftExpected).map(PDiffSection.part("left", _)),
                recursiveDifferences(rightResult, rightExpected).map(PDiffSection.part("right", _))
              )
            case (LeftOnlyExample(leftResult), LeftOnlyExample(leftExpected)) =>
              recursiveDifferences(leftResult, leftExpected).map(PDiffSection.part("left", _))
            case (RightOnlyExample(rightResult), RightOnlyExample(rightExpected)) =>
              recursiveDifferences(rightResult, rightExpected).map(PDiffSection.part("right", _))
          }
        case (result: Map[Any, _], expected: Map[Any, _]) =>
          def strOrder(a: String, b: String): Boolean = {
            if (a.length == b.length) a < b
            else a.length < b.length
          }
          val commonDiff = PrettyDifference
            .merge(
              result.keySet
                .intersect(expected.keySet)
                .toSeq
                // sort String by length, nested fields should be at the end, as they provide less information.
                .sortWith({
                  case (a, b) => debugString(a) < debugString(b)
                })
                .map(key => {
                  val r = result(key)
                  val e = expected(key)
                  recursiveDifferences(r, e).map(PDiffSection.element(s"${debugString(key)} ${typeInfo(r, e)}", _))
                }))

          recursiveDifferences(result.keySet, expected.keySet)
            .map(PDiffSection.part("keys:", _))
            .map(diff => PrettyDifference.merge(Some(diff), commonDiff.map(PDiffSection.part("common", _))))
            .getOrElse(commonDiff)
        case (result: Set[Any], expected: Set[Any]) =>
          SomeIf(result != expected) {
            val resultOnly = result.diff(expected)
            val expectedOnly = expected.diff(result)
            PDiffSeq(
              Seq(
                PDiff(s"result-only = ${resultOnly.map(debugString).toSeq.sorted.mkString(" | ")}"),
                PDiff(s"expected-only = ${expectedOnly.map(debugString).toSeq.sorted.mkString(" | ")}")
              ))
          }
        case (result: Seq[_], expected: Seq[_]) =>
          val diff = PrettyDifference
            .merge(
              result
                .zip(expected)
                .zipWithIndex
                .map({
                  case ((r, e), i) =>
                    recursiveDifferences(r, e).map(PDiffSection.element(s"$i ${typeInfo(r, e)}", _))
                })
            )
          SomeIf(result.length != expected.length)({
            PDiff(s"Seq with different sizes: ${result.length} is not ${expected.length}")
          }).map(x => PrettyDifference.merge(Some(x), diff.map(PDiffSection.part(s"Elements:", _))))
            .getOrElse(diff)
        case (result: Array[Byte], expected: Array[Byte]) =>
          SomeIf(!util.Arrays.equals(result, expected)) {
            PDiff(s"${debugString(result)} is not equal ${debugString(expected)}")
          }
        case (a: Float, b: Float) if java.lang.Float.isNaN(a) || java.lang.Float.isNaN(b) =>
          if (java.lang.Float.isNaN(a) && java.lang.Float.isNaN(b)) None
          else Some(PDiff(s"${debugString(a)} is not equal ${debugString(b)}"))
        case (a: Double, b: Double) if java.lang.Double.isNaN(a) || java.lang.Double.isNaN(b) =>
          if (java.lang.Double.isNaN(a) && java.lang.Double.isNaN(b)) None
          else Some(PDiff(s"${debugString(a)} is not equal ${debugString(b)}"))
        case _ =>
          SomeIf(a != b) { PDiff(s"${debugString(a)} is not equal ${debugString(b)}") }
      }

    private def debugString(x: Any): String =
      x match {
        case null           => "null"
        case s: String      => s"'$s'"
        case b: Array[Byte] => s"<binary> 0x${hex(b)}"
        case b: Byte        => s"<byte> 0x${hex(Array[Byte](b))}"
        case i: Short       => s"<short> $i"
        case i: Long        => s"${i}L"
        case f: Float       => s"${f}f"
        case d: Double      => s"${d}d"
        case FieldsKeyExample(fields) =>
          fields.toSeq
            .sortBy(_._1)
            .map({
              case (name, value) => s"$name: " + value.map(debugString).getOrElse("null")
            })
            .mkString(", ")
        case PositionKeyExample(position)     => position.toString
        case CompositeKeyExample(keyExamples) => keyExamples.reverse.map(debugString).mkString(" >> ")
        case _                                => x.toString
      }

    private def typeInfo(result: Any, expected: Any): String = {
      def typeName(x: Any): String =
        x match {
          case null         => "null"
          case _: List[_]   => "List"
          case _: Map[_, _] => "Map"
          case _            => x.getClass.getSimpleName
        }

      val r = typeName(result)
      val e = typeName(expected)
      if (r == e) s"<$r>" else s"<$r|$e>"
    }

    private def hex(x: Array[Byte]): String = x.map("%02X".format(_)).mkString

    private def SomeIf[T](b: Boolean)(x: => T): Option[T] = if (b) Some(x) else None

    private def FlatSomeIf[T](b: Boolean)(x: => Option[T]): Option[T] = if (b) x else None
  }

}
