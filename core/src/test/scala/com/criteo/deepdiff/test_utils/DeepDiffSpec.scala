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
import com.criteo.deepdiff.config.DeepDiffConfig
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types.{ArrayType, MapType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.CancelAfterFailure
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.{Files, Path}
import java.util.UUID
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

object DeepDiffSpec {
  private object CaseClassExamples {
    def productAsExample(p: Product): Map[String, Any] = {
      if (p == null) null
      else {
        val values = p.productIterator
        p.getClass.getDeclaredFields
          .filterNot(_.isSynthetic)
          .map({ _.getName -> asExample(values.next()) })
          .toMap
      }
    }

    def asExample(x: Any): Any =
      x match {
        case s: Seq[_]                    => s.map(asExample)
        case m: Map[_, _]                 => m.mapValues(asExample)
        case p: Product if isCaseClass(p) => productAsExample(p)
        case x                            => x
      }

    private def isCaseClass(x: Any): Boolean = {
      import reflect.runtime.universe._
      val typeMirror = runtimeMirror(x.getClass.getClassLoader)
      val instanceMirror = typeMirror.reflect(x)
      val symbol = instanceMirror.symbol
      symbol.isCaseClass
    }
  }

  sealed trait Properties
  object Properties {
    sealed trait Empty extends Properties
    sealed trait Config extends Properties
    sealed trait Inputs extends Properties
    sealed trait Expected extends Properties

    type AllMandatory = Empty with Config with Inputs with Expected
  }

  final case class MetaProperties(
      testReversedLeftAndRight: Boolean = false,
      testPruned: Boolean = false
  )

  sealed case class DeepDiffTestCase[I <: Properties](
      private[test_utils] val config: Option[String],
      private[test_utils] val reversedConfig: Option[String],
      private[test_utils] val inputs: Option[SparkSession => (DataFrame, DataFrame)],
      private[test_utils] val expected: Option[DatasetDiffsBuilder[KeyExample, Map[String, Any]]],
      private[test_utils] val prunedSchema: Option[(StructType, StructType)],
      private[test_utils] val meta: MetaProperties
  ) {
    def defaultConf(): DeepDiffTestCase[I with Properties.Config] =
      copy( // language=HOCON
        config = Some("""keys = ["key"]
            |""".stripMargin))

    def conf(config: String): DeepDiffTestCase[I with Properties.Config] = copy(config = Some(config))

    def withReversedConfig(config: String): DeepDiffTestCase[I] = copy(reversedConfig = Some(config))

    def testPruned(left: StructType, right: StructType): DeepDiffTestCase[I] =
      copy(meta = meta.copy(testPruned = true), prunedSchema = Some((left, right)))

    def testPruned(common: StructType): DeepDiffTestCase[I] =
      copy(meta = meta.copy(testPruned = true), prunedSchema = Some((common, common)))

    def testPruned(): DeepDiffTestCase[I] =
      copy(meta = meta.copy(testPruned = true))

    // should be used when the test is not strictly symmetrical, which is most of the time.
    def testReversedLeftAndRight(): DeepDiffTestCase[I] = copy(meta = meta.copy(testReversedLeftAndRight = true))

    def compare[L <: Product: TypeTag, R <: Product: TypeTag](
        left: Seq[L],
        right: Seq[R]
    ): DeepDiffTestCase[I with Properties.Inputs] =
      copy(inputs = Some(spark => (spark.createDataFrame(left), spark.createDataFrame(right))))

    def compare(
        left: SparkSession => DataFrame,
        right: SparkSession => DataFrame
    ): DeepDiffTestCase[I with Properties.Inputs] =
      copy(inputs = Some(spark => (left(spark), right(spark))))

    def expect[P <: Product](
        builder: DatasetDiffsBuilder[KeyExample, P]
    ): DeepDiffTestCase[I with Properties.Expected] = {
      import CaseClassExamples._
      copy(expected = Some(builder.transform(productAsExample, asExample)))
    }

    def expectRaw(
        builder: DatasetDiffsBuilder[KeyExample, Map[String, Any]]
    ): DeepDiffTestCase[I with Properties.Expected] =
      copy(expected = Some(builder))
  }
  object DeepDiffTestCase extends DeepDiffTestCase[Properties.Empty](None, None, None, None, None, MetaProperties())
}

abstract class DeepDiffSpec extends AnyWordSpec with Matchers with LocalSparkSpec with CancelAfterFailure {
  import DeepDiffSpec._

  // Can only be used within a test (a in { ... } clause)
  implicit private def spark: SparkSession = getSparkSession

  final def runningDeepDiffTestCase(f: => DeepDiffTestCase[Properties.AllMandatory]): Unit =
    behave like {
      val testCase = f
      val config = testCase.config.get
      val reversedConfig = testCase.reversedConfig.getOrElse(config)
      // spark is not yet defined.
      lazy val inputs = testCase.inputs.get(spark)
      lazy val reversedInputs = (inputs._2, inputs._1)
      lazy val expected =
        testCase.prunedSchema
          .map({ case (l, r) => testCase.expected.get.schemaIfAbsent(l, r) })
          .getOrElse(
            testCase.expected.get.schemaIfAbsent(forceNullable(inputs._1.schema), forceNullable(inputs._2.schema)))
      "generating the expected diff report" in {
        runDeepDiff(config, inputs, expected)
      }
      if (testCase.meta.testReversedLeftAndRight) {
        "generating the expected diff report with left and right reversed" in {
          runDeepDiff(reversedConfig, reversedInputs, expected.reverseLeftAndRight)
        }
      }
      if (testCase.meta.testPruned) {
        "generating the expected diff report with pruned records" in {
          runPrunedDeepDiff(config, inputs, expected)
        }
        if (testCase.meta.testReversedLeftAndRight) {
          "generating the expected diff report with pruned records and left and right reversed" in {
            runPrunedDeepDiff(reversedConfig, reversedInputs, expected.reverseLeftAndRight)
          }
        }
      }
    }

  private def forceNullable(schema: StructType): StructType = StructType(schema.fields.map(forceNullable))

  private def forceNullable(field: StructField): StructField =
    field.dataType match {
      case s: StructType                   => field.copy(dataType = forceNullable(s))
      case m: MapType                      => field.copy(dataType = m.copy(valueContainsNull = true), nullable = true)
      case a @ ArrayType(s: StructType, _) => field.copy(dataType = a.copy(elementType = forceNullable(s)))
      case _                               => field.copy(nullable = true)
    }

  final def withTmpDir[T](test: Path => T): T = {
    val tmpDir = Files.createTempDirectory("deepdiff-tests")
    try {
      test(tmpDir)
    } finally {
      Files
        .walk(tmpDir)
        .iterator()
        .asScala
        .toSeq
        .sorted
        .reverse
        .foreach(Files.delete)
    }
  }

  private def runDeepDiff(config: String,
                          inputs: (DataFrame, DataFrame),
                          expected: DatasetDiffsBuilder[KeyExample, Map[String, Any]]): Unit = {
    val rawConfig = ConfigFactory.parseString(config)
    val deepDiff = new DeepDiff {
      val config: DeepDiffConfig = DeepDiffConfig.fromRawConfig(rawConfig)
      override def leftAndRightDataFrames(): (DataFrame, DataFrame) = inputs
    }

    Checker.check(
      result = deepDiff.run(),
      expected = expected
    )
  }

  private def runPrunedDeepDiff(config: String,
                                inputs: (DataFrame, DataFrame),
                                expected: DatasetDiffsBuilder[KeyExample, Map[String, Any]]): Unit = {
    withTmpDir(tmpDir => {
      val rawConfig = ConfigFactory.parseString(config)
      val (left, right) = inputs
      val deepDiff = new FastDeepDiff {
        val config: DeepDiffConfig = DeepDiffConfig.fromRawConfig(rawConfig)
        def leftSchema: StructType = left.schema
        def rightSchema: StructType = right.schema
        def leftDataFrame(schema: StructType): DataFrame = forceSchema(left, schema)
        def rightDataFrame(schema: StructType): DataFrame = forceSchema(right, schema)

        def forceSchema(input: DataFrame, schema: StructType): DataFrame = {
          val parquetPath = tmpDir.resolve(UUID.randomUUID().toString).normalize().toAbsolutePath.toString
          input.write.parquet(parquetPath)
          spark.read.schema(schema).parquet(parquetPath)
        }
      }

      Checker.check(
        result = deepDiff.run(),
        expected = expected
      )
    })
  }
}
