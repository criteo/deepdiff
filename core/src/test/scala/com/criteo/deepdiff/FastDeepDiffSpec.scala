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

import com.criteo.deepdiff.config.DeepDiffConfig
import com.criteo.deepdiff.test_utils.LocalSparkSpec
import com.criteo.deepdiff.test_utils.SparkUtils.struct
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/** Spark does not guarantee to respect strictly the given pruned schema. This typically happens
  * with partition columns that are always put at the end, even though we may order them differently.
  */
final class FastDeepDiffSpec extends AnyWordSpec with Matchers with LocalSparkSpec {

  "FastDeepDiff" should {
    "accept retrieved DataFrames schemas if they match the expected pruned one" when {
      "left and right schema are different" in {
        checkFastDeepDiff(
          left = struct("key" -> StringType, "b" -> IntegerType, "d" -> LongType),
          right = struct("key" -> StringType, "b" -> IntegerType)
        )
      }
      "left and right schema are different and have a different ordering than the expected ones" in {
        checkFastDeepDiff(
          left = struct("key" -> StringType, "b" -> IntegerType, "d" -> LongType),
          right = struct("key" -> StringType, "b" -> IntegerType),
          actualLeft = struct("key" -> StringType, "b" -> IntegerType, "d" -> LongType),
          actualRight = struct("b" -> IntegerType, "key" -> StringType)
        )
      }
      "left and right schema are equal" in {
        checkFastDeepDiff(
          left = struct("key" -> StringType, "b" -> IntegerType, "d" -> LongType),
          right = struct("key" -> StringType, "b" -> IntegerType, "d" -> LongType)
        )
      }
      "left and right schema are equal but have a different ordering than the expected one." in {
        checkFastDeepDiff(
          left = struct("key" -> StringType, "b" -> IntegerType, "d" -> LongType),
          right = struct("key" -> StringType, "b" -> IntegerType, "d" -> LongType),
          actualLeft = struct("key" -> StringType, "d" -> LongType, "b" -> IntegerType),
          actualRight = struct("key" -> StringType, "d" -> LongType, "b" -> IntegerType)
        )
      }
    }
    "raise an error if the retrieved DataFrames schemas do not match the expected pruned one" when {
      "any field is missing" in {
        // left missing
        a[AssertionError] should be thrownBy {
          checkFastDeepDiff(
            left = struct("key" -> StringType, "a" -> LongType),
            right = struct("key" -> StringType, "b" -> IntegerType),
            actualLeft = struct("key" -> StringType),
            actualRight = struct("key" -> StringType, "b" -> IntegerType)
          )
        }
        // right missing
        a[AssertionError] should be thrownBy {
          checkFastDeepDiff(
            left = struct("key" -> StringType, "a" -> LongType),
            right = struct("key" -> StringType, "b" -> IntegerType),
            actualLeft = struct("key" -> StringType, "a" -> LongType),
            actualRight = struct("key" -> StringType)
          )
        }
      }
      "any field is unexpected" in {
        // left unexpected
        a[AssertionError] should be thrownBy {
          checkFastDeepDiff(
            left = struct("key" -> StringType, "a" -> LongType),
            right = struct("key" -> StringType, "a" -> LongType),
            actualLeft = struct("key" -> StringType, "a" -> LongType, "c" -> StringType),
            actualRight = struct("key" -> StringType, "a" -> LongType)
          )
        }
        // right unexpected
        a[AssertionError] should be thrownBy {
          checkFastDeepDiff(
            left = struct("key" -> StringType, "a" -> LongType),
            right = struct("key" -> StringType, "a" -> LongType),
            actualLeft = struct("key" -> StringType, "a" -> LongType),
            actualRight = struct("key" -> StringType, "a" -> LongType, "c" -> StringType)
          )
        }
      }
      "any field has a different type" in {
        // left different
        a[AssertionError] should be thrownBy {
          checkFastDeepDiff(
            left = struct("key" -> StringType, "a" -> LongType),
            right = struct("key" -> StringType, "b" -> IntegerType),
            actualLeft = struct("key" -> StringType, "a" -> BinaryType),
            actualRight = struct("key" -> StringType, "b" -> IntegerType)
          )
        }
        // right different
        a[AssertionError] should be thrownBy {
          checkFastDeepDiff(
            left = struct("key" -> StringType, "a" -> LongType),
            right = struct("key" -> StringType, "b" -> IntegerType),
            actualLeft = struct("key" -> StringType, "a" -> LongType),
            actualRight = struct("key" -> StringType, "b" -> BinaryType)
          )
        }
      }
      "the actual left and right schemas should be equal but aren't" in {
        a[AssertionError] should be thrownBy {
          checkFastDeepDiff(
            left = struct("key" -> StringType, "a" -> LongType, "b" -> IntegerType),
            right = struct("key" -> StringType, "a" -> LongType, "b" -> IntegerType),
            actualLeft = struct("key" -> StringType, "a" -> LongType, "b" -> IntegerType),
            actualRight = struct("key" -> StringType, "b" -> IntegerType, "a" -> LongType)
          )
        }
      }
    }
  }

  private def checkFastDeepDiff(left: StructType, right: StructType): Unit =
    checkFastDeepDiff(left, right, left, right)

  private def checkFastDeepDiff(left: StructType,
                                right: StructType,
                                actualLeft: StructType,
                                actualRight: StructType): Unit = {
    val deepDiff = new FastDeepDiff()(getSparkSession) {
      val config: DeepDiffConfig = DeepDiffConfig(keys = Set("key"))

      def leftSchema: StructType = left
      def rightSchema: StructType = right

      def leftDataFrame(schema: StructType): DataFrame =
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], actualLeft)
      def rightDataFrame(schema: StructType): DataFrame =
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], actualRight)

      // loading DataFrames with provided schemas shouldn't fail.
      def loadDataFrames(): Unit = leftAndRightDataFrames()
    }
    deepDiff.loadDataFrames()
  }
}
