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
import com.criteo.deepdiff.test_utils.DeepDiffSpec.DeepDiffTestCase
import com.criteo.deepdiff.test_utils.{DatasetDiffsBuilder, DeepDiffSpec}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.TimestampType

import java.util.TimeZone

object TimeTypeSpec {
  final case class Record(key: String, date: String)
}

final class TimeTypeSpec extends DeepDiffSpec {
  import TimeTypeSpec._
  import com.criteo.deepdiff.test_utils.EasyKeyExample._

  "DeepDiff" should {
    "support the date type" when runningDeepDiffTestCase {
      import org.apache.spark.sql.functions._
      val left = Seq(
        Record("identical", "2020-01-01"),
        Record("left-only", "2000-01-01"),
        Record("right-only", null),
        Record("different", "2020-01-01")
      )
      val right = Seq(
        Record("identical", "2020-01-01"),
        Record("left-only", null),
        Record("right-only", "2010-01-01"),
        Record("different", "2021-01-01")
      )
      def read(rows: Seq[Record]) =
        (spark: SparkSession) => {
          import spark.implicits._
          TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
          spark
            .createDataFrame(rows)
            .select($"key", to_date($"date").as("date"))
        }
      DeepDiffTestCase
        .defaultConf()
        .compare(
          left = read(left),
          right = read(right)
        )
        .expect(
          DatasetDiffsBuilder[KeyExample, Product]()
            .kindOfDifferent(content = 1, leftOnly = 1, rightOnly = 1)
            .record(
              identical = 1,
              diffExamples = left.zip(right).drop(1).map({ case (l, r) => (Key(l.key), l, r) })
            )
            .common("key", identical = 4)
            .common(
              "date",
              identical = 1,
              diffExamples = Seq((Key("different"), "2020-01-01", "2021-01-01")),
              leftExamples = Seq((Key("left-only"), "2000-01-01")),
              rightExamples = Seq((Key("right-only"), "2010-01-01"))
            )
        )
    }

    "support the timestamp type" when runningDeepDiffTestCase {
      import org.apache.spark.sql.functions._
      val left = Seq(
        Record("identical", "2020-01-01 10:23:34"),
        Record("left-only", "2000-01-01 00:00:00"),
        Record("right-only", null),
        Record("different", "2020-01-01 00:00:00")
      )
      val right = Seq(
        Record("identical", "2020-01-01 10:23:34"),
        Record("left-only", null),
        Record("right-only", "2010-01-01 00:00:00"),
        Record("different", "2020-01-01 00:00:10")
      )
      def read(rows: Seq[Record]) =
        (spark: SparkSession) => {
          import spark.implicits._
          spark
            .createDataFrame(rows)
            .select($"key", unix_timestamp($"date", "yyyy-MM-dd HH:mm:ss").cast(TimestampType).as("date"))
        }
      DeepDiffTestCase
        .defaultConf()
        .compare(
          left = read(left),
          right = read(right)
        )
        .expect(
          DatasetDiffsBuilder[KeyExample, Product]()
            .kindOfDifferent(content = 1, leftOnly = 1, rightOnly = 1)
            .record(
              identical = 1,
              diffExamples = left.zip(right).drop(1).map({ case (l, r) => (Key(l.key), l, r) })
            )
            .common("key", identical = 4)
            .common(
              "date",
              identical = 1,
              diffExamples = Seq((Key("different"), "2020-01-01 00:00:00", "2020-01-01 00:00:10")),
              leftExamples = Seq((Key("left-only"), "2000-01-01 00:00:00")),
              rightExamples = Seq((Key("right-only"), "2010-01-01 00:00:00"))
            )
        )
    }
  }
}
