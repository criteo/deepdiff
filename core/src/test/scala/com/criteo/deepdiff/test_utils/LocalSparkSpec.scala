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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait LocalSparkSpec extends BeforeAndAfterAll { _: Suite =>
  override def beforeAll(): Unit = {
    SparkSession
      .builder()
      .config(
        new SparkConf(false)
          .setMaster("local[2]")
          .set("spark.default.parallelism", "4")
          .set("spark.sql.shuffle.partitions", "4")
          .set("spark.eventLog.enabled", "false")
          .set("spark.ui.enabled", "false")
          .set("spark.sql.catalogImplementation", "in-memory")
      )
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  // Can only be used within a test (a in { ... } clause)
  protected def getSparkSession: SparkSession = SparkSession.getActiveSession.get
}
