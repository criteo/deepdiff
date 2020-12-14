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

package com.criteo.deepdiff.type_support.example

import org.apache.spark.sql.catalyst.util.DateTimeUtils

/** Used in both KeyBuffer and ExampleFactory to be consistent with date formatting. */
private[deepdiff] object DateTimeExampleUtils {
  def dateToString(raw: Int): String =
    DateTimeUtils.dateToString(raw) // Using Spark's conversion which relies on default TimeZone

  def timestampToString(raw: Long): String =
    DateTimeUtils.timestampToString(raw) // Using Spark's conversion which relies on default TimeZone
}
