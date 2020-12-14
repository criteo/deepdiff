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

package com.criteo.deepdiff.config

import com.typesafe.config.Config

import scala.collection.JavaConverters._

private[config] object ConfigUtils {
  implicit class RichConfig(config: Config) {
    private def getOption[T](path: String, getter: Config => String => T): Option[T] =
      if (config.hasPath(path)) Some(getter(config)(path)) else None

    def getIntOption(path: String): Option[Int] = getOption(path, _.getInt)
    def getDoubleOption(path: String): Option[Double] = getOption(path, _.getDouble)
    def getStringOption(path: String): Option[String] = getOption(path, _.getString)
    def getBooleanOption(path: String): Option[Boolean] = getOption(path, _.getBoolean)
    def getConfigOption(path: String): Option[Config] = getOption(path, _.getConfig)
    def getStringListOption(path: String): Option[List[String]] = getOption(path, _.getStringList).map(_.asScala.toList)
  }
}
