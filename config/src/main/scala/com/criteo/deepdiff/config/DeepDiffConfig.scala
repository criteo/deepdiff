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

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions, ConfigValueFactory}

import scala.collection.JavaConverters._

sealed abstract class ToleranceSatisfactionCriterion(val name: String) extends Serializable
object ToleranceSatisfactionCriterion {
  case object All extends ToleranceSatisfactionCriterion(name = "all")
  case object Any extends ToleranceSatisfactionCriterion(name = "any")

  private[config] def apply(satisfies: String): ToleranceSatisfactionCriterion =
    satisfies match {
      case All.name => All
      case Any.name => Any
      case _ =>
        throw new IllegalArgumentException(s"Unsupported tolerances criterion $satisfies")
    }
}

final case class Tolerance(absolute: Option[Double] = None,
                           relative: Option[Double] = None,
                           satisfies: Option[ToleranceSatisfactionCriterion] = None) {
  require(absolute.isDefined || relative.isDefined || satisfies.isDefined)

  private[config] def toRawConfig: Config = {
    Seq[Option[Config => Config]](
      absolute.map(value => _.withValue("absolute", ConfigValueFactory.fromAnyRef(value))),
      relative.map(value => _.withValue("relative", ConfigValueFactory.fromAnyRef(value))),
      satisfies.map(value => _.withValue("satisfies", ConfigValueFactory.fromAnyRef(value.name)))
    ).foldLeft(ConfigFactory.empty()) {
      case (config, addValue) =>
        addValue.getOrElse(identity[Config](_))(config)
    }
  }
}

object Tolerance {
  val default: Tolerance =
    Tolerance(absolute = None, relative = None, satisfies = Some(ToleranceSatisfactionCriterion.All))

  private[config] def fromRawConfig(config: Config): Tolerance = {
    import ConfigUtils._
    Tolerance(
      absolute = config.getDoubleOption("absolute"),
      relative = config.getDoubleOption("relative"),
      satisfies = config.getStringOption("satisfies").map(ToleranceSatisfactionCriterion(_))
    )
  }
}

final case class ExplodedArrayConfig(keys: Set[String],
                                     multipleMatches: MultipleMatchesConfig = MultipleMatchesConfig.default) {
  require(keys.nonEmpty, "keys cannot be empty")

  private[config] def toRawConfig: Config =
    ConfigFactory
      .empty()
      .withValue("keys", ConfigValueFactory.fromIterable(keys.asJava))
      .withValue("multiple-matches", multipleMatches.toRawConfig.root())
}

object ExplodedArrayConfig {
  private[config] def fromRawConfig(config: Config): ExplodedArrayConfig = {
    import ConfigUtils._
    assert(config.hasPath("keys"))
    ExplodedArrayConfig(
      keys = config.getStringList("keys").iterator().asScala.toSet,
      multipleMatches = config
        .getConfigOption("multiple-matches")
        .map(MultipleMatchesConfig.fromRawConfig)
        .getOrElse(MultipleMatchesConfig.default)
    )
  }
}

final case class MultipleMatchesConfig(omitIfAllIdentical: Boolean) {
  private[config] def toRawConfig: Config =
    ConfigFactory
      .empty()
      .withValue("omit-if-all-identical", ConfigValueFactory.fromAnyRef(omitIfAllIdentical))
}

object MultipleMatchesConfig {
  val default: MultipleMatchesConfig = MultipleMatchesConfig(omitIfAllIdentical = false)

  private[config] def fromRawConfig(config: Config): MultipleMatchesConfig = {
    import ConfigUtils._
    MultipleMatchesConfig(
      omitIfAllIdentical = config.getBooleanOption("omit-if-all-identical").getOrElse(default.omitIfAllIdentical)
    )
  }
}

final case class IgnoreConfig(fields: Set[String], leftOnly: Boolean, rightOnly: Boolean) {
  private[config] def toRawConfig: Config =
    ConfigFactory
      .empty()
      .withValue("fields", ConfigValueFactory.fromIterable(fields.asJava))
      .withValue("left-only", ConfigValueFactory.fromAnyRef(leftOnly))
      .withValue("right-only", ConfigValueFactory.fromAnyRef(rightOnly))
}

object IgnoreConfig {
  val default: IgnoreConfig = IgnoreConfig(Set.empty, leftOnly = false, rightOnly = false)

  private[config] def fromRawConfig(config: Config): IgnoreConfig = {
    import ConfigUtils._
    IgnoreConfig(
      fields = config.getStringListOption("fields").getOrElse(default.fields).toSet,
      leftOnly = config.getBooleanOption("left-only").getOrElse(default.leftOnly),
      rightOnly = config.getBooleanOption("right-only").getOrElse(default.rightOnly)
    )
  }
}

/** Configuration used for the Deep Diff.
  *
  * In this context, the fullName designates the path of the fields using "." as field separator. For example a nested
  * field "A" in the struct "STRUCT" has the fullName "STRUCT.A".
  *
  * Aliases are treated first, so all configuration parameters using fields such as keys or ignoredFields, MUST take
  * them into account.
  *
  * @param keys Key fields fullName. While their order doesn't matter for the grouping, it usually will be used when
  *             presenting the examples.
  * @param maxExamples Maximum number of examples to keep for the report.
  * @param leftAliases Aliases to be used on the left side. The Mapping expects the fullName as the key and the alias
  *                    as the value.
  * @param rightAliases Same as leftAliases, but for the right side.
  * @param ignore [[IgnoreConfig]] specifying what must be ignored globally.
  * @param multipleMatches [[MultipleMatchesConfig]] specifying how multiple matches, on the root-level, should be handled.
  * @param explodedArrays Array of structs may use the same grouping strategy, if specified the provided keys will be
  *                       used to group the records within the array. Multiple matches with those keys may also be omitted
  *                       if all identical like the root-level records, but configuration is specific to this exploded array.
  *                       Root-level [[MultipleMatchesConfig]] won't be taken into account.
  * @param tolerances Tolerances are supported for numerical fields whether relative and/or absolute. One can also
  *                   choose whether both need be satisfied or not. Tolerances are propagated downwards. If specified
  *                   on a struct or an array of structs, all of the nested fields will use them unless they override
  *                   them. Defaults to no tolerance at all and both tolerance must be respected. Note that a tolerance
  *                   set to 0 is equivalent to no tolerance at all. A Mapping of the field's fullName to a
  *                   [[Tolerance]] is expected.
  */
final case class DeepDiffConfig(keys: Set[String],
                                maxExamples: Int = DeepDiffConfig.defaultMaxExamples,
                                ignore: IgnoreConfig = IgnoreConfig.default,
                                leftAliases: Map[String, String] = Map.empty,
                                rightAliases: Map[String, String] = Map.empty,
                                multipleMatches: MultipleMatchesConfig = MultipleMatchesConfig.default,
                                explodedArrays: Map[String, ExplodedArrayConfig] = Map.empty,
                                defaultTolerance: Tolerance = Tolerance.default,
                                tolerances: Map[String, Tolerance] = Map.empty) {

  override def toString: String = {
    val rawString = toRawConfig.root().render(ConfigRenderOptions.defaults().setOriginComments(false).setJson(false))
    s"DataFrameDeepDiffConfig(\n$rawString)"
  }

  def toRawConfig: Config =
    ConfigFactory
      .empty()
      .withValue("keys", ConfigValueFactory.fromIterable(keys.asJava))
      .withValue("max-examples", ConfigValueFactory.fromAnyRef(maxExamples))
      .withValue("ignore", ignore.toRawConfig.root())
      .withValue("multiple-matches", multipleMatches.toRawConfig.root())
      .withValue("left-aliases", ConfigValueFactory.fromMap(leftAliases.asJava))
      .withValue("right-aliases", ConfigValueFactory.fromMap(rightAliases.asJava))
      .withValue("exploded-arrays", ConfigValueFactory.fromMap(explodedArrays.mapValues(_.toRawConfig.root()).asJava))
      .withValue("default-tolerance", defaultTolerance.toRawConfig.root())
      .withValue("tolerances", ConfigValueFactory.fromMap(tolerances.mapValues(_.toRawConfig.root()).asJava))
}

object DeepDiffConfig {
  import ConfigUtils._

  val defaultMaxExamples: Int = 50
  val defaultIgnoreLeftOrRightOnlyFields: Boolean = false

  /** Example configuration:
    * {{{
    *   keys = ["key"]
    *   max-examples = 100
    *   ignore = {
    *     // left and right only fields will be ignored
    *     left-only = true
    *     right-only = true
    *     fields = [
    *       "count",
    *       "events.name", // ignoring field "name" in the records of the array "events" (exploded or not)
    *       "nested.value" // ignoring nested field "value"
    *     ]
    *    }
    *   // identical multiple matches, which does NOT take into account tolerances, can be omitted.
    *   // If so, they will be treated as any other pair of records without multiple matches.
    *   multiple-matches.omit-if-all-identical = true
    *   exploded-arrays {
    *     "events": {
    *        keys = ["key", "key2"]
    *        // Same configuration as root-level, but specific to this exploded array.
    *        // root-level configuration is never taken into account for the exploded array
    *        multiple-matches.omit-if-all-identical = true
    *      }
    *   }
    *   // All tolerances are propagated downwards
    *   default-tolerance = { absolute = 1e-3 }
    *   tolerances {
    *     "a": { absolute = 1e-3 }
    *     "b": { relative = .1 }
    *     // One can specify whether only either absolute or relative must be satisfied
    *     // with satisfies = "any". The default is "all"
    *     "nested.c": { absolute = 1e-4, relative = .01, satisfies = "any" }
    *   }
    *   left-aliases {
    *     "d": "dd"
    *     "nested.e": "ee"
    *   }
    *   right-aliases {
    *     "f": "ff"
    *     "nested.h": "hh"
    *   }
    * }}}
    */
  def fromRawConfig(config: Config): DeepDiffConfig = {
    if (config.getIntOption("version").exists(_ != 2)) {
      throw new IllegalArgumentException("Only version two is supported.")
    }
    new DeepDiffConfig(
      keys = config.getStringList("keys").asScala.toSet,
      ignore = config.getConfigOption("ignore").map(IgnoreConfig.fromRawConfig).getOrElse(IgnoreConfig.default),
      maxExamples = config.getIntOption("max-examples").getOrElse(defaultMaxExamples),
      leftAliases = getAliases(config, "left-aliases"),
      rightAliases = getAliases(config, "right-aliases"),
      multipleMatches = config
        .getConfigOption("multiple-matches")
        .map(MultipleMatchesConfig.fromRawConfig)
        .getOrElse(MultipleMatchesConfig.default),
      explodedArrays = getFieldConf(config, "exploded-arrays", ExplodedArrayConfig.fromRawConfig),
      defaultTolerance = config
        .getConfigOption("default-tolerance")
        .map(Tolerance.fromRawConfig)
        .map(tolerance => {
          // If not overridden explicitly, we use the values from Tolerance.default.
          Tolerance(
            absolute = tolerance.absolute.orElse(Tolerance.default.absolute),
            relative = tolerance.relative.orElse(Tolerance.default.relative),
            satisfies = tolerance.satisfies.orElse(Tolerance.default.satisfies)
          )
        })
        .getOrElse(Tolerance.default),
      tolerances = getFieldConf(config, "tolerances", Tolerance.fromRawConfig)
    )
  }

  private def getAliases(config: Config, path: String): Map[String, String] =
    config
      .getConfigOption(path)
      .map(
        _.root()
          .unwrapped()
          .asScala
          .toMap
          .map({
            case (field: String, alias: String) => field -> alias
          }))
      .getOrElse(Map.empty)

  private def getFieldConf[T](config: Config, path: String, parseConf: Config => T): Map[String, T] =
    config
      .getConfigOption(path)
      .map(c => c.root().keySet().asScala.map(key => key -> parseConf(c.getConfig(s""""$key""""))).toMap)
      .getOrElse(Map.empty)
}
