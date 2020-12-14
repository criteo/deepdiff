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

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class DeepDiffConfigSpec extends AnyFlatSpec with Matchers {

  it should "use sensible defaults for everything except keys" in {
    val rawConfig = ConfigFactory.parseString( // language=HOCON
      """keys = ["key"]
        |""".stripMargin)

    val defaultConfig = DeepDiffConfig(keys = Set("key"))
    DeepDiffConfig.fromRawConfig(rawConfig) should be(defaultConfig)
    defaultConfig.leftAliases should be('empty)
    defaultConfig.rightAliases should be('empty)
    defaultConfig.explodedArrays should be('empty)
    defaultConfig.defaultTolerance.absolute should be(None)
    defaultConfig.defaultTolerance.relative should be(None)
    defaultConfig.defaultTolerance.satisfies.foreach(_ should be(ToleranceSatisfactionCriterion.All))
    defaultConfig.tolerances should be(Map.empty)
    defaultConfig.ignore.fields should be('empty)
    defaultConfig.ignore.leftOnly should be(false)
    defaultConfig.ignore.rightOnly should be(false)
    defaultConfig.maxExamples should (be > 0 and be < 1000)
    defaultConfig.multipleMatches.omitIfAllIdentical should be(false)
  }

  it should "extract correctly the configuration from a typesafe config" in {
    val rawConfig = ConfigFactory.parseString( // language=HOCON
      """keys = ["key1", "nested.key2"]
        |max-examples = 77
        |ignore {
        |  left-only = true
        |  right-only = true
        |  fields = ["x", "nested.y"]
        |}
        |multiple-matches {
        |  omit-if-all-identical = true
        |}
        |// everything else will take into account aliases
        |left-aliases {
        |  "fieldA": "aliasA"
        |  "structB": "aliasStructB"
        |  "structB.nestedFieldC": "aliasNestedFieldC"
        |}
        |right-aliases {
        |  "fieldE": "aliasE"
        |  "structF": "aliasStructF"
        |  "structF.nestedFieldG": "aliasNestedFieldG"
        |}
        |exploded-arrays {
        |  "arrayFieldA": { keys = ["key1", "key2"] }
        |  "structB.arrayFieldC": { keys = ["c"] }
        |  "structB.structE.arrayFieldF": { 
        |    keys = ["e"]
        |    multiple-matches.omit-if-all-identical = true
        |  }
        |}
        |default-tolerance {
        |  absolute = 1e-3
        |}
        |// Tolerances are propagated downwards, the closest definition to the field wins.
        |tolerances {
        |  "fieldA": { relative = 10 }
        |  // applied on all structB fields and overrides default tolerance
        |  "structB": { absolute = 1, relative = .1 }
        |  // override structB tolerances
        |  "structB.fieldD": {
        |    relative = .2,
        |    satisfies = "any" // Either absolute or relative tolerance must be satisfied
        |  }
        |}
        |""".stripMargin)

    DeepDiffConfig.fromRawConfig(rawConfig) should be(
      DeepDiffConfig(
        keys = Set("key1", "nested.key2"),
        maxExamples = 77,
        ignore = IgnoreConfig(
          fields = Set("x", "nested.y"),
          leftOnly = true,
          rightOnly = true
        ),
        leftAliases = Map(
          "fieldA" -> "aliasA",
          "structB" -> "aliasStructB",
          "structB.nestedFieldC" -> "aliasNestedFieldC"
        ),
        rightAliases = Map(
          "fieldE" -> "aliasE",
          "structF" -> "aliasStructF",
          "structF.nestedFieldG" -> "aliasNestedFieldG"
        ),
        multipleMatches = MultipleMatchesConfig(omitIfAllIdentical = true),
        explodedArrays = Map(
          "arrayFieldA" -> ExplodedArrayConfig(keys = Set("key1", "key2")),
          "structB.arrayFieldC" -> ExplodedArrayConfig(keys = Set("c")),
          "structB.structE.arrayFieldF" -> ExplodedArrayConfig(keys = Set("e"),
                                                               MultipleMatchesConfig(omitIfAllIdentical = true))
        ),
        defaultTolerance = Tolerance(absolute = Some(1e-3), satisfies = Some(ToleranceSatisfactionCriterion.All)),
        tolerances = Map(
          "fieldA" -> Tolerance(relative = Some(10)),
          "structB" -> Tolerance(absolute = Some(1), relative = Some(.1)),
          "structB.fieldD" -> Tolerance(relative = Some(.2), satisfies = Some(ToleranceSatisfactionCriterion.Any))
        )
      ))
  }

  it should "be able to read its own serialization to config" in {
    for (
      config <- Seq(
        DeepDiffConfig(keys = Set("key")),
        DeepDiffConfig(
          keys = Set("key1", "nested.key2"),
          maxExamples = 77,
          multipleMatches = MultipleMatchesConfig(omitIfAllIdentical = true),
          ignore = IgnoreConfig(
            fields = Set("x", "nested.y"),
            leftOnly = true,
            rightOnly = true
          ),
          leftAliases = Map(
            "fieldA" -> "aliasA",
            "structB" -> "aliasStructB",
            "structB.nestedFieldC" -> "aliasNestedFieldC",
            "structB.nestedFieldD" -> "aliasNestedFieldD",
            "structC.nestedFieldX" -> "aliasNestedFieldX"
          ),
          rightAliases = Map(
            "fieldE" -> "aliasE",
            "structF" -> "aliasStructF",
            "structF.nestedFieldG" -> "aliasNestedFieldG",
            "structF.nestedFieldH" -> "aliasNestedFieldH"
          ),
          explodedArrays = Map(
            "arrayFieldA" -> ExplodedArrayConfig(keys = Set("key1", "key2")),
            "structB.arrayFieldC" -> ExplodedArrayConfig(keys = Set("c")),
            "structB.arrayFieldD" -> ExplodedArrayConfig(keys = Set("d"),
                                                         MultipleMatchesConfig(omitIfAllIdentical = true)),
            "structB.structE.arrayFieldF" -> ExplodedArrayConfig(keys = Set("e"))
          ),
          defaultTolerance = Tolerance(absolute = Some(1e-3), satisfies = Some(ToleranceSatisfactionCriterion.All)),
          tolerances = Map(
            "fieldA" -> Tolerance(relative = Some(10)),
            "structB" -> Tolerance(absolute = Some(1), relative = Some(.1)),
            "structB.fieldD" -> Tolerance(relative = Some(.2), satisfies = Some(ToleranceSatisfactionCriterion.Any))
          )
        )
      )
    ) {
      DeepDiffConfig.fromRawConfig(config.toRawConfig) should be(config)
    }
  }
}
