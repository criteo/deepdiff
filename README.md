Deep Diff
=========

Deep Diff is a tool used at Criteo to compare datasets and return a comprehensible summary, count and examples, of 
their difference. The library presented here is a full rewrite of the original one for best performance on top of Spark
2.4. Currently this repository only contains the core functionality without the actual job or the friendly HTML report.

In short, the Deep Diff will group rows together by a given key and compare the found rows. It will count all the 
differences and keep to a certain number of examples.

Quick Start
-----------

### Using the core library

```scala
import org.apache.spark.sql.SparkSession
import com.criteo.deepdiff.{FastDeepDiff, DeepDiff}
import com.criteo.deepdiff.config.DeepDiffConfig
import com.typesafe.config.ConfigFactory

implicit val spark: SparkSession = SparkSession.builder().getOrCreate()
val config = DeepDiffConfig.fromRawConfig(ConfigFactory.parseString("""
keys = ["my-key"]
"""))

// most flexible
val deepDiff = new DeepDiff(config) {
  def leftAndRightDataFrames(): (DataFrame, DataFrame) = ???
}

// best performance, as it'll read only what is necessary and re-organize the schema for better comparisons
val fastDeepDiff = new FastDeepDiff(config) {
  def leftSchema: StructType = ???
  def rightSchema: StructType = ???

  // the dataframe is expected to read the data with the specified schema: spark.read.schema(...)
  def leftDataFrame(schema: StructType): DataFrame = ???
  def rightDataFrame(schema: StructType): DataFrame = ???
}

// Contains all the differences
val datasetDiffs = deepDiff.run() // or fastDeepDiff.run()
```

### Configuration Specification

```hocon
// Fields used to group the rows. Nested fields can also be used through the dot notation. Most types
// are supported except data structure such as map or arrays.
keys = ["key1", "nested.key2"]
// Maximum number of examples to keep for the datasetDiff.
max-examples = 50
ignore {
  // Whether left-only fields should be ignored
  left-only = false
  // Same for right-only fields
  right-only = false
  // Fields to ignore
  fields = ["x", "nested.y"]
}
// Multiple matches are multiple rows on the left and/or right side that match the same key. 
// They will appear in the datasetDiff with the count and examples.
multiple-matches {
  // If all rows on one side (left/right) are equal they can be treated as a single row, we're omitting all the other
  // matches. For example if for a key K we match (Left1, Left2) and (Right) and if Left1 equals Left2, Left2 will just
  // be ignored. Multiple matches comparison DO NOT use the tolerances.
  omit-if-all-identical = false
}
// Aliases are used to rename columns for either side. The format is "full path" -> "new field name". The new names
// must be used everywhere else in the configuration.
left-aliases {
  "fieldA": "aliasA"
  "structB": "aliasStructB"
  "structB.nestedFieldC": "aliasNestedFieldC"
}
right-aliases {
  "fieldE": "aliasE"
  "structF": "aliasStructF"
  "structF.nestedFieldG": "aliasNestedFieldG"
}
// For arrays of structs, deep diff can apply the same algorithm on those structs. A key is used to match those from both
// sides. It is named "exploded-arrays" as functionnaly it ressembles somewhat what an EXPLODE in SQL, but it has barely
// any performance impact. When exploded-arrays is not used, arrays of struct are considered to be an ordered collection
// of structs and the position will be used to match the structs.
exploded-arrays {
  "arrayFieldA": { keys = ["key1", "key2"] }
  "structB.arrayFieldC": { keys = ["c"] }
  "structB.structE.arrayFieldF": { 
    keys = ["e"]
    multiple-matches.omit-if-all-identical = false
  }
}
// Default tolerances applied to all fields. A tolerance of 0 is equivalent to no tolerance at all.
default-tolerance {
  // abs(x - y) <= absolute
  absolute = 0
  // abs(x - y) / min(abs(x), abs(y)) <= relative
  relative = 0
  // When both absolute and relative tolerances are specified, one can choose that a field should either satisfy "all"
  // of them or "any" of them.
  satisfies = "all"
}
// Field-specifics tolerances. Those can be applied on arrays and structs, nested fields within them will take those 
// into account.
tolerances {
  "fieldA": { relative = 10 }
  // applied on all structB fields and overrides default tolerance
  "structB": { absolute = 1, relative = .1 }
  // override structB tolerances
  "structB.fieldD": {
    relative = .2,
    satisfies = "any" // Either absolute or relative tolerance must be satisfied
  }
}
```

Spark parameters & Performance
------------------------------

Executors having 8 GB of RAM & 4 tasks on them (each having roughly 1 cpu core) were enough to compare datasets with
several hundreds of fields and nested structures. So it is a good starting point.

It could compare 4 TB of data without any issue. Most of the time (80-90%) is spent in the DataFrame creation and the 
shuffle itself. It consumes 2-4 times less resources compared to a similar Spark application as deep diff avoids all
intermediate serialization from Spark and manipulates the binary data (UnsafeRows) directly.

License
-------

[Apache License v2.0](https://github.com/criteo/deepdiff/blob/main/LICENSE)
