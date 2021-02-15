package examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Demo extends App {
  // reduce log output
  Logger.getLogger("org").setLevel(Level.ERROR)

  // SparkSession that is used to create the DataFrames
  val session = SparkSession
    .builder
    .config("spark.master", "local[*]")
    .getOrCreate

  // DataFrame with a filter that checks equality to a String
  val dataFramePosition = session
    .read.option("header", value = true)
    .csv("src/main/resources/data.csv")
    .filter(col("position") === "tester")
  dataFramePosition.show()
  dataFramePosition.explain()

  // math expression is not pushed down, because a cast is required
  val dataFrameAge = session
    .read.option("header", value = true)
    .csv("src/main/resources/data.csv")
    .filter(col("age") <= 25)
  dataFrameAge.show()
  dataFrameAge.explain()

  // schema that defines id column and age column as integers
  val schema = StructType(Array(
    StructField("id", IntegerType, nullable = true),
    StructField("name", StringType, nullable = true),
    StructField("age", IntegerType, nullable = true),
    StructField("position", StringType, nullable = true)))

  // math expression can be pushed down, because we defined the age field as IntegerType
  val dataFrameSchema = session
    .read.option("header", value = true)
    .schema(schema)
    .csv("src/main/resources/data.csv")
    .filter(col("age") <= 25)
  dataFrameSchema.show()
  dataFrameSchema.explain()

  // same operations on the custom base relation
  var dataFrameBaseRelation = session
    .baseRelationToDataFrame(new MyBaseRelation(session))
    .filter(col("position") === "tester")
    .filter(col("age") <= 25)
  dataFrameBaseRelation.show()
  dataFrameBaseRelation.explain()

}
