package examples

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

class MyBaseRelation(val sparkSession: SparkSession) extends BaseRelation with PrunedFilteredScan {

  val DATA = Seq(
    Row(0, "Alice", 20, "developer"),
    Row(1, "Bob", 35, "tester"),
    Row(2, "Homer", 40, "developer"),
    Row(3, "Marge", 23, "developer"),
    Row(4, "Bart", 29, "designer"),
    Row(5, "Lisa", 43, "developer"),
    Row(6, "Maggie", 20, "tester"),
    Row(7, "Frodo", 33, "developer"),
    Row(8, "Sam", 50, "team lead"),
    Row(9, "Gandalf", 50, "db engineer")
  )

  override def sqlContext: SQLContext = this.sparkSession.sqlContext

  override def schema: StructType = {
    StructType(
      Array(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = true),
        StructField("age", IntegerType, nullable = true),
        StructField("position", StringType, nullable = true)
      )
    )
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val seq = for (row <- DATA if passFilters(row, filters))
      yield Row.fromSeq(for (column <- requiredColumns)
        yield row.get(schema.fieldIndex(column)))
    println("Number of returned rows: " + seq.size)
    sparkSession.sparkContext.parallelize(seq)
  }

  def passFilters(row: Row, filters: Array[Filter]): Boolean = {
    for (filter <- filters) {
      filter match {
        case IsNotNull(x) =>
          if (row.get(schema.fieldIndex(x)) == null) return false
        case EqualTo(attribute, value) =>
          val index = schema.fieldIndex(attribute)
          if (index == -1 || !row.get(index).equals(value)) return false

        case LessThanOrEqual(attribute, value) =>
          if (!checkInequality(row, attribute, value, (a: Double, b: Double) => a <= b)) return false
        case LessThan(attribute, value) =>
          if (!checkInequality(row, attribute, value, (a: Double, b: Double) => a < b)) return false
        case GreaterThanOrEqual(attribute, value) =>
          if (!checkInequality(row, attribute, value, (a: Double, b: Double) => a >= b)) return false
        case GreaterThan(attribute, value) =>
          if (!checkInequality(row, attribute, value, (a: Double, b: Double) => a > b)) return false

        case _ => // ignore other filters
      }
    }
    true
  }

  def checkInequality(row: Row, attribute: String, value: Any, comparison: (Double, Double) => Boolean): Boolean = {
    val index = schema.fieldIndex(attribute)
    val doubleVal = schema.fields(index).dataType match {
      case _: IntegerType => row.get(index).asInstanceOf[Integer].doubleValue()
      case _: DoubleType => row.get(index).asInstanceOf[Double]
      case _ => return true
    }
    value match {
      case i: Integer => comparison(doubleVal, i.doubleValue())
      case d: Double => comparison(doubleVal, d)
      case _ => true
    }
  }

}
