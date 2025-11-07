// Imports
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

def nafill[T <: Any]( /*spark: org.apache.spark.sql.SparkSession,*/ df: DataFrame, colNames: Array[String], defaults: Map[String, T] = Map("StringType" -> "N/A", "IntegerType" -> 0, "DateType" -> "3099-12-31", "TimestampType" -> "3099-12-31 00:00:00", "LongType" -> 0L, "DoubleType" -> 0.0, "BooleanType" -> true)): DataFrame = {

    /**
     * Arguments - SparkSession, Dataframe to Transform
     * ColNames - List of Columns for which Nulls needs to be handled
     * defaults - Map consisting of Datatype (as String) -> Default Value
     *
     */

    /*import spark.implicits._*/

    val strCols = df.schema.fields.filter(f => f.dataType == StringType).map(_.name).intersect(colNames)
    val intCols = df.schema.fields.filter(f => f.dataType == IntegerType).map(_.name).intersect(colNames)
    val boolCols = df.schema.fields.filter(f => f.dataType == BooleanType).map(_.name).intersect(colNames)
    val longCols = df.schema.fields.filter(f => f.dataType == LongType).map(_.name).intersect(colNames)
    val realCols = df.schema.fields.filter(f => f.dataType == DoubleType || f.dataType == FloatType).map(_.name).intersect(colNames)
    val dateCols = df.schema.fields.filter(f => f.dataType == DateType).map(_.name).intersect(colNames)
    val timestampCols = df.schema.fields.filter(f => f.dataType == TimestampType).map(_.name).intersect(colNames)

    val tempdf = df
      .na.fill(defaults.getOrElse("StringType", "N/A").asInstanceOf[String], strCols)
      .na.fill(defaults.getOrElse("IntegerType", 0).asInstanceOf[Int].toDouble, intCols)
      .na.fill(defaults.getOrElse("BooleanType", true).asInstanceOf[Boolean], boolCols)
      .na.fill(defaults.getOrElse("LongType", 0L).asInstanceOf[Long], longCols)
      .na.fill(defaults.getOrElse("DoubleType", 0.0).asInstanceOf[Double], realCols)

    val nameType = df.schema.fields.map(f => (f.name, f.dataType))
    val selectExpr = nameType.map(f =>

      {
        val defaultDate = defaults.getOrElse("DateType", "3099-01-01").asInstanceOf[String]
        val defaultTs = defaults.getOrElse("TimestampType", "3099-01-01 00:00:00").asInstanceOf[String]

        if (colNames.contains(f._1)) {
          f._2 match {

            case DateType      => expr(s"CASE WHEN ${f._1} IS NULL THEN to_date('${defaultDate}','yyyy-MM-dd') ELSE ${f._1}  END AS ${f._1}")
            case TimestampType => expr(s"CASE WHEN ${f._1} IS NULL THEN to_timestamp('${defaultTs}','yyyy-MM-dd HH:mm:ss') ELSE ${f._1} END AS ${f._1}")
            case _             => expr(s"${f._1}")

          }

        } else expr(s"${f._1}")
      })

    tempdf.select(selectExpr: _*)

  }

  def cleanHeaders( /*spark: org.apache.spark.sql.SparkSession,*/ inputDF: DataFrame): DataFrame = {

    /*import spark.implicits._ */

    //val remRegx = """([\(,\),\,,\;,\{,\},\n,\t,\r,\$,#,\-,–,/,\.])""".r
    val remRegx = """([^a-zA-Z0-9])""".r
    val multspRegx = """[\s]{2,}""".r
    val wspwRegx = """([\s])""".r
    val rem2Regx = """(_$)""".r
    val outCols: Array[String] = inputDF.columns.map(col =>
      rem2Regx.replaceFirstIn(
        wspwRegx.replaceAllIn(
          multspRegx.replaceAllIn(
            remRegx.replaceAllIn(col, " "),
            " "),
          "_"),
        ""))

    inputDF.toDF(outCols: _*)
  }

  def replaceString(df: DataFrame, cols: Array[String], remRegx: scala.util.matching.Regex, rString: String) = {
    val allCols = df.columns
    val restCols = allCols.diff(cols).map(col(_))
    val selectCols = cols.map(x => regexp_replace(col(x), remRegx.regex, rString).alias(x + "_new"))
    val renameCols = cols.map(x => (col(x + "_new").alias(x)))
    df.select((restCols ++ selectCols): _*).select((restCols ++ renameCols): _*).select(allCols.map(col(_)): _*)
  }

  def cleanString(df: DataFrame, cols: Array[String], remRegx: scala.util.matching.Regex) = {
    val allCols = df.columns
    val restCols = allCols.diff(cols).map(col(_))
    val selectCols = cols.map(x => regexp_replace(col(x), remRegx.regex, "").alias(x + "_new"))
    val renameCols = cols.map(x => (col(x + "_new").alias(x)))
    df.select((restCols ++ selectCols): _*).select((restCols ++ renameCols): _*).select(allCols.map(col(_)): _*)
  }
