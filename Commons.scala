//Imports

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

def StructToString(schema: StructType): String = {
    schema.fields.map(x => (s"""StructField("${x.name}",${x.dataType},${x.nullable})""")).mkString(",")
  }

  def StructToConfArray(schema: StructType): Array[String] = {
    schema.fields.map(x => (s"""${x.name},${x.dataType},${x.nullable}"""))
  }

  def StrtoConfJson(attrList: Array[String], sep: Char = ','): String = {
    attrList.map(_.split(sep)).map(x => s"""{"name": "${x(0)}","DataType": "${x(1)}","Nullable": "${x(2)}"}""").mkString(",")
  }

  def StructToConfJson(schema: StructType): String = {
    val attrList = StructToConfArray(schema)
    StrtoConfJson(attrList, ',')

  }
