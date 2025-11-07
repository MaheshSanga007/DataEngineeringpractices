%run "/APMUtils/DQMethods"
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
//import APMUtils.DQMethods._
import com.crealytics.spark.excel._
import org.apache.poi._
import org.apache.poi.ooxml._
import java.time.format._
import java.sql.Timestamp
import java.time.LocalDateTime


import scala.collection.mutable.Map
import io.delta.tables._   
    def deltaUpsertSCDType1(stageDF: DataFrame,
                            targetDeltaLocation: String,
                            naturalKeys: Array[String]) = {
      
      val columnListToBeUpdated = stageDF.columns.diff(naturalKeys)
      val columnsToBeInserted = stageDF.columns
      var columnsToUpdateMap = Map.empty[String, String]
      columnListToBeUpdated.map { x =>
        columnsToUpdateMap += x -> s"incrementTbl.$x"
      }
      var columnsToInsertMap = Map.empty[String, String]
      columnsToBeInserted.map { x =>
        columnsToInsertMap += x -> s"incrementTbl.$x"
      }
      val joinConditionExpr = naturalKeys.map(x => (col(s"deltaTbl.$x"), col(s"incrementTbl.$x")))
        .foldLeft(lit(true)) { (x, y) => (x && y._1 === y._2) }

      DeltaTable.forPath(spark, targetDeltaLocation)
        .as("deltaTbl")
        .merge(
          stageDF.as("incrementTbl"),
          joinConditionExpr)
        .whenMatched
        .updateExpr(
          columnsToUpdateMap)
        .whenNotMatched
        .insertExpr(
          columnsToInsertMap)
        .execute()
    }

def SCDType1(stageDF: DataFrame, targetDF: DataFrame, naturalKeys: Array[String], partCols: Array[String]): DataFrame = {

      val updatedColselector = ((stageDF.columns.diff(partCols).map("stage." ++ _)) ++
      partCols.map("target." ++ _)).map(org.apache.spark.sql.functions.col(_))
  
    val masterCols = (stageDF.columns.diff(partCols) ++ partCols).map(col)
  
    val joinCondition = naturalKeys.map(x => (col("target." + x), col("stage." + x))).foldLeft(lit(true)) { (x, y) => (x && y._1 === y._2) }

    val updated_rec_df = stageDF.alias("stage").join(targetDF.alias("target"), joinCondition, "inner").select(updatedColselector: _*).select(masterCols: _*)

    val unchaged_rec_df = targetDF.alias("target").join(stageDF.alias("stage"), joinCondition, "left_anti").select(col("target.*")).select(masterCols: _*)

    val new_rec_df = stageDF.alias("stage").join(targetDF.alias("target"), joinCondition, "left_anti").select(col("stage.*")).select(masterCols: _*)

    updated_rec_df.union(unchaged_rec_df).union(new_rec_df)
  }

  def SCDType2(stageDF: DataFrame, targetDF: DataFrame, naturalKeys: Array[String], scdCols: Array[String], MetadataCols: Array[String] = Array("Effective_From_Date", "Effective_To_Date", "Current_Flg"),
               partCols: Array[String], spark: org.apache.spark.sql.SparkSession): DataFrame = {

     import spark.implicits._

    val schemaSelector = targetDF.columns.map(col(_))
    val updatedColselector = ((stageDF.columns.diff(partCols).map("stage." ++ _)) ++
      partCols.map("target." ++ _)).map(org.apache.spark.sql.functions.col(_))

    val scdClause = scdCols.map(x => (col("target." + x), col("stage." + x))).foldLeft(lit(false)) { (x, y) => (x || y._1 =!= y._2) }

    val join_condition = naturalKeys.map(x => (col("target." + x), col("stage." + x))).foldLeft(lit(true)) { (x, y) => (x && y._1 === y._2) }
    val target_inner_join_stage = targetDF.alias("target").join(stageDF.alias("stage"), join_condition, "inner")
    val current_target_inner_join_stage = target_inner_join_stage.where($"target.Current_Flg" === "Y")

    // SCD Ops

    val unjoined_unchaged_rec_df = targetDF.alias("target").join(stageDF.alias("stage"), naturalKeys, "left_anti").select(col("target.*"))

    val joined_history_target = target_inner_join_stage.where($"target.Current_Flg" === "N").select(col("target.*")).select(schemaSelector: _*)

    // 2 Records, 1st existing target record, setting current to old
    val joined_scd_changed_target = current_target_inner_join_stage.where(scdClause).select(col("target.*")).drop("Effective_To_Date", "Current_Flg")
      .select(col("*"), date_add(current_date(), -1).alias("Effective_To_Date"), lit("N").alias("Current_Flg")).select(schemaSelector: _*)
    //2nd as insert, new current record
    val joined_scd_changed_stage = current_target_inner_join_stage.where(scdClause).select(col("stage.*")).select(schemaSelector: _*)

    // SCD Cols unchanged, Update
    val joined_scd_unchanged_stage = current_target_inner_join_stage.where(!scdClause).select(updatedColselector: _*)

    val new_rec_stage = stageDF.alias("stage").join(targetDF.alias("target"), naturalKeys, "left_anti").select(col("stage.*"))

    //Final
    unjoined_unchaged_rec_df.union(joined_history_target).union(joined_scd_changed_target).union(joined_scd_changed_stage).union(new_rec_stage)

  }

  // DataFrameReaders

  //SchemaDF : to InferSchema

  /**
   * OneTime Process
   * Step 1 - Create SchemaDF with Inference set to true. Limit 1000 Rows
   * Step 2 - Clean Headers
   * Step 3 - Edit Schema if Needed. Create StructType
   * Step 4 - Use CustomSchema to read Excel Files again
   */

  /*  lazy val SynschemaDF = spark.read

    .format("com.crealytics.spark.excel")
    .option("useHeader", "true") // Required
    .option("treatEmptyValuesAsNulls", "true") // Optional, default: true
    .option("inferSchema", "true") // Optional, default: false
    .option("addColorColumns", "false") // Optional, default: false
    .option("timestampFormat", "dd-MM-yyyy HH:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
    //.option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files
    .option("excerptSize", 1000) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
    .load("/mnt/mns_adls/raw/obupoc/Syngenta/\*.xlsx").limit(1000)
  //
  //Create SchemaDF With Cleaned up Headers
  val SynschemaDFCleaned = cleanHeaders(SynschemaDF)
  //
  // Schema
  SynschemaDFCleaned.schema.fields
  */

  /*val schemaDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("dateFormat", "dd-MM-yyyy")
      .option("timestampFormat", "dd-MM-yyyy HH:mm:ss")
      .load("C:\\TestData\\Raw\\DailyPO\\daily_po.csv").limit(500)

      val schemastr = StructToConfArray(cleanHeaders(schemaDF).schema)
      println(strtoAttrjson(schemastr))*/

  def ReadCSV(path: String, Schema: StructType,header:String= "true",spark: org.apache.spark.sql.SparkSession, datefmt: String = "dd-MM-yyyy", tsfmt: String = "dd-MM-yyyy HH:mm:ss"): DataFrame = {

    import spark.implicits._
    
    val naString:String ="#N/A"
    
    cleanHeaders(spark.read.format("csv")
      .option("header", header)
      .option("treatEmptyValuesAsNulls", "true")
      .option("nullValue", naString)
      .option("dateFormat", datefmt)
      .option("timestampFormat", tsfmt)
      .schema(Schema)
      .load(path))

  }



// def ReadExcel(path: String, Schema: StructType, spark: org.apache.spark.sql.SparkSession, datefmt: String = "dd-MM-yyyy", tsfmt: String = "dd-MM-yyyy HH:mm:ss"): DataFrame = {

//     /**
//      * Though Crealytics accept TimestampFormat Only
//      * You can Create CustomSchema with DateType and Date values in data will be typed to Date
//      */

//     cleanHeaders(spark.read
//       .format("com.crealytics.spark.excel")
//       //.option("dataAddress", dataAddress) // Optional, default: "A1" .. "'My Sheet'!B3:C35"
//       .option("useHeader", "false") // Required
//       .option("treatEmptyValuesAsNulls", "true") // Optional, default: true
//       .option("inferSchema", "false") // Optional, default: false
//       .option("addColorColumns", "false") // Optional, default: false
//       .option("timestampFormat", "dd-MM-yyyy HH:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
//       //.option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files
//       //.option("excerptSize", 1000) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
//       .schema(Schema)
//       .load(path))

//   }


//Overload with Dataaddress Param

   def ReadExcel(path: String, dataAddress: String = "A2", Schema: StructType, spark: org.apache.spark.sql.SparkSession, datefmt: String = "dd-MM-yyyy", tsfmt: String = "dd-MM-yyyy HH:mm:ss"): DataFrame = {

    /**
     * Though Crealytics accept TimestampFormat Only
     * You can Create CustomSchema with DateType and Date values in data will be typed to Date
     */

    cleanHeaders(spark.read
      .format("com.crealytics.spark.excel")
      .option("dataAddress", dataAddress) // Optional, default: "A1" .. "'My Sheet'!B3:C35" ..Keep default as A2 with customschema and header false
      .option("useHeader", "false") // Required
      .option("treatEmptyValuesAsNulls", "true") // Optional, default: true
      .option("inferSchema", "false") // Optional, default: false
      .option("addColorColumns", "false") // Optional, default: false
      .option("timestampFormat", tsfmt) // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
      //.option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files
      //.option("excerptSize", 1000) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
      .schema(Schema)
      .load(path))

  }

  // Some Datetime Arithmetics
  import java.util.Calendar
  import java.util.Date
  import java.util.GregorianCalendar
  import java.text.SimpleDateFormat

  def addDays(date: java.util.Date, days: Int): java.util.Date = {

    val cal = new GregorianCalendar()
    cal.setTime(date)
    cal.add(Calendar.DATE, days)
    cal.getTime()
  }
  val cal = new GregorianCalendar()
  val today = cal.getTime
  // val yday = addDays(today,-1)

  val yformat = new SimpleDateFormat("yyyy")
  val mformat = new SimpleDateFormat("MM")
  val dformat = new SimpleDateFormat("dd")
  val wformat = new SimpleDateFormat("w")
  val udateformat = new SimpleDateFormat("yyyy-MM-dd")
  val uTZdateformat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
  val utimeStampformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val batchIDFormat = new SimpleDateFormat("yyyyMMddHHmm")

  //val Batch_Run_ID = batchIDFormat.format(today).toLong

  def addCalPartCols(df: DataFrame, dateString: String, spark: org.apache.spark.sql.SparkSession): DataFrame = {
    import spark.implicits._
    val dateCol = org.apache.spark.sql.functions.to_date(lit(dateString))
    df.select($"*", year(dateCol).alias("Year"), month(dateCol).alias("Month"), dayofmonth(dateCol).alias("Day"))
  }

  def addCalPartCols(df: DataFrame, dateCol: org.apache.spark.sql.Column, spark: org.apache.spark.sql.SparkSession): DataFrame = {
    import spark.implicits._
    df.select($"*", year(dateCol).alias("Year"), month(dateCol).alias("Month"), dayofmonth(dateCol).alias("Day"))
  }

  def addCalPartCols(df: DataFrame, date: java.util.Date, spark: org.apache.spark.sql.SparkSession): DataFrame = {
    import spark.implicits._
    import java.util.Calendar
    import java.util.Date
    import java.util.GregorianCalendar
    import java.text.SimpleDateFormat
    val yformat = new SimpleDateFormat("yyyy")
    val mformat = new SimpleDateFormat("MM")
    val dformat = new SimpleDateFormat("dd")
    val wformat = new SimpleDateFormat("w")
    val udateformat = new SimpleDateFormat("yyyy-MM-dd")
    df.select($"*", lit(yformat.format(date).toInt).alias("Year"), lit(mformat.format(date).toInt).alias("Month"), lit(dformat.format(date).toInt).alias("Day"))
  }

  def dropDupsByKey(df: DataFrame, naturalKey: Array[String], sortCol: Array[String], spark: org.apache.spark.sql.SparkSession): DataFrame = {

    import spark.implicits._
    val keys = naturalKey.map(col(_))
    val orderbyCols = sortCol.map(col(_).desc)
    val window = Window.partitionBy(keys: _*).orderBy(orderbyCols: _*)
    df.withColumn("RNK", row_number().over(window)).filter($"RNK" === 1).drop($"RNK")
  }
