import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, DateType, DoubleType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions.countDistinct

object App {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("audnzd_join")
      .master("local")
      .getOrCreate()

    var audusdDF: DataFrame = readCurrencyData("AUDUSDM1.csv",spark)
    var nzdusdDF: DataFrame = readCurrencyData("NZDUSDM1.csv",spark)
    var audnzdDF: DataFrame = readCurrencyData("AUDNZDM1.csv",spark)


    audusdDF = audusdDF.filter(audusdDF.col("datetime") > lit("2023-01-25 00:00:00").cast("timestamp"))
    nzdusdDF = nzdusdDF.filter(nzdusdDF.col("datetime") > lit("2023-01-25 00:00:00").cast("timestamp"))
    audnzdDF = audnzdDF.filter(audnzdDF.col("datetime") > lit("2023-01-25 00:00:00").cast("timestamp"))

    val auLen: Long = audusdDF.count()
    val nuLen: Long = nzdusdDF.count()
    val anLen: Long = audnzdDF.count()

    val dfTotal = audnzdDF
                  .join(audusdDF,Seq("datetime"),joinType = "left")
                  .join(nzdusdDF,Seq("datetime"),joinType = "left")

    val totalLen: Long = dfTotal.count()

    println(s"auLen:$auLen")
    println(s"nuLen:$nuLen")
    println(s"anLen:$anLen")
    println(s"totalLen:$totalLen")

    //audnzdDF.printSchema()
    dfTotal.show()
  }

  private def readCurrencyData(fileName: String,sparkSession: SparkSession): DataFrame = {
    //custom scema for datetime parse
    val customSchema = StructType(Array(
      StructField("datetime", TimestampType , true),
      StructField("open", DoubleType, true),
      StructField("high",  DoubleType, true),
      StructField("low",  DoubleType, true),
      StructField("close", DoubleType, true),
      StructField("vol1", DoubleType, true),
      StructField("vol2", DoubleType, true)
    )
    )

    var df:DataFrame = sparkSession.read
      .option("header", true)
      .option("delimiter", ",")
      .option("timestampFormat", "yyyy.MM.dd HH:mm")
      .schema(customSchema)
      .csv(fileName)
      .drop("vol1")
      .drop("vol2")

    //add filename prefix to col names
    val renamedColumns = df.columns.map(c => fileName.split("[.]")(0)+df(c))
    renamedColumns(0) = "datetime" // set index column to datetime
    df = df.toDF(renamedColumns:_*)

    return df
  }
}
