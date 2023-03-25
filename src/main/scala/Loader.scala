import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType, TimestampType}

class Loader {
   def readCurrencyData(fileName: String, sparkSession: SparkSession): DataFrame = {
    //custom scehma for datetime parse
    val customSchema:StructType = StructType(Array(
      StructField("datetime", TimestampType, nullable = true),
      StructField("open", DoubleType, nullable = true),
      StructField("high", DoubleType, nullable = true),
      StructField("low", DoubleType, nullable = true),
      StructField("close", DoubleType, nullable = true),
      StructField("vol1", DoubleType, nullable = true),
      StructField("vol2", DoubleType, nullable = true)))

     val colList: List[String] = List("datetime", "close")
     val df: DataFrame = sparkSession.read
      .option("header", value = true)
      .option("delimiter", ",")
      .option("timestampFormat", "yyyy.MM.dd HH:mm")
      .schema(customSchema)
      .csv(fileName)
      .drop("vol1")
      .drop("vol2")
      .filter(col("datetime") > lit("2023-01-25 00:00:00").cast("timestamp"))
      .select(colList.map(m => col(m)): _*)


    //add filename prefix to col names
    val renamedColumns:Array[String] = df.columns.map(c => fileName.split("[.]")(0) + df(c))
    renamedColumns(0) = "datetime" // set index column to datetime
    df.toDF(renamedColumns: _*)



  }


}
