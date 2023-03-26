import org.apache.spark.sql.functions.{col, date_add, lit, to_date, _}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType, TimestampType}


class Loader {

  /**
   *
   *
   * @param dailyDate - daily date to select 24 hour period yyyy.MM.dd HH:mm"
   * @param path      - path to csv file
   * @param symbol    - symbol of currency
   * @param spark     - spark session
   * @param series    - column from file open, high,low,close
   * @return          - formated Dataframe
   */

   private def loadCurrencyData(dailyDate:String,path:String,symbol:String,series:String,spark:SparkSession): Dataset[Row] ={

     val customSchema: StructType = StructType(Array(
       StructField("datetime", TimestampType, nullable = true),
       StructField("open", DoubleType, nullable = true),
       StructField("high", DoubleType, nullable = true),
       StructField("low", DoubleType, nullable = true),
       StructField("close", DoubleType, nullable = true),
       StructField("vol1", DoubleType, nullable = true),
       StructField("vol2", DoubleType, nullable = true)))

     //val date: Column = lit(dailyDate)

     val colList: List[String] = List("datetime", series)
     val DF : Dataset[Row] =  spark.read
                                   .option("header", value = true)
                                   .option("delimiter", ",")
                                   .option("timestampFormat", "yyyy.MM.dd HH:mm")
                                   .schema(customSchema)
                                   .csv(path)
                                   .drop("vol1")
                                   .drop("vol2")
                                   .filter(col("datetime") > lit(dailyDate) && col("datetime") < date_add(lit(dailyDate),1))    // select only one day
                                   .select(colList.map(m => col(m)): _*)

     val renamedColumns: Array[String] = DF.columns.map(c => symbol + DF(c))
     renamedColumns(0) = "datetime" // set index column to datetime
     DF.toDF(renamedColumns: _*)

  }

  /**
   *
   * @param dailyDate - daily date to select 24 hour period yyyy.MM.dd HH:mm"
   * @param firstCurrencyDataSetPath - path to filename
   * @param firstCurrencySymbol   - currency symbol
   * @param secoundCurrencyDataSetPath - path to csv file
   * @param secoundCurrencySymbol - currensy symbol
   * @param crossCurrencyDataPath - syntetic caculated cross currency exchange rate
   * @param crossCurrencySymbol   - cross currency symbol
   * @param series                - series open,high,low or close
   * @param sparkSession          - spark session
   * @return
   */
   def readCurrencyData(dailyDate:String,series:String,
                        firstCurrencyDataSetPath:String,firstCurrencySymbol:String,
                        secoundCurrencyDataSetPath:String,secoundCurrencySymbol:String,
                        crossCurrencyDataPath:String,crossCurrencySymbol:String,
                        sparkSession: SparkSession): DataFrame = {
     val firstCurrencyDF  : Dataset[Row] = loadCurrencyData(dailyDate, firstCurrencyDataSetPath,firstCurrencySymbol,   series, sparkSession)
     val secondCurrencyDF : Dataset[Row] = loadCurrencyData(dailyDate,secoundCurrencyDataSetPath,secoundCurrencySymbol,series, sparkSession)
     val crossCurrencyDF  : Dataset[Row] = loadCurrencyData(dailyDate,crossCurrencyDataPath,crossCurrencySymbol,       series, sparkSession)

        //add filename prefix to col names
     crossCurrencyDF.join(firstCurrencyDF,  Seq("datetime"), joinType = "left")
                    .join(secondCurrencyDF, Seq("datetime"), joinType = "left")
    }


}
