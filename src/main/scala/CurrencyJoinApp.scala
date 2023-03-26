import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, corr, round}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.xbean.asm9.tree.analysis.Analyzer


object CurrencyJoinApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("CurrencyJoin")
      .master("local[*]")
      .getOrCreate()

    //params
    val symbol1: String = "AUDUSDM1"
    val symbol2: String = "NZDUSDM1"
    val symbol3: String = "AUDNZDM1"
    val dailyDate: String = "2023-01-25 00:00:00"
    val series:String = "close"

    val currencyLoader: Loader = new Loader
    val analyser: Analyser     = new Analyser

    val joinedDF: DataFrame = currencyLoader.readCurrencyData(dailyDate,series,"AUDUSDM1.csv",symbol1,
                                                                                "NZDUSDM1.csv",symbol2,
                                                                                "AUDNZDM1.csv",symbol3,spark)

    val calculatedData:DataFrame = analyser.analyzeCrossCurrencyData(joinedDF,symbol1,symbol2,symbol3,series)

    calculatedData.show(100)

    calculatedData.write.mode("overwrite").parquet("result.pq")

    //joinedDF.show()
    //dfTotal.show()
    //dfTotal.printSchema()
  }


}
