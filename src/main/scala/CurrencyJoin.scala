import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, corr, round}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}


object CurrencyJoin {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("CurrencyJoin")
      .master("local[*]")
      .getOrCreate()

    val currencyLoader = new Loader()

    val audusdDF: DataFrame = currencyLoader.readCurrencyData("AUDUSDM1.csv",spark)
    val nzdusdDF: DataFrame = currencyLoader.readCurrencyData("NZDUSDM1.csv",spark)
    val audnzdDF: DataFrame = currencyLoader.readCurrencyData("AUDNZDM1.csv",spark)

    val dfTotal = audnzdDF.join(audusdDF,Seq("datetime"),joinType = "left")
                          .join(nzdusdDF,Seq("datetime"),joinType = "left")


    println(s"auLen:${audusdDF.count()}")
    println(s"nuLen:${nzdusdDF.count()}")
    println(s"anLen:${audnzdDF.count()}")
    println(s"totalLen:${dfTotal.count()}")

    dfTotal.withColumn("calculatedAUDNZD",col("AUDUSDM1close")/col("NZDUSDM1close"))
           .withColumn("calculatedAUDNZD",round(col("calculatedAUDNZD"),5))
           .withColumn("realCalculatedDiff",col("calculatedAUDNZD")-col("AUDNZDM1close"))
           .withColumn("realCalculatedDiff",round(col("realCalculatedDiff"),5))
           .withColumn("difMA",avg(col("realCalculatedDiff")).over(Window.rowsBetween(-10,0)))
           .withColumn("difMA",round(col("difMA"),5))
           .withColumn("corrAUNU",corr(col("AUDUSDM1close"),col("NZDUSDM1close")).over(Window.rowsBetween(-20,0)))
           .withColumn("corrAUNU",round(col("corrAUNU"),2))
           .show(150,truncate = false)

    //dfTotal.show()

    //dfTotal.printSchema()
  }


}
