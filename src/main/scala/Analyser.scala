import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, corr, round}

class Analyser() {

  /**
   * 
   * @param currencyData - dataframe output of Loader
   * @return dataframe with additional collumns 
   */
  def analyzeCrossCurrencyData(currencyData:DataFrame,symbolName1:String,symbolName2:String,symbolName3:String,series:String):DataFrame ={

    val corrColumnName:String = s"corr${symbolName1}_${symbolName2}"

    currencyData.withColumn("synteticCross", col(symbolName1+series) / col(symbolName2+series))
                .withColumn("synteticCross", round(col("synteticCross"), 5))
                .withColumn("realCalculatedDiff", col("synteticCross") - col(symbolName3+series))
                .withColumn("realCalculatedDiff", round(col("realCalculatedDiff"), 5))
                .withColumn("difMA", avg(col("realCalculatedDiff")).over(Window.rowsBetween(-10, 0)))
                .withColumn("difMA", round(col("difMA"), 5))
                .withColumn(corrColumnName+"_"+series, corr(col(symbolName1+series), col(symbolName2+series)).over(Window.rowsBetween(-20, 0)))
                .withColumn(corrColumnName+"_"+series, round(col(corrColumnName+"_"+series), 2))
  }

}
