package utils

/**
  * Created by youzhenghong on 21/03/2017.
  * this class if used for load input csv files
  */
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import utils._

object fileLoader {
  /**
    * load user_log.csv
   */

  private def loadDF(sc : SparkContext, PATH : String) : DataFrame = {
    val sqlContext = SQLContextSingleton.getInstance(sc)
    val DF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(PATH).toDF()
    DF
  }

  def loadUserInfo(sc : SparkContext) : DataFrame = {
    val PATH = propertyLoader.USER_INFO_PATH
    loadDF(sc, PATH)
  }

  def loadUserLog(sc : SparkContext) : DataFrame = {
    val PATH = propertyLoader.USER_LOG_PATH
    loadDF(sc, PATH)
  }

  def loadUserMerchant(sc :SparkContext) :DataFrame = {
    val PATH = propertyLoader. USER_MERCHANT_CSV
    loadDF(sc, PATH)
  }
}
