/**
  * Created by youzhenghong on 21/03/2017.
  */
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import utils._
import features.userFeatures
import features.userMerchantFeatures
import features.merchantFeatures
object appMain {
  def main(args: Array[String]): Unit = {
    /*
      initialize spark, create spark context
     */
    val sc = createSparkContext()
    //println(sc)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    var df = fileLoader.loadUserLog(sc)
    df.show()

    //userFeatures.itemActionCount(df)
    //userFeatures.getUserFeatures(df)
    var user_merchant_action = userMerchantFeatures.userMerchantActionCount(df)
    var merchant_user_count = merchantFeatures.merchantUserCount(user_merchant_action)
    merchant_user_count.show()


  }

  /**
    * Initialize sparkContext, return sc
    *
   */
  def createSparkContext(): SparkContext = {
    val config = new SparkConf()
      .setAppName("final project")
      .setMaster("local")
    val sc = SparkContext.getOrCreate(config)
    sc
  }


}

