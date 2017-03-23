package feature.extractors

import org.apache.spark.sql.DataFrame

/**
  * Created by youzhenghong on 21/03/2017.
  */
object userMerchantFeatures {
  def userMerchant(df : DataFrame) : Unit = {
    val userMerchantAction = userMerchantActionCount(df)
    userMerchantAction.cache()

    val click_action = {
      userMerchantAction.filter("action_type = 0")
        .select("user_id", "merchant_id", "count")
        .withColumnRenamed("count", "click_action")
    }

    val purchase_acction = {
      userMerchantAction.filter("action_type = 2")
        .select("user_id", "merchant_id", "count")
        .withColumnRenamed("count", "purchase_action")
    }

    val favor_action = {
      userMerchantAction.filter("action_type = 3")
        .select("user_id", "merchant_id", "count")
        .withColumnRenamed("count", "favor_action")
    }

    //click_action.show()
    //purchase_acction.show()
    //favor_action.show()

  }


  private def userMerchantActionCount(df : DataFrame) : DataFrame = {
    df.groupBy("user_id", "merchant_id", "action_type").count()
  }
}
