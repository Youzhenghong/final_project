package features

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.countDistinct

/**
  * Created by youzhenghong on 21/03/2017.
  */
object merchantFeatures {
  def merchantActionCount(user_merchant: DataFrame): DataFrame = {
    user_merchant.groupBy("merchant_id").sum("click_action", "favor_action", "purchase_action")
      .withColumnRenamed("sum(click_action)", "num_of_click")
      .withColumnRenamed("sum(favor_action)","num_of_favor")
      .withColumnRenamed("sum(purchase_action)","num_of_purchase")
  }
  // userMerchantActionCount
  def merchantUserCount(user_merchant_action: DataFrame): DataFrame = {
    val merchantUser = user_merchant_action.groupBy("merchant_id", "action_type").agg(countDistinct("user_id"))
    val num_of_click_user = merchantUser.filter("action_type = 0")
      .select("merchant_id", "count(user_id)").withColumnRenamed("count(user_id)", "num_of_click_user")
    val num_of_purchase_user = merchantUser.filter("action_type = 2")
      .select("merchant_id", "count(user_id)").withColumnRenamed("count(user_id)", "num_of_purchase_user")
    val num_of_favor_user = merchantUser.filter("action_type = 3")
      .select("merchant_id", "count(user_id)").withColumnRenamed("count(user_id)", "num_of_favor_user")


    val cnt_click_user = num_of_click_user.count()
    val cnt_purchase_user = num_of_purchase_user.count()
    val cnt_favor_user = num_of_favor_user.count()

    var click_favor : DataFrame = num_of_click_user
    var click_favor_count : Long = 0

    if(cnt_click_user > cnt_favor_user) {
      click_favor = num_of_click_user.join(num_of_favor_user, Seq("merchant_id"), "LEFT")
      click_favor = click_favor.na.fill(0, Seq("num_of_click_user", "num_of_favor_user"))
      click_favor_count = cnt_click_user
    }
    else {
      click_favor = num_of_favor_user.join(num_of_click_user, Seq("merchant_id"), "LEFT")
      click_favor = click_favor.na.fill(0, Seq("num_of_click_user", "num_of_favor_user"))
      click_favor_count = cnt_favor_user
    }

    var click_favor_purchase = click_favor
    var click_favor_purchase_count = click_favor_count

    if(cnt_purchase_user > click_favor_purchase_count) {
      click_favor_purchase = num_of_purchase_user.join(click_favor_purchase, Seq("merchant_id"), "LEFT")
      click_favor_purchase = click_favor_purchase.na.fill(0, Seq("num_of_click_user", "num_of_favor_user", "num_of_purchase_user"))
    }
    else {
      click_favor_purchase = click_favor_purchase.join(num_of_purchase_user, Seq("merchant_id"), "LEFT")
      click_favor_purchase = click_favor_purchase.na.fill(0, Seq("num_of_click_user", "num_of_favor_user", "num_of_purchase_user"))
    }


    click_favor_purchase
  }
}
