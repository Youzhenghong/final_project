package feature.extractors

/**
  * Created by youzhenghong on 21/03/2017.
  */
import breeze.linalg.sum
import feature.extractors.userMerchantFeatures
import org.apache.spark.sql.DataFrame
import utils.fileLoader
import org.apache.spark.sql.functions._

object userFeatures {
    def userItemCount(user_log : DataFrame) : DataFrame = {
      val groupped = user_log.groupBy("user_id","action_type")
      val userItem = groupped.agg(countDistinct("item_id"))
      val item_click = userItem.filter("action_type=0")
        .select("user_id","count(item_id)").withColumnRenamed("count(item_id)", "item_click")
      val item_purchase = userItem.filter("action_type=2")
        .select("user_id","count(item_id)").withColumnRenamed("count(item_id)", "item_purchase")
      val item_favor = userItem.filter("action_type=3")
        .select("user_id","count(item_id)").withColumnRenamed("count(item_id)", "item_favor")

      item_click.show()
      item_purchase.show()
      item_favor.show()

      val num_item_click = item_click.count()
      val num_item_purchase = item_purchase.count()
      val num_item_favor = item_favor.count()
      var click_favor : DataFrame = item_click
      var click_favor_count : Long = 0
      if(num_item_click > num_item_favor) {
        click_favor = item_click.join(item_favor, Seq("user_id"), "LEFT")
        click_favor = click_favor.na.fill(0, Seq("item_favor", "item_click"))
        click_favor_count = num_item_click

      }
      else {
        click_favor = item_favor.join(item_click,Seq("user_id"), "LEFT")
        click_favor = click_favor.na.fill(0, Seq("item_favor", "item_click"))
        click_favor_count = num_item_favor
      }

      var click_favor_purchase = click_favor
      var click_favor_purchase_count = click_favor_count
      if(click_favor_count > num_item_purchase) {
        click_favor_purchase = click_favor.join(item_purchase, Seq("user_id"), "LEFT")
        click_favor_purchase = click_favor_purchase.na.fill(0, Seq("item_click", "item_favor", "item_purchase"))
        click_favor_purchase_count = click_favor_count
      }
      else {
        click_favor_purchase = item_purchase.join(click_favor, Seq("user_id"), "LEFT")
        click_favor_purchase = click_favor_purchase.na.fill(0, Seq("item_click", "item_favor", "item_purchase"))
        click_favor_purchase_count = num_item_purchase
      }

      click_favor_purchase

    }


    def userAction(user_merchant : DataFrame) : DataFrame = {
      user_merchant.groupBy("user_id").sum("click_action", "favor_action", "purchase_action")
        .withColumnRenamed("sum(click_action)", "num_of_click")
        .withColumnRenamed("sum(favor_action)","num_of_favor")
        .withColumnRenamed("sum(purchase_action)","num_of_purchase")
    }
}
