package features

/**
  * Created by youzhenghong on 21/03/2017.
  */
import breeze.linalg.sum
import features.userMerchantFeatures
import org.apache.spark.sql.DataFrame
import utils.fileLoader
import org.apache.spark.sql.functions._

object userFeatures {
    def getUserFeatures(user_log : DataFrame, user_merchant: DataFrame) : DataFrame = {
      user_log.cache()
      val itemAction = itemActionCount(user_log)
      val merchantAction = merchantActionCount(user_log)
      val categoryAction = categoryActionCount(user_log)
      val userAction = userActionCount(user_merchant)

      //println("\n\n\n\n\n\n itemaction" + itemAction.count())
      //itemAction.show()
      //println("\n\n\n\n\n\n")
      //println("\n\n\n\n\n\n merchantaction  " + merchantAction.count())
      //merchantAction.show()
      //println("\n\n\n\n\n\n categoryaction"+ categoryAction.count())
      //println("\n\n\n\n\n\n categoryaction")
      //categoryAction.show()
      //println("\n\n\n\n\n\n")
      //println("\n\n\n\n\n\n userAction    " + userAction.count())
      val userFeatures = itemAction.join(merchantAction, Seq("user_id"), "LEFT")
        .join(categoryAction, Seq("user_id"), "LEFT").join(itemAction, Seq("user_id"), "LEFT")
      userFeatures
    }



    private def itemActionCount(user_log : DataFrame) : DataFrame = {
      val userItem = user_log.groupBy("user_id","action_type").agg(countDistinct("item_id"))
      val item_click = userItem.filter("action_type=0")
        .select("user_id","count(item_id)").withColumnRenamed("count(item_id)", "item_click")
      val item_purchase = userItem.filter("action_type=2")
        .select("user_id","count(item_id)").withColumnRenamed("count(item_id)", "item_purchase")
      val item_favor = userItem.filter("action_type=3")
        .select("user_id","count(item_id)").withColumnRenamed("count(item_id)", "item_favor")

      //item_click.show()
      //item_purchase.show()
      //item_favor.show()

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

  private def merchantActionCount(user_log : DataFrame) : DataFrame = {
    val userItem = user_log.groupBy("user_id","action_type").agg(countDistinct("merchant_id"))
    val merchant_click = userItem.filter("action_type=0")
      .select("user_id","count(merchant_id)").withColumnRenamed("count(merchant_id)", "merchant_click")
    val merchant_purchase = userItem.filter("action_type=2")
      .select("user_id","count(merchant_id)").withColumnRenamed("count(merchant_id)", "merchant_purchase")
    val merchant_favor = userItem.filter("action_type=3")
      .select("user_id","count(merchant_id)").withColumnRenamed("count(merchant_id)", "merchant_favor")

    //merchant_click.show()
    //merchant_purchase.show()
    //merchant_favor.show()

    val num_merchant_click = merchant_click.count()
    val num_merchant_purchase = merchant_purchase.count()
    val num_merchant_favor = merchant_favor.count()
    var click_favor : DataFrame = merchant_click
    var click_favor_count : Long = 0
    if(num_merchant_click > num_merchant_favor) {
      click_favor = merchant_click.join(merchant_favor, Seq("user_id"), "LEFT")
      click_favor = click_favor.na.fill(0, Seq("merchant_favor", "merchant_click"))
      click_favor_count = num_merchant_click

    }
    else {
      click_favor = merchant_favor.join(merchant_click,Seq("user_id"), "LEFT")
      click_favor = click_favor.na.fill(0, Seq("merchant_favor", "merchant_click"))
      click_favor_count = num_merchant_favor
    }

    var click_favor_purchase = click_favor
    var click_favor_purchase_count = click_favor_count
    if(click_favor_count > num_merchant_purchase) {
      click_favor_purchase = click_favor.join(merchant_purchase, Seq("user_id"), "LEFT")
      click_favor_purchase = click_favor_purchase.na.fill(0, Seq("merchant_click", "merchant_favor", "merchant_purchase"))
      click_favor_purchase_count = click_favor_count
    }
    else {
      click_favor_purchase = merchant_purchase.join(click_favor, Seq("user_id"), "LEFT")
      click_favor_purchase = click_favor_purchase.na.fill(0, Seq("merchant_click", "merchant_favor", "merchant_purchase"))
      click_favor_purchase_count = num_merchant_purchase
    }
    //click_favor_purchase.show()
    click_favor_purchase

  }

  private def categoryActionCount(user_log : DataFrame) : DataFrame = {
    val userItem = user_log.groupBy("user_id","action_type").agg(countDistinct("cat_id"))
    val category_click = userItem.filter("action_type=0")
      .select("user_id","count(cat_id)").withColumnRenamed("count(cat_id)", "category_click")
    val category_purchase = userItem.filter("action_type=2")
      .select("user_id","count(cat_id)").withColumnRenamed("count(cat_id)", "category_purchase")
    val category_favor = userItem.filter("action_type=3")
      .select("user_id","count(cat_id)").withColumnRenamed("count(cat_id)", "category_favor")

    //category_click.show()
    //category_purchase.show()
    //category_favor.show()

    val num_category_click = category_click.count()
    val num_category_purchase = category_purchase.count()
    val num_category_favor = category_favor.count()
    var click_favor : DataFrame = category_click
    var click_favor_count : Long = 0
    if(num_category_click > num_category_favor) {
      click_favor = category_click.join(category_favor, Seq("user_id"), "LEFT")
      click_favor = click_favor.na.fill(0, Seq("category_favor", "category_click"))
      click_favor_count = num_category_click

    }
    else {
      click_favor = category_favor.join(category_click,Seq("user_id"), "LEFT")
      click_favor = click_favor.na.fill(0, Seq("category_favor", "category_click"))
      click_favor_count = num_category_favor
    }

    var click_favor_purchase = click_favor
    var click_favor_purchase_count = click_favor_count
    if(click_favor_count > num_category_purchase) {
      click_favor_purchase = click_favor.join(category_purchase, Seq("user_id"), "LEFT")
      click_favor_purchase = click_favor_purchase.na.fill(0, Seq("category_click", "category_favor", "category_purchase"))
      click_favor_purchase_count = click_favor_count
    }
    else {
      click_favor_purchase = category_purchase.join(click_favor, Seq("user_id"), "LEFT")
      click_favor_purchase = click_favor_purchase.na.fill(0, Seq("category_click", "category_favor", "category_purchase"))
      click_favor_purchase_count = num_category_purchase
    }
    //click_favor_purchase.show()
    click_favor_purchase

  }


   def userActionCount(user_merchant : DataFrame) : DataFrame = {
      user_merchant.groupBy("user_id").sum("click_action", "favor_action", "purchase_action")
        .withColumnRenamed("sum(click_action)", "num_of_click")
        .withColumnRenamed("sum(favor_action)","num_of_favor")
        .withColumnRenamed("sum(purchase_action)","num_of_purchase")
    }
}
