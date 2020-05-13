package offline

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, collect_set, count, size, sum}
import org.apache.spark.storage.StorageLevel

object Feature {
  def GenerateFeature(priors: DataFrame, orders: DataFrame): (DataFrame,DataFrame) = {
    /**
     * produce feature
     * 1.销售量 prod_cnt
     * 2.商品被再次购买(reordered)量prod_sum_rod
     * 3.统计reordered比率prod_rod_rate  avg=sum/count [1,0,1,0,1] 3/5
     **/

    val prodRodCnt = priors.selectExpr("product_id", "cast(reordered as int)")
      .groupBy("product_id")
      .agg(sum("reordered").as("prod_sum_rod"),
        count("product_id").as("prod_cnt"),
        avg("reordered").as("prod_rod_rate"))

    /**
     * user Feature
     * 1.每个用户购买订单的平均day间隔 days_since orders
     * 2.每个用户的总订单数 count order_number
     * 3.每个用户购买的product商品去重后的集合数据
     * 4.用户总商品数量以及去重后的商品数量
     * 5.每个用户平均每个订单多少商品
     **/
    //对异常值处理，将days_since_prior_order中的空值进行处理
    val ordersNew = orders.selectExpr("*",
      "if(days_since_prior_order='',0.0,days_since_prior_order) as dspo")
      .drop("days_since_prior_order")
    //NULL 自动填0:orders.na.fill(0.0)
    //1.每个用户购买订单的平均day间隔 days_since orders
    //2.每个用户的总订单数 count order_number
    val userGap = ordersNew.selectExpr("user_id", "cast(dspo as int) as dspo")
      .groupBy("user_id")
      .agg(avg("dspo").as("u_avg_day_gap"),
        count("user_id").as("u_ord_cnt"))

    val op = orders.join(priors, "order_id").persist(StorageLevel.MEMORY_AND_DISK)
    val up = op.select("user_id", "product_id")

    import orders.sparkSession.implicits._
    //用function中的方法统计
    val userProRcdSize = up.groupBy("user_id")
      .agg(collect_set("product_id").as("u_prod_records"),
        //相同效果，统计prod_uni_size:countDistinct和size
        // countDistinct(collect_set("product_id")).as("prod_uni_size"),
        size(collect_set("product_id")).as("u_prod_uni_size"),
        count("product_id").as("u_prod_size")
      )

    //5.每个用户平均每个订单多少商品
    //1)先求每个订单多少商品
    val ordProdCnt = priors.groupBy("order_id").count()//.withColumnRenamed("count","u_ord_cnt")
    //2）求每个用户订单商品数量的平均值
    val userPerOrdProdCnt = orders.join(ordProdCnt, "order_id")
      .groupBy("user_id")
      .agg(avg("count").as("u_avg_ord_prods"))

    //用户特征合并
    val userFeat = userPerOrdProdCnt.join(userProRcdSize,"user_id").join(userGap,"user_id")

    /** user and product Feature :cross feature 交叉特征
     *1.统计user和对应product在多少个订单中出现（distinct order_id）
     *2.特定product具体在购物车(cart)中出现位置的平均位置
     *3.最后一个订单id
     *4.用户对应product在所有这个用户购买产品量中的占比rate
     * */

    //以user和product两个id为组合key
    val userXprod = op.selectExpr("concat_ws('_',user_id,product_id) as user_prod",
      "order_number", "order_id", "order_hour_of_day",
      "cast(add_to_cart_order as int) as add_to_cart_order")

    //1.统计user和对应product在多少个订单中出现（distinct order_id）
    //2.特定product具体在购物车(cart)中出现位置的平均位置
    val userXprodNbOrd = userXprod.groupBy("user_prod")
      .agg(count("user_prod").as("orders_cnt"),
        avg("add_to_cart_order").as("avg_pos_in_cart"))
    //3.共同的最后一个订单的id,order_number以及对应的hour
    val lastOrder = userXprod.rdd
      .map(x => (x(0).toString, (x(1).toString, x(2).toString, x(3).toString)))
      .groupByKey().mapValues(_.toArray.maxBy(_._1.toInt)).toDF("user_prod", "order_num_id")
      .selectExpr("user_prod", "cast(order_num_id._1 as int) as max_ord_num",
        "order_num_id._2 as last_order_id", "order_num_id._3 as last_order_hour")


    val xFeat = userXprodNbOrd.join(lastOrder, "user_prod")
      .selectExpr("*", "split(user_prod,'_')[0] as user_id",
        "split(user_prod,'_')[1] as product_id").drop("user_prod")
    //4.用户对应product在所有这个用户购买产品量中的占比rate
    val rate = xFeat.selectExpr("user_id", "product_id",
      "concat(round(100*orders_cnt/max_ord_num),'%') as user_prod_rate")
    (userFeat,prodRodCnt)
  }
}
