package offline

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object Feat {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Feature Transform")
      .enableHiveSupport()
      .getOrCreate()
    // spark shell [client] 实例化sc,spark
    val orders = spark.sql("select * from badou.orders")
    val priors = spark.sql("select * from badou.priors")

    /**
     * produce feature
     * 1.销售量 prod_cnt
     * 2.商品被再次购买(reordered)量prod_sum_rod
     * 3.统计reordered比率prod_rod_rate  avg=sum/count [1,0,1,0,1] 3/5
     **/
    val prodCnt = priors.groupBy("product_id").count()
      .withColumnRenamed("count", "prod_cnt")
    //      .selectExpr("produce_id","count as prod_cnt")

    //用sqlgroupby
    // val prodCnt = spark.sql("select prodcue_id,count(1) as prod_cnt from badou.priors group by produce_id")

    val prodRodCnt = priors.selectExpr("product_id", "cast(reordered as int)")
      .groupBy("product_id")
      .agg(sum("reordered").as("prod_sum_rod"),
        count("product_id").as("prod_cnt"),
        avg("reordered").as("prod_rod_rate"))
    prodRodCnt.show()
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
    ordersNew.show()
    //NULL 自动填0:orders.na.fill(0.0)
    //1.每个用户购买订单的平均day间隔 days_since orders
    //2.每个用户的总订单数 count order_number
    val userGap = ordersNew.selectExpr("user_id", "cast(dspo as int) as dspo")
      .groupBy("user_id")
      .agg(avg("dspo").as("u_avg_day_gap"),
        count("user_id").as("u_ord_cnt"))
    //3.每个用户购买的product商品去重后的集合数据
    val op = orders.join(priors, "order_id").persist(StorageLevel.MEMORY_AND_DISK)
    val up = op.select("user_id", "product_id")
    import spark.implicits._
    //DataFrame.Row()
    val userUniOrdRecs = up.rdd.map(x => (x(0).toString, x(1).toString))
      .groupByKey().mapValues(_.toSet.mkString(",")) //concat_ws(',',collect_set())
      .toDF("user_id", "prod_cnt_uni_cnt")
    //查看每一列的所有内容：userUniOrdRecs.show(false)
    //val tmp = up.groupBy("user_id").agg(collect_set("product_id").as("prod_uni_cnt"))

    //3.每个用户购买的product商品去重后的集合数据
    //4.用户总商品数量以及去重后的商品数量
    val userProRcdSize = up.rdd.map(x => (x(0).toString, x(1).toString))
      .groupByKey().mapValues { records =>
      val rs = records.toSet;
      (rs.size, rs.mkString(","), records.size)
    }
      .toDF("user_id", "tuple")
      .selectExpr("user_id", "tuple._1 as prod_uni_size",
        "tuple._2 as prod_records", "tuple._3 as prod_size")

    //用function中的方法统计
    val userProRcdSize1 = up.groupBy("user_id")
      .agg(collect_set("product_id").as("prod_records"),
        //相同效果，统计prod_uni_size:countDistinct和size
        // countDistinct(collect_set("product_id")).as("prod_uni_size"),
        size(collect_set("product_id")).as("prod_uni_size"),
        count("product_id").as("prod_size")
      )
    userProRcdSize.write.mode("overwrite").saveAsTable("badou.user_pro_records")
    userProRcdSize.show()
    //5.每个用户平均每个订单多少商品
    //1)先求每个订单多少商品
    val ordProdCnt = priors.groupBy("order_id").count()
    //2）求每个用户订单商品数量的平均值
    val userPerOrdProdCnt = orders.join(ordProdCnt, "order_id")
      .groupBy("user_id")
      .agg(avg("count").as("u_avg_ord_prods"))
    userPerOrdProdCnt.show()
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
    userXprod.show()
    //1.统计user和对应product在多少个订单中出现（distinct order_id）
    //2.特定product具体在购物车(cart)中出现位置的平均位置
    //userXprod.where("user_prod like '163668%' ").orderBy(desc("order_id")).show()
    //userXprod.selectExpr("*","cast(order_id as int) as order_id_int").where("user_prod like '163668%' ").orderBy(desc("order_id_int")).show()
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
    val rate = xFeat.selectExpr("user_id","product_id",
      "concat(round(100*orders_cnt/max_ord_num),'%') as user_prod_rate")
  }
}
