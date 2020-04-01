package CF

import breeze.numerics.{pow, sqrt}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

object UserBase {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val warehouse = "hdfs://master:9000/usr/soft/apache-hive-1.2.2-bin/warehouse"

    val spark = SparkSession.builder()
      .config("spark.sql.warehouse.dir", warehouse)
      .master("local[2]")
      .appName("User Base")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.sql("select user_id,item_id,rating from badou.udata")
    /**
     * +-------+-------+------+
     * |user_id|item_id|rating|
     * +-------+-------+------+
     * |    196|    242|     3|
     * |    186|    302|     3|
     * |     22|    377|     1|
     * |    244|     51|     2|
     * |    166|    346|     1|
     * +-------+-------+------+
     */
    //1.计算相似用户  相似度度量 cosine = a*b/(|a|*|b|) a和b的点乘
    //选取的用户和相似用户都是在user_id中

    //每一个用户的分母
    //第一种方法，使用sql
    // 选择一个用户
    val df1 = df.filter("user_id='196'")
    /**
     * +-------+-------+------+
     * |user_id|item_id|rating|
     * +-------+-------+------+
     * |    196|    242|     3|
     * |    196|    393|     4|
     * |    196|      8|     5|
     * |    196|    428|     4|
     * |    196|   1118|     4|
     * +-------+-------+------+
     */
    // 求平方
    val df2 = df.selectExpr("*", "pow(cast(rating as double),2) as pow_rating")
    /**
     * +-------+-------+------+----------+
     * |user_id|item_id|rating|pow_rating|
     * +-------+-------+------+----------+
     * |    196|    242|     3|       9.0|
     * |    186|    302|     3|       9.0|
     * |     22|    377|     1|       1.0|
     * |    244|     51|     2|       4.0|
     * |    194|    274|     2|       4.0|
     * +-------+-------+------+----------+
     */
    // 求平方和
    val df3 = df2.groupBy("user_id").agg(sum("pow_rating").as("sum_pow_rating"))
    /**
     * +-------+--------------+
     * |user_id|sum_pow_rating|
     * +-------+--------------+
     * |    467|         638.0|
     * |    675|         528.0|
     * |    296|        2770.0|
     * |    718|         574.0|
     * |    574|         634.0|
     * +-------+--------------+
     */
    // 求平方根
    val userScoreSum = df3.selectExpr("*", "sqrt(sum_pow_rating) as sqrt_rating")

    /**
     * +-------+--------------+------------------+
     * |user_id|sum_pow_rating|       sqrt_rating|
     * +-------+--------------+------------------+
     * |    467|         638.0| 25.25866188063018|
     * |    675|         528.0|22.978250586152114|
     * |    296|        2770.0|52.630789467763066|
     * |    691|         611.0| 24.71841418861655|
     * |     51|         324.0|              18.0|
     * +-------+--------------+------------------+
     */
    //另外一个方法 rdd,这种方式偏底层，结果跟userScoreSum一样
    import spark.implicits._
    val userScoreSumRdd = df.rdd.map(x => (x(0).toString, x(2).toString))
      .groupByKey()
      .mapValues(x => sqrt(x.toArray.map(rating => pow(rating.toDouble, 2)).sum))
      .toDF("user_id", "sqrt_rating")

    // 1.1 倒排表 item->users

    val df_v = df.selectExpr("user_id as user_v", "item_id", "rating as rating_v")
    /**
     * +------+-------+--------+
     * |user_v|item_id|rating_v|
     * +------+-------+--------+
     * |   196|    242|       3|
     * |   186|    302|       3|
     * |    22|    377|       1|
     * |   244|     51|       2|
     * |   166|    346|       1|
     * +------+-------+--------+
     */
    val df_decare = df.join(df_v, "item_id").filter("cast(user_id as long)<>cast(user_v as long)")

    /**
     * +-------+-------+------+------+--------+
     * |item_id|user_id|rating|user_v|rating_v|
     * +-------+-------+------+------+--------+
     * |    242|    196|     3|   721|       3|
     * |    242|    196|     3|   720|       4|
     * |    242|    196|     3|   500|       3|
     * |    242|    196|     3|   845|       4|
     * |    242|    196|     3|   305|       5|
     * +-------+-------+------+------+--------+
     */
    //计算两个用户在同一个item下的评分的乘积，就是cosine公式的分子中的一部分
    val df_product = df_decare.selectExpr("user_id", "user_v", "cast(rating as double)*cast(rating_v as double) as prod")

    /**
     * +-------+------+----+
     * |user_id|user_v|prod|
     * +-------+------+----+
     * |    196|   721| 9.0|
     * |    196|   720|12.0|
     * |    196|   500| 9.0|
     * |    196|   845|12.0|
     * |    196|   305|15.0|
     * +-------+------+----+
     */

    //相同的两个用户会有在不同item上的打分,其实就是两个用户在item上的交集
    df_product.filter("user_id='196' and user_v='721'").show()

    /**
     * +-------+------+----+
     * |user_id|user_v|prod|
     * +-------+------+----+
     * |    196|   721| 9.0|
     * |    196|   721|20.0|
     * |    196|   721|10.0|
     * |    196|   721|12.0|
     * |    196|   721|16.0|
     * +-------+------+----+
     */
    //求和，计算完整的分子部分
    val df_sim_group = df_product.groupBy("user_id", "user_v").agg(sum("prod").as("rating_dot"))


    /**
     * +-------+------+----------+
     * |user_id|user_v|rating_dot|
     * +-------+------+----------+
     * |    196|   617|      24.0|
     * |    196|   123|      71.0|
     * |    166|   206|     104.0|
     * |    298|   465|     523.0|
     * |    305|   807|     843.0|
     * +-------+------+----------+
     */

    val userScoreSum_v = userScoreSum.selectExpr("user_id as user_v", "sqrt_rating as sqrt_rating_v")

    val df_sim = df_sim_group.join(userScoreSum,"user_id").join(userScoreSum_v,"user_v")

    /**
     * +------+-------+----------+--------------+------------------+------------------+
     * |user_v|user_id|rating_dot|sum_pow_rating|       sqrt_rating|     sqrt_rating_v|
     * +------+-------+----------+--------------+------------------+------------------+
     * |   617|    196|      24.0|         549.0|23.430749027719962| 30.54504869860253|
     * |   123|    196|      71.0|         549.0|23.430749027719962|29.427877939124322|
     * |   206|    166|     104.0|         291.0| 17.05872210923198|20.904544960366874|
     * |   465|    298|     523.0|        2148.0| 46.34652090502587|31.575306807693888|
     * |   807|    305|     843.0|        2839.0| 53.28226721902888| 56.83308895353129|
     * +------+-------+----------+--------------+------------------+------------------+
     */
    val df_cosine = df_sim.selectExpr("user_id","user_v","rating_dot/(sqrt_rating*sqrt_rating_v) as cosine_sim")
    df_cosine.show()

    /**
     * +-------+------+--------------------+
     * |user_id|user_v|          cosine_sim|
     * +-------+------+--------------------+
     * |    196|   617|0.033533914107330615|
     * |    196|   123| 0.10297059695164824|
     * |    166|   206| 0.29163935315828643|
     * |    298|   465| 0.35738553543065843|
     * |    305|   807| 0.27838358105878475|
     * +-------+------+--------------------+
     */
  }
}
