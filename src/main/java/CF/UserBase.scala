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
    val userScoreSumRdd = df.rdd.map(x => (x(0).toString(), x(2).toString))
      .groupByKey()
      .mapValues(x => sqrt(x.toArray.map(rating => pow(rating.toDouble, 2)).sum))
      .toDF("user_id","sqrt_rating")

    // 1.1 倒排表 item->users

    val df_v = df.selectExpr("user_id as user_v","item_id","rating_v" )
  }
}
