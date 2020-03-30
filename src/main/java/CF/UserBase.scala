package CF

import org.apache.spark.sql.SparkSession

object UserBase {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val warehouse = "hdfs://master:9000/usr/soft/apache-hive-1.2.2-bin/warehouse"

    val spark = SparkSession.builder()
      .config("spark.sql.warehouse.dir",warehouse)
      .master("local[2]")
      .appName("User Base")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.sql("select user_id,item_id,rating from badou.udata")
    df.show()

    /**
     * +-------+-------+------+
     * |user_id|item_id|rating|
     * +-------+-------+------+
     * |    196|    242|     3|
     * |    186|    302|     3|
     * |     22|    377|     1|
     * |    244|     51|     2|
     * |    166|    346|     1|
     * |    298|    474|     4|
     * +-------+-------+------+
     */
    //1.计算相似用户 cosine = a*b/(|a|*|b|) a和b的点乘

  }
}
