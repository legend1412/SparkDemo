package streaming

import com.alibaba.fastjson.{JSON, JSONException}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ReceiverTest2 {
  case class order(order_id:String,user_id:String)
  def main(args: Array[String]): Unit = {
   System.setProperty("HADOOP_USER_NAME","root")
    val warehouse = "hdfs://192.168.137.3:9000/usr/soft/apache-hive-1.2.2-bin/warehouse"
    val spark = SparkSession.builder()
      .config("spark.sql.warehouse.dir",warehouse).master("local[2]")
      .appName("rdd2DF").enableHiveSupport().getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext,Seconds(5))
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    //需要消费kafka中的topic
    val topicMap = Map{"badou"->2}
    val zkQuorum = "192.168.137.3:2181"
    val Dstream = KafkaUtils.createStream(ssc,zkQuorum,"group1",topicMap).map(_._2)

    import spark.implicits._
    Dstream.map{line=>
      try{
        val mess = JSON.parseObject(line,classOf[Orders])
        order(mess.getOrder_id,mess.getUser_id)
      }catch{
        case e:JSONException=>
          println(e,line)
          //需要存hive表中，为了方便进一步了解解析问题的具体是哪些数据，以及数据量，会不会影响整体情况，看需不需要单独对某一些日志重写解析
          order("wrong",line)
      }

    }.foreachRDD{rdd=>
      val df=rdd.toDF("order_id","user_id")
     try{
       df.write.mode(SaveMode.Append).insertInto("badou.order_test")
       println("insertInto")
     }catch {case e:AnalysisException=>
       println(e)
       df.write.saveAsTable("badou.order_test")
       println("create table,insertInto")
     }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
