package streaming

import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level,Logger}
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ReceiverTest {

  case class order(order_id: String, user_id: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))
    //ssc.checkpoint("hdfs:///streaming/checkpoint")
   Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    //需要消费kafka中的topic
    val topicMap = Map {
      "badou" -> 2
    }
    val zkQuorum = "192.168.137.3:2181"
    val Dstream = KafkaUtils.createStream(ssc, zkQuorum, "group1", topicMap).map(_._2)
    //Dstream.map((_,1L)).reduceByKey(_+_).print()
    //Dstream.print()
    //解析{"order_id": 2539329, "user_id": 1, "eval_set": "prior", "order_number": 1, "order_dow": 2, "hour": 8, "day": 0.0}
    //统计hour的分布
    //    Dstream.map { line =>
    //      val mess = JSON.parseObject(line, classOf[Orders])
    //      mess.getOrder_dow
    //    }.map((_, 1L))
    //      .reduceByKeyAndWindow((a:Long,b:Long)=>a+b,Minutes(5),Seconds(2))
    //      //.reduceByKey(_ + _)
    //      .print()

    val spark = SparkSession.builder().appName("rddtoDF").enableHiveSupport().getOrCreate()
    import spark.implicits._

    Dstream.map { line =>
      val mess = JSON.parseObject(line, classOf[Orders])
      order(mess.getOrder_id, mess.getUser_id)
    }.foreachRDD { rdd =>
      val df = rdd.toDF()
      try {
        df.write.mode(SaveMode.Append).insertInto("")
      } catch {case e:SparkException=>
        df.write.saveAsTable("")
      }
    }
    //.map(x=>(x.order_id,1L))
    //.reduceByKeyAndWindow((a:Long,b:Long)=>a+b,Minutes(5),Seconds(2))
    //.reduceByKey(_ + _)
    //.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
