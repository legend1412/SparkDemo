package streaming

import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._

object DirectKafkaTest {
  def main(args: Array[String]): Unit = {
    val Array(brokers, groupId, topics) = Array("192.168.137.3:9092", "group1", "badou")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
      .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val topicsSet = topics.split(",").toSet

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "group.id" -> groupId)
    val km = new KafkaManager(kafkaParams)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    //offset获取、打印
    var offsetRanges = Array[OffsetRange]()
    messages.foreachRDD {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (offsize <- offsetRanges) {
          km.commitOffsetsToZK(offsetRanges)
          println(s"${offsize.topic},${offsize.partition},${offsize.fromOffset},${offsize.untilOffset}")
        }
    }
    // Get the lines, split them into words, count the words and print
    val lines = messages.map(x => JSON.parseObject(x._2, classOf[Orders]).getOrder_dow)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
