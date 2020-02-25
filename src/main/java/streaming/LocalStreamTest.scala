package streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object LocalStreamTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC")
    //.setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))
    //ssc.checkpoint("D:\\DATA\\checkpoint")
    ssc.checkpoint("hdfs:///streaming/checkpoint")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val addFunc = (curValues: Seq[Long], preValueState: Option[Long]) => {
      val preCount = preValueState.getOrElse(0L)
      val curCount = curValues.sum
      Some(preCount + curCount)
    }

    val lines = ssc.socketTextStream("192.168.137.3", 9999)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map((_, 1L)).updateStateByKey[Long](addFunc)
    //.reduceByKeyAndWindow((a:Long,b:Long)=>a+b,Seconds(10),Seconds(2))
    //.reduceByKey(_+_) //[1,1,1,2,3] ((((1+1)+1)+2)+3)
    wordCounts.print()
    wordCounts.saveAsTextFiles(args(0))

    // yarn application -kill applicationid
    ssc.start()
    ssc.awaitTermination()
  }
}
