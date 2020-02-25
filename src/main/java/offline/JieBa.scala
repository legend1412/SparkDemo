package offline

import com.huaban.analysis.jieba.JiebaSegmenter
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import utils.Utils

object JieBa {
  def main(args: Array[String]): Unit = {
    //定义结巴分词类的序列化
    val conf = new SparkConf()
      .registerKryoClasses(Array(classOf[JiebaSegmenter]))

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Jieba")
      .config(conf)
      //  .config("spark.storge.memoryFaction",0.6)
      .enableHiveSupport()
      .getOrCreate()
    val df = spark.sql("select * from badou.news_noseg")
    //df.show();
    //结巴对sentence进行分词
    //    val segmenter = new JiebaSegmenter();
    //    //将对应结巴分类创建braodcast
    //    val broadcastSeg = spark.sparkContext.broadcast(segmenter);
    //    val jiebaUdf = udf{(sentence:String)=>
    //      val exeSegmenter = broadcastSeg.value
    //      exeSegmenter.process(sentence.toString,SegMode.INDEX)
    //        .toArray().map(_.asInstanceOf[SegToken].word)
    //        .filter(_.length>1).mkString("/")
    //
    //    val df_seg = df.withColumn("seg",jiebaUdf(col("sentence")))
    //    df_seg.show(50);
    val JiebaUtils = new Utils();
    val df_seg_utils = JiebaUtils.jieba_seg(df, "sentence")
    df_seg_utils.show(20)
  }
}