package CF

import org.apache.spark.{SparkConf, SparkContext}


object ReadUdataFile {
  def main(args: Array[String]): Unit = {
    var masterUrl = "local[1]"
    var inputPath = "E:\\GitHub\\SparkDemo\\src\\main\\java\\data\\u.data"
    var outputPath = "E:\\output"

    if (args.length == 1) {
      masterUrl = args(0)
    } else if (args.length == 3) {
      masterUrl = args(0)
      inputPath = args(1)
      outputPath = args(2)
    }

    println(s"""masterUrl:$masterUrl, inputPath: $inputPath, outputPath: $outputPath""")

    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rowRdd = sc.textFile(inputPath)
    val resultRdd = rowRdd.flatMap(line => line.split("\\s+"))
      .map(word => (word, 1)).reduceByKey(_ + _)

    resultRdd.saveAsTextFile(outputPath)
  }
}
