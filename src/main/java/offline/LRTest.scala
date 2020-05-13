package offline

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import offline.Feature.GenerateFeature
import org.apache.spark.ml.classification.{BinaryLogisticRegressionTrainingSummary, LogisticRegression}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.functions._

object LRTest {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val warehouse = "hdfs://master:9000/usr/soft/apache-hive-1.2.2-bin/warehouse"
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir",warehouse)
      .appName("LRTest")
      .master("local[8]")
      .enableHiveSupport()
      .getOrCreate()

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    //1、读取对应所需的数据
    val priors = spark.sql("select * from badou.priors")
    val orders = spark.sql("select * from badou.orders")
    val trains = spark.sql("select * from badou.trains")

    priors.show()
    orders.show()

    //2、获取生成的特征
    val op = orders.join(priors,"order_id").persist(StorageLevel.MEMORY_AND_DISK)
    val (userFeat,prodFeat) = GenerateFeature(priors,orders)

    val opTrain=orders.join(trains,"order_id")
    //train的数据标签为1
    val user_real = opTrain.select("product_id","user_id").distinct().withColumn("label",lit(1))
    //priors表中的标签为0，train中的标签为1，合并在一起
    val trainData = op.join(user_real,Seq("product_id","user_id"),"outer")
      .select("user_id","product_id","label").distinct().na.fill(0)
    //关联特征：根据user_id关联用户特征，根据product_id关联product
    val train = trainData.join(userFeat,"user_id").join(prodFeat,"product_id")
    //rformula将对应值（不管离散还是连续）转变成向量形式放入到features这一列中
    val rformula = new RFormula().setFormula("label~ u_avg_day_gap+u_ord_cnt+u_prod_size+" +
      "u_prod_uni_size+u_agv_ord_prods+prod_sum_rod+prod_rod_rate+prod_cnt")
      .setFeaturesCol("features").setLabelCol("label")
    //formula的数据转换
    val df = rformula.fit(train).transform(train).select("features","label")
    df.show()
//#######################################LR model########################################################
    //实例化LR模型，setElasticNetParam 0:L2,1:L1 通过正则解决过拟合问题 0~1之间是L1和L2正则的组合
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0) //具体参数值
    //将训练集70%作为训练，30%作为test
    val Array(trainingData,testData)=df.randomSplit(Array(0.7,0.3))
    //训练模型
    val lrModel = lr.fit(trainingData)
    //打印参数（权重w，截距b）
    print(s"weights:${lrModel.coefficients} intercept:${lrModel.intercept}")
    //收集日志
    val trainingSummary=lrModel.summary
    val objectHistory = trainingSummary.objectiveHistory
    //打印loss
    objectHistory.foreach(loss=>println(loss))
    //用auc来评价模型训练结果，auc是roc曲线下的面积
    val binarySummary=trainingSummary.asInstanceOf[BinaryLogisticRegressionTrainingSummary]
    val roc = binarySummary.roc
    roc.show()
    println(binarySummary.areaUnderROC)
  }
}
