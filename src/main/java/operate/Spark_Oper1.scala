package operate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Oper1 {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建spark上下文对象
    val sc = new SparkContext(config)

    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7))
    val rddtuple: RDD[(Int, String)] = rdd.mapPartitionsWithIndex {
      (num, datas) => {
        datas.map((_, "分区号:" + num))
      }
    }
    rddtuple.collect().foreach(println)
  }
}
