package operate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //创建sparkconf对象 设置spark配置
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建spark上下文对象
    val sc = new SparkContext(config)
    //逐行读取文件数据
    val words: RDD[String] = sc.textFile("in")
    //转换单词形式
    val word: RDD[String] = words.flatMap(_.split(" "))
    //将单词转换成k-v形式
    val value: RDD[(String, Int)] = word.map((_, 1))
    //将数据进行聚合
    val value1: RDD[(String, Int)] = value.reduceByKey(_ + _)
    val tuples: Array[(String, Int)] = value1.collect()
    tuples.foreach(println)
  }
}
