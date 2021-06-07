package operate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//TODO Filter算子:根据条件过滤 true 符合条件留下   false:不符合要求  过滤掉
object Rdd_Filter {
  def main(args: Array[String]): Unit = {
    //map算子  对RDD元素进行转换 RDD原结构不变 有多少元素循环多少次 就有多少次网络传输 效率较低
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Map")
    //创建spark上下文对象
    val sc = new SparkContext(config)

    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 4)

    val rdd1: RDD[Int] = rdd.filter(_ > 2)

    rdd1.collect().foreach(println)
  }
}
