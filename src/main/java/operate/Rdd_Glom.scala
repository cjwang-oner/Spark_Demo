package operate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//TODO glom算子 将每个分区的数据放到一个数组
object Rdd_Glom {
  def main(args: Array[String]): Unit = {
    //map算子  对RDD元素进行转换 RDD原结构不变 有多少元素循环多少次 就有多少次网络传输 效率较低
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Map")
    //创建spark上下文对象
    val sc = new SparkContext(config)

    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 4)
    val rdd2: RDD[Array[Int]] = rdd.glom()
    rdd2.collect().foreach(array => {
      println(array.mkString(","))
    })
  }
}
