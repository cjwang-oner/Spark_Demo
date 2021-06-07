package operate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//TODO 根据函数返回值进行分组
object Rdd_Groupby {
  def main(args: Array[String]): Unit = {
    //map算子  对RDD元素进行转换 RDD原结构不变 有多少元素循环多少次 就有多少次网络传输 效率较低
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Map")
    //创建spark上下文对象
    val sc = new SparkContext(config)

    val rdd: RDD[Int] = sc.makeRDD(1 to 10)
    //TODO K：分组的K v:表示分组的数据集合
    val rdd2: RDD[(Int, Iterable[Int])] = rdd.groupBy(_ % 2)

    rdd2.collect().foreach(println)
  }
}
