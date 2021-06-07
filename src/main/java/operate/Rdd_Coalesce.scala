package operate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
 TODO
    coalesce算子:减少分区数
    coalesce(numPartitions, shuffle) 是否shuffle 默认为false 为false时其实是合并分区
    repartition 将RDD全部打乱重新分组  存在shuffle过程  底层其实也是调用coalesce 但shuffle固定为true
 */
object Rdd_Coalesce {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Map")
    //创建spark上下文对象
    val sc = new SparkContext(config)

    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 4)
    val rdd1: RDD[Array[Int]] = rdd.glom()
    rdd1.collect().foreach(array => println(array.mkString(",")))
    val rdd2: RDD[Int] = rdd.coalesce(3)
    rdd2.glom().collect().foreach(array => println(array.mkString(",")))
    rdd2.repartition(2)
  }
}
