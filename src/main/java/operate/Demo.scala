package operate

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/*
 TODO
    coalesce算子:减少分区数
    coalesce(numPartitions, shuffle) 是否shuffle 默认为false 为false时其实是合并分区
    repartition 将RDD全部打乱重新分组  存在shuffle过程  底层其实也是调用coalesce 但shuffle固定为true
 */
object Demo {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Map")
    //创建spark上下文对象
    val sc = new SparkContext(config)

      StorageLevel
    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))

    //3.创建一个Search对象
    val search = new Search("a")

    //4.运用第一个过滤函数并打印结果
    val match1: RDD[String] = search.getMatche2(rdd)
    match1.collect().foreach(println)
  }
}
class Search(query:String) extends Serializable{

  //过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  //过滤出包含字符串的RDD
  def getMatch1 (rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  //过滤出包含字符串的RDD
  def getMatche2(rdd: RDD[String]): RDD[String] = {
    val query_ = this.query
    rdd.filter(x => x.contains(query))
  }

}

