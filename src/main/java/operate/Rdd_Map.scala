package operate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//TODO 转换算子结构
object Rdd_Map {

  def main(args: Array[String]): Unit = {
    //map算子  对RDD元素进行转换 RDD原结构不变 有多少元素循环多少次 就有多少次网络传输 效率较低
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Map")
    //创建spark上下文对象
    val sc = new SparkContext(config)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("Scala", 1), ("Hadoop", 1), ("Spark", 2), ("Spark", 1), ("Hbase", 2), ("Hive", 1)))
    val rdd1: RDD[(String, Int)] = rdd.map(x => (x._1, x._2 * 2))
    val tuples: Array[(String, Int)] = rdd1.collect()
    print(tuples.mkString(","))


    //对分区内的数据进行 有多少个分区就进行几次数据的网络传播 有内存溢出的风险存在 参数:每个分区内的数据(可迭代的集合)
    val rdd2: RDD[(String, Int)] = rdd.mapPartitions { datas =>
      (
        //这里的map是Scala的map函数
        datas.map(x => (x._1, x._2 * 2))
        )
    }
    rdd.glom().collect().foreach(x => (println(x.mkString(","))))


    //对分区数据进行处理 带上分区号 index:分区号 datas：每个分区的数据
    val rdd3: RDD[(String, Int)] = rdd.mapPartitionsWithIndex { (index, datas) =>
      (
        datas.map(x => (x._1 + "分区号:" + index, x._2))
        )
    }
    print(rdd3.collect().mkString(","))
  }
}
