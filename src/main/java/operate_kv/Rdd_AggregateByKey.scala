package operate_kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * TODO 可以实现分区内 和 分区间 使用不同的计算规则  aggregateByKey[U](zeroValue: U)(seqOp: (U, V) => U, combOp: (U, U) => U) zerovalue:给分区内每个key的初始值 seqop:分区内的计算规则 combop:分区间的计算规则
 */
object Rdd_AggregateByKey {
  def main(args: Array[String]): Unit = {
    // TODO map算子  对RDD元素进行转换 RDD原结构不变 有多少元素循环多少次 就有多少次网络传输 效率较低
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Map")
    //TODO 创建spark上下文对象
    val sc = new SparkContext(config)
    val rdd2: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("a", 3), ("a", 5), ("c", 1), ("c", 2), ("b", 1), ("b", 3)),3)








    rdd2.glom().collect().foreach(array=>{println(array.mkString(","))})

    val rdd3: RDD[(String, Int)] = rdd2.aggregateByKey(0)(math.max(_, _), _ + _)
    rdd3.collect().foreach(println)

  }
}
