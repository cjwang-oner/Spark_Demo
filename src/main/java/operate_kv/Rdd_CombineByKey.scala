package operate_kv

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 * def combineByKey[C](
 * createCombiner: V => C,
 * mergeValue: (C, V) => C,
 * mergeCombiners: (C, C) => C): RDD[(K, C)] = self.withScope {
 * combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners)(null)
 * combineByKey函数主要有三个参数，而且这三个参数都是函数：
 * createCombiner: V => C 产生一个combiner的函数，将RDD[K,V]中的V转换成一个新的值C1
 * mergeValue: (C, V) => C 合并value的函数，将一个C1类型的值与一个V合并成一个新的C类型的值，假设这个新的C类型的值为C2
 * mergeCombiners: (C, C) => C) 将两个C类型的值合并为一个C类型的值
 */
object Rdd_CombineByKey {
  def main(args: Array[String]): Unit = {
    // TODO map算子  对RDD元素进行转换 RDD原结构不变 有多少元素循环多少次 就有多少次网络传输 效率较低
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Map")
    //TODO 创建spark上下文对象
    val sc = new SparkContext(config)
    val rdd2: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("a", 3), ("a", 5), ("c", 1), ("c", 2), ("b", 1), ("b", 3)),3)
    rdd2.glom().collect().foreach(array=>{println(array.mkString(","))})

    val rdd3: RDD[(String, (Int, Int))] = rdd2.combineByKey((_, 1), (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val rdd4: RDD[(String, Int)] = rdd3.map(x => {
      val x1 = x._2._1
      val x2 = x._2._2
      (x._1, x1 / x2)
    })
    rdd4.collect().foreach(println)
    val rdd5: RDD[(String, Int)] = rdd2.combineByKey(v => v, (acc: Int, v) => acc + v, (acc1: Int, acc2: Int) => acc1 + acc2)
    rdd5.collect().foreach(println)
  }
}
