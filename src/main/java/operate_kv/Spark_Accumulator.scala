package operate_kv

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * TODO 累加器 excutor会将结果返回
 * TODO spark 自带long类型累加器 add函数为数字相加  合并函数也为累加器相加
 * TODO 累加器只有在RDD执行计算(即行动算子)后才生效,转换算子没有真正执行计算  只是封装计算逻辑  累加器不会也不会触发真正的累加操作
 * TODO cache 可以避免在转换操作时累加器重复累加的情况   检查点机制实现不了这种操作
 */
object Spark_Accumulator {
  def main(args: Array[String]): Unit = {
    // TODO map算子  对RDD元素进行转换 RDD原结构不变 有多少元素循环多少次 就有多少次网络传输 效率较低
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("accumulator")
    //TODO 创建spark上下文对象
    val sc = new SparkContext(config)
    sc.setCheckpointDir("checkPoint")

    val rdd: RDD[String] = sc.makeRDD(Array("Hbase", "Spark", "Hive", "Hadoop", "Scala"))
    //声明累加器 spark 自带long类型累加器
    /* TODO 整型累加器
    val acc: LongAccumulator = sc.longAccumulator("wcjLongAccumulator")
    val rdd2: RDD[Int] = rdd.map(x => {
        acc.add(1)
       x+1
    })
    rdd2.collect().foreach(println)
    println("accumulator:"+acc.value)
    */
    //TODO 自定义字符类型累加器
    // TODO 声明累加器
    val stringAccumulator: StringAccumulator = new StringAccumulator
    //TODO 向spark注册累加器
    sc.register(stringAccumulator)

    val rdd2: RDD[(String, Int)] = rdd.map(x => {
      stringAccumulator.add(x)
      (x, 1)
    })
//    rdd2.cache()
    rdd2.checkpoint()
    rdd2.foreach(x=>println(x._1))
    rdd2.collect()
    val arrays: util.ArrayList[String] = stringAccumulator.value
    println(arrays)
    sc.stop()

  }
}

/**
 *TODO 自定义字符串累加器
 * AccumulatorV2[input,output]：input 累加器输入类型 output：累加器输出类型
 */
class StringAccumulator extends AccumulatorV2[String,util.ArrayList[String]]{
  private val arraylist : util.ArrayList[String] = new util.ArrayList[String]()
  //TODO 累加器是否为空
  override def isZero: Boolean = arraylist.isEmpty
  //TODO 创建一个累加器
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = new StringAccumulator()
  //TODO 重置累加器
  override def reset(): Unit = arraylist.clear()
  //TODO 往累加器添加元素
  override def add(value: String): Unit = {
    if(value.contains("H")) {
      arraylist.add(value)
    }
  }

  //TODO 合并累加器 合并集合
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    if (other.isInstanceOf[StringAccumulator]) {
      val accumulator: StringAccumulator = other.asInstanceOf[StringAccumulator]
      arraylist.addAll(accumulator.value)
    }
  }
  //TODO 取出累加器的值
  override def value: util.ArrayList[String] = arraylist
}
