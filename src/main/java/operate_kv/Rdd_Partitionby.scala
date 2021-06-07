package operate_kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * K-V算子 partitionBy 按照指定规则重新分区
 * 自定义分区器 Partitioner  numPartitions:分区数量 getPartition:分区规则
 */
object Rdd_Partitionby {
  def main(args: Array[String]): Unit = {
    //map算子  对RDD元素进行转换 RDD原结构不变 有多少元素循环多少次 就有多少次网络传输 效率较低
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Map")
    //创建spark上下文对象
    val sc = new SparkContext(config)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "aaaa"), (2, "bbbb"), (3, "cccc"), (4, "dddd")), 4)
    rdd.glom().collect().foreach(array=>println(array.mkString(",")))
    val rdd2: RDD[(Int, String)] = rdd.partitionBy(new PartitionWork(3))
    rdd2.glom().collect().foreach(array=>println(array.mkString(",")))
  }
}
class PartitionWork(partitions : Int) extends Partitioner{
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    if(key.isInstanceOf[Int]){
      val key_int: Int = key.asInstanceOf[Int]
      key_int%partitions
    }else{
      1
    }
  }
}