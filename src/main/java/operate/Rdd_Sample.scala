package operate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*TODO
   Sample算子:做数据抽样sample(withReplacement, fraction, seed)
   withReplacement:表示是否放回 true:放回 false:不放回抽样
   fraction：表示抽样样本大小 0.3表示抽30%
   seed:随机数种子 随机数种子相同的随机数都是伪随机 java里面一般采用时间戳作为随机数种子
*/
object Rdd_Sample {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Map")
    //创建spark上下文对象
    val sc = new SparkContext(config)

    val rdd: RDD[Int] = sc.makeRDD(1 to 100)
    val rdd1: RDD[Int] = rdd.sample(true, 0.4, 1)
    println(rdd1.collect().size)
    rdd1.collect().foreach(println)
  }
}
