package operate_kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * TODO 根据KEY做计算
 */
object Rdd_ReduceByKey {
  def main(args: Array[String]): Unit = {
    // TODO map算子  对RDD元素进行转换 RDD原结构不变 有多少元素循环多少次 就有多少次网络传输 效率较低
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Map")
    //TODO 创建spark上下文对象
    val sc = new SparkContext(config)

    val value: RDD[(String, Int)] = sc.makeRDD(Array(("wcj", 3000), ("lhc", 2020), ("wcj", 3001)))

    val value1: RDD[(String, Int)] = value.reduceByKey((x, y) => {
      if (x > y) {
        x
      } else {
        y
      }
    })
    value1.collect().foreach(println)

//    val value1: RDD[Person] = value.map(x => {
//      new Person(x._1, x._2, x._3)
//    })
//    val person: Person = value1.max()
//    System.out.println(person.name+","+person.age+","+person.fv)

  }
}

//通过自定义类重写compare的方法，来实现课比较的规则
class Person(val name:String,val age:Int,val fv:Int) extends Ordered[Person] with Serializable{
  override def compare(that: Person): Int = {
    this.age - that.age
  }
}
