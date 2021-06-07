package spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Spark_sql {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("accumulator")
    //TODO 创建sparksession对象
    val spark: SparkSession = SparkSession.builder().config(config).getOrCreate()
    //导入隐式转换
    import spark.implicits._
    //TODO RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "wangwu", 18), (2, "zhangsan", 20), (3, "lisi", 30)))
    //TODO RDD TO DF 给数据赋予结构
    val df: DataFrame = rdd.toDF("id", "name", "age")
    df.show()
    //TODO DF TO DS  给数据类型
    val ds: Dataset[Person] = df.as[Person]
    ds.show()

    //TODO 创建tempView
    ds.createTempView("table")
    spark.sql("select age+1 from table").show()
    //TODO DSL风格
    ds.select($"age"+1).show()

    //TODO DS TO RDD
    val rdd1: RDD[Person] = ds.rdd
    rdd1.foreach(x=>println(x))
    //TODO 释放资源
    spark.stop()
  }
}
//样例类
case class Person(id:Int,name:String,age:Int)
