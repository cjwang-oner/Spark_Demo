package spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator

object Spark_Aggregator {
  def main(args: Array[String]): Unit = {
    //TODO 创建Spark对象
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("userDefinedAggregateFunction")
    val spark: SparkSession = SparkSession.builder().config(config).getOrCreate()
    //导入隐式转换
    import spark.implicits._
    //创建DF
    val df: DataFrame = spark.read.json("in/2.json")
    val ds: Dataset[UserBean] = df.as[UserBean]
    //将自定义聚合函数转换成列
    val ageObject: MyAvgAgeObject = new MyAvgAgeObject
    val value: TypedColumn[UserBean, Double] = ageObject.toColumn.name("MyAvgAge")
    ds.select(value).show()
    //释放资源
    spark.stop()
  }
}

/**
 * 样例类声明
 */
case class UserBean(id:BigInt,name:String,age:BigInt)
case class AvgBuffer(var sum:BigInt,var count:BigInt)
/**
 * Aggregator[-IN, BUF, OUT] IN：输入类型   BUF：缓冲区  OUT:输出类型
 */
class MyAvgAgeObject extends Aggregator[UserBean,AvgBuffer,Double]{
  //TODO 初始化缓冲区
  override def zero: AvgBuffer = {
    AvgBuffer(0,0)
  }

  //TODO 合并缓冲区和输入数据
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  //TODO 合并多个Excutor 缓冲区数据
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count +b2.count
    b1
  }

  //TODO 返回数据
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble/reduction.count.toDouble
  }

  override def bufferEncoder: Encoder[AvgBuffer] = {
    Encoders.product
  }

  override def outputEncoder: Encoder[Double] ={
    Encoders.scalaDouble
  }
}