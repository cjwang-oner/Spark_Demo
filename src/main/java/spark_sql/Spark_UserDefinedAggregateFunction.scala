package spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

/**
 * TODO 自定义聚合函数(弱类型)
 */
object Spark_UserDefinedAggregateFunction {
  def main(args: Array[String]): Unit = {
    //TODO 创建Spark对象
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("userDefinedAggregateFunction")
    val spark: SparkSession = SparkSession.builder().config(config).getOrCreate()

    val avgage: MyAvgage = new MyAvgage
    val avgAge: UserDefinedAggregateFunction = spark.udf.register("avgAge", avgage)
    val rdd: RDD[String] = spark.sparkContext.textFile("in/2.json")
    
    rdd.collect().foreach(println)
    //创建DF
    val df: DataFrame = spark.read.json("in/2.json")
    df.show()
    df.createTempView("person")
    spark.sql("select avgAge(age) from person").show()
    //释放资源
    spark.stop()
  }
}
class MyAvgage extends UserDefinedAggregateFunction{
  //TODO 输入的结构
  override def inputSchema: StructType = {
    new StructType().add("age",LongType)
  }

  //TODO 缓冲区结构
  override def bufferSchema: StructType = {
    new StructType().add("sum",LongType).add("count",LongType)
  }

  //返回值类型
  override def dataType: DataType = DoubleType

  //TODO 是否稳定（多次输入同一值 返回值是否一样）
  override def deterministic: Boolean = true

  //TODO 初始化缓冲区
  override def initialize(buffer: MutableAggregationBuffer)= {
    //sum初始化为0
    buffer(0) = 0L
    //count初始化为0
    buffer(1) = 0L
  }

  //TODO 根据输入值更改缓冲区
  override def update(buffer: MutableAggregationBuffer, input: Row) = {
  buffer(0)= buffer.getLong(0) + input.getLong(0)
  buffer(1) = buffer.getLong(1) + 1L
  }

  //TODO 合并缓冲区(由于各个Excutor都在执行)
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = (buffer1.getLong(0) + buffer2.getLong(0))
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //TODO 返回值
  override def evaluate(buffer: Row) = {
    buffer.getLong(0).toDouble/buffer.getLong(1)
  }
}
