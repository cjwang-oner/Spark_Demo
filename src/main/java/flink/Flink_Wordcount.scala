package flink

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.collection.immutable
import scala.util.Random
object Flink_Wordcount {
  def main(args: Array[String]): Unit = {
    /*
    批处理
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val textFile = environment.readTextFile("E:\\Spark_Demo\\in\\word")
    val value = textFile
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(1)
      .sum(1)
    value.print()
    */
    //TODO 流式处理
    //创建流处理环境
    val steamEnvironment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //TODO 设置并行度 并行度设置配置文件《 全局设置《每个算子单独设置
    //设置并行度为2
    steamEnvironment.setParallelism(2)
    //接收socket 文本流
    val socketdata: DataStream[String] = steamEnvironment.socketTextStream("127.0.0.1", 9999)
    val value: DataStream[(String, Int)] = socketdata.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)
    value.print().setParallelism(1)
    steamEnvironment.execute()
  }
}
