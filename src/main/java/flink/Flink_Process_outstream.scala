package flink

import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 利用process生成侧输出流
 */
object Flink_Process_outstream {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val value: DataStream[String] = env.socketTextStream("localhost", 9999)
    val value1 = value.map(x => {
      val strs: Array[String] = x.split(",")
      SensorReading(strs(0), strs(1).toLong, strs(2).toDouble)
    })
    val value3: DataStream[SensorReading] = value1.process(new OutputTagProcess())
    value3.print("high")
    value3.getSideOutput(new OutputTag[SensorReading]("low")).print("low")
    env.execute()
  }
}
class OutputTagProcess extends ProcessFunction[SensorReading,SensorReading]{
  private val value = new OutputTag[SensorReading]("low")
  override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
    if(i.temperature > 37.2){
      collector.collect(i)
    }else {
      context.output(value, i)
    }
  }
}