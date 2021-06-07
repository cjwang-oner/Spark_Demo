package flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time


/*
TODO 窗口  watermark demo
 */
object Flink_Kafka_watermark {
  def main(args: Array[String]): Unit = {
    //TODO 创建Flink上下文对象
    val streamEnvironment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    streamEnvironment.setParallelism(1)
    //TODO 设置时间语义  以事件时间为准
    streamEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //创建sokcet source
    val value: DataStream[String] = streamEnvironment.socketTextStream("localhost", 9999)
    val sens: DataStream[SensorReading] = value.map(x => {
      val strs: Array[String] = x.split(",")
      SensorReading(strs(0), strs(1).toLong*1000L, strs(2).toDouble)
    })
    //TODO 自定义watermark 生成周期时间 间隔  默认为200毫秒  下面我们设置成500毫秒
    streamEnvironment.getConfig.setAutoWatermarkInterval(500)
    //TODO 创建watermark periodicwatermark  以指定时间间隔 生成watermark 默认是200毫秒
    //TODO 指定事件时间  告诉系统 当前时间是多少  下面watermark 数据延时时间是15秒钟

    val waterStream: DataStream[SensorReading] = sens.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {
        override def extractTimestamp(t: SensorReading) = {
          t.timestamp
        }
      }
    )
    //TODO 生成一个滚动窗口  滚动SIZE为 15秒
    val value1: DataStream[SensorReading] = waterStream
      .keyBy(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(15)))
      //TODO 允许数据延时1分钟
      .allowedLateness(Time.minutes(1))
      //TODO 延时未到达数据放在侧输出流
      .sideOutputLateData(new OutputTag[SensorReading]("lateData"))
      .reduce((sen1, sen2) => {
        SensorReading(sen1.id, sen2.timestamp, sen1.temperature.min(sen2.temperature))
      })

    val value2: DataStream[SensorReading] = value1.getSideOutput(new OutputTag[SensorReading]("lateData"))
    value1.print()
    value2.print()
    streamEnvironment.execute()
  }
}
//TODO 传感器样例类
case class SensorReading(id:String,timestamp:Long,temperature:Double)


