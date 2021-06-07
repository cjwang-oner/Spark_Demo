package flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * todo watermark 水位线---设置延迟时间
 *      处理乱序数据 能顺利进入窗口的一种机制
 *      当watermark = 关窗时间 = 当前最大事件时间 - 最大乱序时间(即允许迟到的时间) 时认为该窗口所有的数据到齐 触发关窗操作
 */
class Flink_Watermark {
  //TODO  窗口 水位线
  def main(args: Array[String]): Unit = {
    //TODO 创建Flink上下文环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //TODO 创建source-socket
    val value: DataStream[String] = environment.socketTextStream("localhost", 9999)

    //
    val senStream: DataStream[SensorReading] = value.map(x => {
      val strs: Array[String] = x.split(",")
      SensorReading(strs(0), strs(1).toLong, strs(2).toDouble)
    })
    //TODO 设置时间语义-Eventtime
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //TODO 设置watermark
    val waterStream: DataStream[SensorReading] = senStream.assignTimestampsAndWatermarks(new MyWatermark())

    /*TODO 设置窗口 TumblingEventTimeWindow -- 滚动窗口 参数一:窗口大小  滚动窗口可以理解成特殊的活动窗口 即滑动大小 = 窗口大小
                   SlidingEventTimeWindow  -- 滑动窗口 参数一:窗口大小  参数二:滑动大小

    * */
    //waterStream.keyBy(_.id).window(TumblingEventTimeWindows.of(Time.seconds(5)))
    waterStream.keyBy(_.id).window(SlidingEventTimeWindows.of(Time.seconds(5),Time.seconds(2)))
    
  }
}
//TODO AssignerWithPeriodicWatermarks -- 周期性生成watermark  默认周期为200ms 可以手动设置周期
class MyWatermark extends AssignerWithPeriodicWatermarks[SensorReading]{

  //TODO 当前最大时间
  var maxTime: Long = Long.MinValue

  //TODO 设置延迟时间 - 一分钟
  val bound: Long = 60 * 1000L

  //TODO 得到Watermark
  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTime - bound)
  }

  //TODO 设置Eventtime
  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    maxTime = maxTime.max(t.timestamp)
    t.timestamp
  }
}



