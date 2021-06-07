package flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Table, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
/**
 * 窗口操作
 */
object Flink_Table_Api_window {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val tabEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val value: DataStream[String] = env.readTextFile("")

    val value1: DataStream[SensorReading] = value.map(x => {
      val strs: Array[String] = x.split(",")
      SensorReading(strs(0), strs(1).toLong, strs(2).toDouble)
    })

    val value2: DataStream[SensorReading] = value1.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading) = t.timestamp * 1000L
    })

    //创建窗口
    val table: Table = tabEnv.fromDataStream(value2, 'id, 'timestamp as 'ts, 'temperature, 'timestamp.rowtime as 'rt)
    //创建一个窗口大小为10分钟的滚动窗口
    val table1: Table = table.window(Tumble over 10.minutes on "rowtime" as 'w)
      .groupBy('w, 'id)
      .select('id, 'id.count, 'w.rowtime, 'w.`end`)






























  }
}
