package flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Rowtime, Schema}

/**
 * Flink定义时间特性 -- eventtime
 */
object Flink_Table_Api_time_eventtime {
  def main(args: Array[String]): Unit = {
    //设置watermark
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tabEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    //1:定义tabke schema时定义
    tabEnv.connect(new FileSystem().path(""))
      .withFormat(new Csv)
      .withSchema(
        new Schema()
          .field("id", DataTypes.STRING())
          .field("timestamp", DataTypes.BIGINT())
          .field("temperature", DataTypes.DOUBLE())
          .rowtime(new Rowtime().timestampsFromField("timestamp").watermarksPeriodicBounded(1000))
      ).createTemporaryTable("inputTable")

    //2.直接在DataStream 转换成 Table时定义
    val value: DataStream[String] = env.readTextFile("")
    val value1: DataStream[SensorReading] = value.map(x => {
      val str: Array[String] = x.split(",")
      SensorReading(str(0), str(1).toLong, str(2).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading) = t.timestamp*1000L
    })
    val table: Table = tabEnv.fromDataStream(value1, 'id, 'timestamp.rowtime as 'ts, 'temperature)

    //3.创建表DDL时指定
    val string: String =
      """
        |create datatable(
        | id varchar(20) not null,
        | ts bigint,
        | temperature double,
        | rt AS TO_TIMESTAMP(FROM_UNIXTIME(rs)),
        | watermark fro rt as rt - interval  '1' second
        |)
        |with(
        |'connector.type' = 'filesystem',
        |'connector.path' = 'file:///D:\\..\\sensor.txt',
        |'format.type' = 'csv'
        |)
        |""".stripMargin
    tabEnv.sqlUpdate(string)


  }
}
