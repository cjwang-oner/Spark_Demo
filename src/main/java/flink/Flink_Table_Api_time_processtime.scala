package flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
 * 定义时间特性  --- 处理时间
 */
object Flink_Table_Api_time_processtime {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val tabEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    //1.直接在DataStream 转换 table 时指定
    val value: DataStream[String] = env.readTextFile("")
    val value1: DataStream[SensorReading] = value.map(x => {
      val strs: Array[String] = x.split(",")
      SensorReading(strs(0), strs(1).toLong, strs(2).toDouble)
    })
    tabEnv.fromDataStream(value1,'id,'timestamp,'temperature,'rt.proctime)

    //2.在与外部系统连接是指定
    tabEnv.connect(new FileSystem().path(""))
      .withFormat(new Csv())
      .withSchema(
        new Schema()
          .field("id",DataTypes.STRING())
          .field("ts",DataTypes.BIGINT())
          .field("temperature",DataTypes.DOUBLE())
          .field("pt",DataTypes.TIMESTAMP(3))
          .proctime()
      ).createTemporaryTable("inputTable")

    //3.在创建table DDL时指定
    val string: String =
      """
        |create datatable(
        | id varchar(20) not null,
        | ts bigint,
        | temperature double,
        | pt as proctime
        |)
        |with(
        | 'connector.type' = 'filesystem',
        | 'connector.path' = 'file:///D:\\..\\sensor.txt',
        | 'format.type' = 'csv'
        |)
        |
        |
        |)
        |""".stripMargin
    tabEnv.sqlUpdate(string)
  }
}
