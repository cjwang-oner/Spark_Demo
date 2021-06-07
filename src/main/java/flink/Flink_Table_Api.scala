package flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, OldCsv, Schema}
import org.apache.flink.types.Row
/**
 * table api
 */
object Flink_Table_Api {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()

    val tabEnv: StreamTableEnvironment = StreamTableEnvironment.create(env,settings)
    //创建inputTable表
    tabEnv.connect(new FileSystem().path("E:\\Spark_Demo\\in\\sensors"))
      .withFormat(new OldCsv)
      .withSchema(new Schema()
        .field("id",DataTypes.STRING)
        .field("timestamp",DataTypes.BIGINT)
        .field("temperature",DataTypes.DOUBLE)
      ).createTemporaryTable("inputTable")

    //table表 跟 Table对象的相互转换
    val table: Table = tabEnv.from("inputTable")

//    val table1: Table = table.select("id,temperature").filter("id = 'sensor_1'")
    val table2: Table = table.groupBy('id).select('id, 'id.count as 'cnt)
//    val value1: DataStream[(String, Double)] = tabEnv.toAppendStream[(String, Double)](table1)
//    val value: DataStream[(String, Long, Double)] = tabEnv.toAppendStream[(String, Long, Double)](table)
val value2: DataStream[(Boolean, Row)] = tabEnv.toRetractStream[Row](table2)
//    value1.print("value1")
//    value.print("value")
    value2.print("value2")
    env.execute()
  }
}
