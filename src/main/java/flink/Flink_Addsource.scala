package flink

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._

import scala.collection.immutable
import scala.util.Random

object Flink_Addsource {
  def main(args: Array[String]): Unit = {
    //自定义source
    //获取Flink流处理环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val value: DataStream[SensorReading] = environment.addSource(new MySource())
    value.print()
    environment.execute()
  }
}

//样例类
//case class SensorReading(id:String,timestamp:Long,temperature:Double)

//自定义source
class MySource extends SourceFunction[SensorReading]{
  //控制数据源是否运行
  var running = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit ={
    val rand = new Random()
    val tuples: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(i => {
      println(rand.nextGaussian()*20)
      ("sensor" + i, 65 + rand.nextGaussian() * 20)
    })
    while(running){
      val timestamp = System.currentTimeMillis()
      val readings: immutable.IndexedSeq[SensorReading] = tuples.map(i => {
        SensorReading(i._1, timestamp, i._2 + rand.nextGaussian())
      })
      readings.foreach(sourceContext.collect(_))
      Thread.sleep(10000)
    }
  }

  //停止数据源的运行
  override def cancel(): Unit = {
    running = false
  }
}
