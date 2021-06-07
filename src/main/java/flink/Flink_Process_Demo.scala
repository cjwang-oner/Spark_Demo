package flink

import org.apache.flink.api.common.state._
import org.apache.flink.configuration._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 状态  process方法
 */
object Flink_Process_Demo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val value: DataStream[String] = env.socketTextStream("localhost", 9999)

    val value1: DataStream[SensorReading] = value.map(x => {
      val strs: Array[String] = x.split(",")
      SensorReading(strs(0), strs(1).toLong, strs(2).toDouble)
    })
    value1.keyBy(_.id).process(new MyProcess()).print()
    env.execute()
  }
}

/**
 * K  I  O k:key类型  I:输入类型  O:输出类型
 * 10秒钟温度连续上升  则输出报警信息
 */
class MyProcess extends KeyedProcessFunction[String,SensorReading,String] {
  //记录上一次温度
  var lastTemperature: ValueState[Double] = _
  //记录定时器时间
  var lastTime: ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {
    lastTemperature = getRuntimeContext.getState(new ValueStateDescriptor[Double]("temperature", classOf[Double]))
    lastTime = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time", classOf[Long]))
  }

  override def processElement(sens: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    //获取上一次温度
    val lasttem: Double = lastTemperature.value()
    //获取定时器时间
    val lasttime: Long = lastTime.value()
    if (sens.temperature > lasttem && lasttime == 0) {
      val ts = context.timerService().currentProcessingTime() + 10000
      //注册定时器
      context.timerService().registerProcessingTimeTimer(ts)
      //修改时间
      lastTime.update(ts)
    } else if (sens.temperature < lasttem) {
      //当温度发现下降 则删除定时器任务
      context.timerService().deleteProcessingTimeTimer(lasttime)
      //将状态清空
      lastTime.clear()
    }
    lastTemperature.update(sens.temperature)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("温度连续上升")
  }
}