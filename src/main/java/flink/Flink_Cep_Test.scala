package flink

import java.util

import org.apache.flink.cep._
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._

/**
 * CEP测试
 */
object Flink_Cep_Test {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val value: DataStream[String] = env.readTextFile("E:\\Spark_Demo\\in\\word ")

    val value1: Pattern[String, String] = Pattern.begin[String]("first").where(_.contains("a")).timesOrMore(2)
    val value2: PatternStream[String] = CEP.pattern(value, value1)
    val value3: DataStream[String] = value2.select(new myCep)
    value3.print()
    env.execute()
  }
}
class myCep extends PatternSelectFunction[String,String]{
  override def select(map: util.Map[String, util.List[String]]): String = {
    val strBuffer = new StringBuffer()
    val strings: util.List[String] = map.get("first")
    for(str <- 0 to strings.size()-1){
      strBuffer.append(strings.get(str))
    }
    strBuffer.toString
  }
}
