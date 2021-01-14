package com.kino.udf

import com.kino.mode.SensorReading
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

/**
 * Flink 暴露了所有的 udf 函数的接口(实现方式为接口或抽象类), 例如: MapFunction, FilterFunction, ProcessFunction 等
 * create by kino on 2021/1/14
 */
object FilterFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputDS = env.readTextFile("D:\\work\\note-codes\\FlinkTutorial\\src\\main\\resources\\SensorReading.txt")
      .map(x => {
        val splits = x.split(",")
        SensorReading(splits(0).toString,splits(1).toLong,splits(2).toDouble)
      })
    val filterDS = inputDS.filter(new MyFilterFunction("30"))
    filterDS.print()
    env.execute(this.getClass.getName)
  }
}

/**
 * 自定义 FilterFunction, 将传感器温度小于 condition 的过滤掉
 * @param condition: 传入的温度值, 用来对比
 */
class MyFilterFunction(condition: String) extends FilterFunction[SensorReading]{
  override def filter(t: SensorReading): Boolean = {
    t.temperature < condition.toInt
  }
}