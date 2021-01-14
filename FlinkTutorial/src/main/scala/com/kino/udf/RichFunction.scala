package com.kino.udf

import com.kino.mode.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * "富函数(RichFunction)" 和常规的函数的不同之处在于, RichFunction 可以获取运行环境的上下文, 并拥有一些生命周期方法, 可以实现更复杂的功能
 * create by kino on 2021/1/14
 */
object RichFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputDS = env.readTextFile("D:\\work\\note-codes\\FlinkTutorial\\src\\main\\resources\\SensorReading.txt")
    inputDS.flatMap(new MyFlatMapRichFunction()).print()
    env.execute(this.getClass.getName)
  }
}

class MyFlatMapRichFunction extends RichFlatMapFunction[String, SensorReading]{

  var startTime: Long = _

  override def open(parameters: Configuration): Unit = {
    startTime = System.currentTimeMillis()
  }

  override def flatMap(in: String, out: Collector[SensorReading]): Unit = {
    val splits = in.split(",")
    out.collect(SensorReading(s"$startTime-"+splits(0).toString+s"-${System.currentTimeMillis()}", splits(1).toLong, splits(2).toDouble))
  }

  override def close(): Unit = {
    println("执行所用时间: " + (System.currentTimeMillis() - startTime))
  }
}