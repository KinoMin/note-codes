package com.kino.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Create kino by 2020/12/9
 */
object FlieSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputDS = env.readTextFile("D:\\work\\note-codes\\FlinkTutorial\\src\\main\\resources\\SensorReading.txt")
    inputDS.print().setParallelism(4)
    env.execute(this.getClass.getName)
  }
}
