package com.kino.windows

import com.kino.mode.SensorReading
import org.apache.flink.streaming.api.scala._

/**
 * create by kino on 2021/1/17
 */
object CountWindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream = env.socketTextStream("localhost", 8888)
    val dataStream = inputStream.map(x => {
      val splits = x.split(",")
      SensorReading(splits(0).toString, splits(1).toLong, splits(2).toDouble)
    })
    val outputStream = dataStream
      .map(x => (x.id, x.temperature))
      .keyBy(_._1)
      .countWindow(3, 1)
      .reduce((x1, x2) => (x1._1, x1._2.max(x2._2)))

    outputStream.print()
    env.execute(this.getClass.getName)
  }
}
