package com.kino.source

import com.kino.mode.SensorReading
import org.apache.flink.streaming.api.scala._

/**
 * 从集合中读取数据
 * Create kino by 2020/12/9
 */
object ListSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDS = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    ))

    inputDS.print()

    env.execute()
  }
}
