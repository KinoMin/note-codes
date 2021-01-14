package com.kino.source

import com.kino.mode.SensorReading
import org.apache.flink.streaming.api.scala._

/**
 * create by kino on 2021/1/14
 * 集合 source
 */
object ArraySource{
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    ))
    stream.print()
    env.execute(this.getClass.getName)
  }
}
