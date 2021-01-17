package com.kino.windows

import com.kino.mode.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * create by kino on 2021/1/17
 */
object WatermarkWindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置时间语义为 EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置周期性生成 Watermark 的时间(毫秒)
    env.getConfig.setAutoWatermarkInterval(100L)

    val inputStream = env.socketTextStream("localhost", 8888)
    val dataStream = inputStream.map(x => {
      val splits = x.split(",")
      SensorReading(splits(0).toString, splits(1).toLong, splits(2).toDouble)
    })
      // 当时间语义是 EventTime, 并且数据是有序的时候, 可以直接指定数据中的 timestamp 字段
//      .assignAscendingTimestamps(_.timestamp)
      // 当时间语义是 EventTime, 且数据是无须的时候, 需要使用此 API
      // 该 API 有两种类型的入参:
      //     1. AssignerWithPeriodicWatermarks: 有规律生成 Watermark 的API, 可以自定义生成 Watermark 的规则
      //     2. AssignerWithPunctuatedWatermarks: 没有规律的 API
//      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[SensorReading] {
//        // 延迟为 6s
//        val bound = 6000
//        var maxTime = Long.MaxValue
//
//        override def getCurrentWatermark: Watermark = new Watermark(maxTime - bound)
//
//        override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
//          element.timestamp
//        }
//      })
      // AssignerWithPeriodicWatermarks 的简写
      // BoundedOutOfOrdernessTimestampExtractor 也是 AssignerWithPeriodicWatermarks(周期性) 的
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(6)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp
        }
      })
      // 没有规律的 API
//      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[SensorReading] {
//        override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
//          new Watermark(extractedTimestamp)
//        }
//
//        override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = element.timestamp
//      })
      // 使用自定义的 Watermark 生成规则
//      .assignTimestampsAndWatermarks(new MyWatermark(6000L))

    val outputStream = dataStream
      .map(x => (x.id, x.temperature))
      .keyBy(0)
      // 设置滚动窗口
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .reduce((x1, x2) => (x1._1, x1._2.max(x2._2)))
    outputStream.print("outputStream")
    dataStream.print("dataStream")
    env.execute(this.getClass.getName)
  }
}

// Watermark 可以继承于:
//    1. AssignerWithPeriodicWatermarks: 有规律生成 Watermark 的API, 可以自定义生成 Watermark 的规则
//    2. AssignerWithPunctuatedWatermarks: 没有规律的 API
class MyWatermark(bound: Long) extends AssignerWithPeriodicWatermarks[SensorReading]{
  var maxTime = Long.MaxValue

  override def getCurrentWatermark: Watermark = new Watermark(maxTime - bound)

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    element.timestamp
  }
}