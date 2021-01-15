package com.kino.sink

import com.kino.mode.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

/**
 * create by kino on 2021/1/15
 */
object FileSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取数据
    val inputStream = env.readTextFile("D:\\work\\note-codes\\FlinkTutorial\\src\\main\\resources\\SensorReading.txt")

    // 转换成样例类类型(简单的转换操作)
    val dataStream = inputStream.map(x => {
      val splits = x.split(",")
      SensorReading(splits(0), splits(1).toLong, splits(2).toDouble)
    })

    dataStream.print()
    //    dataStream.writeAsCsv("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\out.txt")
    dataStream.addSink(
      StreamingFileSink.forRowFormat(
        new Path("D:\\work\\note-codes\\FlinkTutorial\\src\\main\\resources\\out.txt"),
        new SimpleStringEncoder[SensorReading]()
      ).build()
    )

    env.execute(this.getClass.getName)
  }
}
