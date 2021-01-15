package com.kino.windows

import com.kino.mode.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.Properties

/**
 * create by kino on 2021/1/15
 * 滚动窗口(Tumbling Window)
 */
object TumblingWindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度
    env.setParallelism(1)

    val props = new Properties()
    props.put("bootstrap.servers","bigdata001:9092")
    props.put("group.id","consumer-group1")
    props.put("zookeeper.connect","bigdata001:2181")
    props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset","earliest")
    // 设置 Kafka Source
    var inputStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), props))
    // 将读取到的数据源转换成 样例类
    val dataStream = inputStream.map(x => {
      val splits = x.split(",")
      SensorReading(splits(0).toString, splits(1).toLong, splits(2).toDouble)
    })
    // 将样例类转换成 元祖, 然后做 keyBy, 设置 15s 一个窗口
    val outputSteam: DataStream[(String, Double)] = dataStream
      .map(x => (x.id, x.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))
      .reduce((x1, x2) => (x1._1, x1._2.min(x2._2)))

    outputSteam.print()
    env.execute(this.getClass.getName)
  }
}
