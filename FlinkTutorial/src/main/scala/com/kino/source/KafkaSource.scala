package com.kino.source

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.Properties

/**
 * create by kino on 2021/1/14
 * kafka source
 */
object KafkaSource {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers","bigdata001:9092")
    properties.put("group.id","consumer-group1")
    properties.put("zookeeper.connect","bigdata001:2181")
    properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("auto.offset.reset","earliest")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
    stream.print()
    env.execute(this.getClass.getName)
  }
}
