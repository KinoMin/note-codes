package com.kino.sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

import java.util.Properties

/**
 * create by kino on 2021/1/14
 */
object kafkaSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "bigdata001:9092")
    prop.setProperty("group.id", "consumer-group1")
    prop.setProperty("zookeeper.connect", "bigdata001:2181")
    prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("auto.offset.reset", "earliest")
    val kafkaSourceStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), prop))
    kafkaSourceStream.print()

    val kafkaSinkStream = new FlinkKafkaProducer011[String]("bigdata001:9092", "sensor1", new SimpleStringSchema())
    kafkaSinkStream.setWriteTimestampToKafka(true)

    kafkaSourceStream.addSink(kafkaSinkStream)
    env.execute(this.getClass.getName)
  }
}
