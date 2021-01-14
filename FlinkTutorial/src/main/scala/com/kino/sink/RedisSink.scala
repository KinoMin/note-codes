package com.kino.sink

import com.kino.mode.SensorReading
import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig

import java.util.Properties

/**
 * create by kino on 2021/1/14
 */
object RedisSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.setProperty("bootstrap.servers", "bigdata001:9092")
    props.setProperty("group.id", "consumer-group1")
    props.setProperty("zookeeper.connect", "bigdata001:2181")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("auto.offset.reset", "earliest")
    val kafkaSourceStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), props))
      .map(x => {
        val splits = x.split(",")
        SensorReading(splits(0).trim, splits(1).trim.toLong, splits(2).trim.toDouble)
      })
    kafkaSourceStream.print()

    val conf = new FlinkJedisPoolConfig.Builder().setHost("192.168.220.110").setPort(6379).build()

    kafkaSourceStream.addSink(new RedisSink[SensorReading](conf, new MyRedisMapper))
  }
}

class MyRedisMapper extends RedisMapper[SensorReading] {
  // 将传感器的 id 和 温度值保存成哈希表: HSET key field value
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
  }

  override def getKeyFromData(data: SensorReading): String = {
    data.id
  }

  override def getValueFromData(data: SensorReading): String = {
    data.temperature.toString
  }
}