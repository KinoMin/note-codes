package com.kino.source

import org.apache.flink.streaming.api.scala._
import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
/**
 * Created by kino on 2020/12/9.
 */
object KafkaSource {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "hadoop1:9092")
        properties.setProperty("group.id", "consumer-group2")
        properties.setProperty("zookeeper.connect", "hadoop1:2181")
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("auto.offset.reset", "earliest")

        /**
         * 查看 Kafka topic
         *  kafka-topics --zookeeper hadoop1:2281 --list
         * 查看详情
         *  kafka-topics --zookeeper hadoop1:2181 --describe --topic sensor
         * 创建 topic
         *  kafka-topics --zookeeper hadoop1:2181 --create --replication-factor 3 --partitions 1 --topic sensor
         * 生产消息
         *  kafka-console-producer --broker-list hadoop1:9092 --topic sensor
         * 消费消息
         *  kafka-console-consumer --bootstrap-server hadoop1:9092 --from-beginning --topic sensor
         */
        val kafkaStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

        kafkaStream.print()
        
        env.execute(this.getClass.getName)
    }
}
