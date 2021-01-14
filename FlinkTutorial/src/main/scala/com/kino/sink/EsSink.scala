package com.kino.sink

import com.kino.mode.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import java.util


/**
 * create by kino on 2021/1/14
 */
object EsSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1. 读取数据
    val inputStream = env.readTextFile("D:\\work\\note-codes\\FlinkTutorial\\src\\main\\resources\\SensorReading.txt")

    // 2. 转换成样例类类型(简单转换操作)
    val dataStream = inputStream.map(x => {
      val arr = x.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    // 3. 定义 httpHosts
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))

    // 4. 自定义写入 es 的 EsSinkFunction
    val myEsSinkFunction  = new ElasticsearchSinkFunction[SensorReading] {
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        // 包装一个 Map 作为 data source
        val datasource = new util.HashMap[String, String]()
        datasource.put("id", t.id)
        datasource.put("temperature", t.temperature.toString)
        datasource.put("ts", t.timestamp.toString)

        // 创建 index request, 用于发送 http 请求
        val indexRequest = Requests.indexRequest()
          .index("sensor")
          .`type`("readingdata")
          .source(datasource)

        // 用indexer发送请求
        requestIndexer.add(indexRequest)
      }
    }
    dataStream.addSink(new ElasticsearchSink.Builder[SensorReading](httpHosts, myEsSinkFunction).build())

    env.execute(this.getClass.getName)
  }
}
