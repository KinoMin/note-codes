package com.kino.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * 流处理 wordcount
 * Create kino by 2020/12/9
 */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //1. 创建 流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val parameterTool = ParameterTool.fromArgs(args)
    val host = parameterTool.get("host")

    //2. 接收 Socket 文本流
    val textDstream = env.socketTextStream(host, 8888)

    //3. flatMap 和 map 需要隐式转换, 我们在上面引包的时候写的是_, 所以此处省了
    val dataStream = textDstream.flatMap(_.split(" ")).filter(_.nonEmpty).map((_, 1)).keyBy(0).sum(1)

    //4. 打印输出
    dataStream.print()

    //5. 启动 executor, 执行任务
    env.execute("Socket stream word count")
  }
}
