package com.kino.source

import org.apache.flink.streaming.api.scala._

/**
 * create by kino on 2021/1/14
 * 文件 source
 */
object FileSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.readTextFile("D:\\apache-atlas-2.1.0-sources\\apache-atlas-sources-2.1.0\\README.txt")
    stream.print()
    env.execute(this.getClass.getName)
  }
}
