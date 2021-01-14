package com.kino.wc

import org.apache.flink.api.scala._


/**
 * 批处理 wordcount
 * Create kino By 2020/12/09
 */
object BatchWordCount {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 从文件中读取数据
    val path = "/opt/flink-file/hello.txt"

    val inputDs = env.readTextFile(path)

    // 分词之后，对单词进行 groupBy 分组，然后用 sum 进行聚合
    val wordCountDs = inputDs.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    wordCountDs.print()
  }
}
