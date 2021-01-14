package com.kino.source

import org.apache.spark.sql.SparkSession

object ReadHDFS {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
//      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val modelNames = Array("FM","FFM","DEEPFM","NFM","DIN","DIEN")
    val modelNamesRdd = spark.sparkContext.parallelize(modelNames,1)
    modelNamesRdd.saveAsTextFile("hdfs://hadoop1:9000/user/")

  }
}
