package com.kino.transform

import org.apache.flink.streaming.api.scala._
/**
 * Created by kino on 2020/12/10.
 */
object FlinkFunction {
    def main(args: Array[String]): Unit = {
        flinkAggregation
    }

    /**
     * map function
     */
    def flinkMap(): Unit ={
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val inputDS = env.socketTextStream("localhost", 8888)
        val mapDS = inputDS.map((_, 1))
        mapDS.print().setParallelism(5)
        env.execute(this.getClass.getName)
    }
    
    def flinkFlatMap(): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val inputDS = env.socketTextStream("localhost", 8888)
        val flatMapDS = inputDS.flatMap((_.split(" ")))
        flatMapDS.print()
        env.execute(this.getClass.getName)
    }
    
    def flinkFilter(): Unit ={
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val inputDS = env.socketTextStream("localhost", 8888)
        val filterDS = inputDS.filter((_.contains("a")))
        filterDS.print()
        env.execute(this.getClass.getName)
    }
    
    def flinkKeyBy: Unit ={
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val inputDS = env.socketTextStream("localhost", 8888)
        val keyByDS = inputDS.map((_, 1)).keyBy(0)
        keyByDS.print()
        env.execute(this.getClass.getName)
    }

    /**
     * 滚动聚合算子
     *      sum
     *      min
     *      max
     *      minBy
     *      maxBy
     */
    def flinkAggregation(): Unit ={
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val inputDS = env.socketTextStream("localhost", 8888)
        val keyByDS = inputDS.map((_, 1)).keyBy(0).maxBy(1)
        keyByDS.print()
        env.execute(this.getClass.getName)
    }
    
}
