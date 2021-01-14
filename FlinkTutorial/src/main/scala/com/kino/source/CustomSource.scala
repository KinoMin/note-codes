package com.kino.source

import com.kino.mode.SensorReading
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

/**
 * create by kino on 2021/1/14
 * 自定义source
 */
object CustomSource {
  class MySensorSource extends SourceFunction[SensorReading]{
    var running = true
    override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
      //初始化一个随机数发生器
      val random = new Random
      var curTemp = 1.to(10).map(
        i => ("sensor_" + i,65+random.nextGaussian()*20)
      )
      while (running){
        //更新温度值
        curTemp = curTemp.map(
          t => (t._1, t._2 + random.nextGaussian())
        )
        //获取当前时间戳
        val curTime = System.currentTimeMillis()
        curTemp.foreach(
          t => sourceContext.collect(SensorReading(t._1, curTime, t._2))
        )
        Thread.sleep(100)
      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.addSource(new MySensorSource())
    stream.print()
    env.execute(this.getClass.getName)
  }
}
