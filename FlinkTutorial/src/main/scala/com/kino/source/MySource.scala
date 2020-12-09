package com.kino.source

import scala.util.Random
import com.kino.mode.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
/**
 * Create kino by 2020/12/9
 */
object MySource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val value = env.addSource(new MySensorSource())
    value.print()
    env.execute(this.getClass.getName)
  }
  
  class MySensorSource extends SourceFunction[SensorReading] {
    var running: Boolean = true
    override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
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
        val curTime: Long = System.currentTimeMillis()
        curTemp.foreach(
          t => ctx.collect(SensorReading(t._1, curTime, t._2))
        )
        Thread.sleep(100)
      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }
}
