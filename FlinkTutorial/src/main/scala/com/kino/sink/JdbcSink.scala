package com.kino.sink

import com.kino.mode.SensorReading
import com.kino.source.CustomSource.MySensorSource
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * create by kino on 2021/1/15
 */
object JdbcSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream: DataStream[SensorReading] = env.addSource(new MySensorSource())
    inputStream.addSink(new MyJdbcSinkFunction())
    env.execute(this.getClass.getName)
  }
}

class MyJdbcSinkFunction() extends RichSinkFunction[SensorReading]{
  // 定义连接、预编译语句
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit ={
    conn = DriverManager.getConnection("jdbc:mysql://192.168.1.140:3307/test", "dmp", "Ioubuy123")
    insertStmt = conn.prepareStatement("insert into sensor_temp (id, temp) values (?, ?)")
    updateStmt = conn.prepareStatement("update sensor_temp set temp = ? where id = ?")
  }

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    // 先执行更新操作，查到就更新
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    // 如果更新没有查到数据，那么就插入
    if( updateStmt.getUpdateCount == 0 ){
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}