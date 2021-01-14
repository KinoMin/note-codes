package com.kino.source

import com.kino.mode.Person
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}


/**
 * create by kino on 2021/1/14
 * 自定义source
 */
object CustomMySQLSource {
  class MySqlSource extends RichSourceFunction[Person]{

    var running: Boolean = true
    var ps: PreparedStatement = null
    var conn: Connection = null

    def getConnection() = {
      var conn: Connection = null
      val DB_URL: String = "jdbc:mysql://192.168.1.140:3307/test"
      val USER_NAME: String = "dmp"
      val PASS_WORD: String = "Ioubuy123"
      try{
        Class.forName("com.mysql.cj.jdbc.Driver")
        conn = DriverManager.getConnection(DB_URL, USER_NAME, PASS_WORD)
      } catch {
        case _: Throwable => println("due to the connect error then exit!")
      }
      conn
    }

    /**
     * open()方法初始化连接信息
     * @param parameters
     */
    override def open(parameters: Configuration) = {
      super.open(parameters)
      conn = this.getConnection()
      val sql = "select * from test.person"
      ps = this.conn.prepareStatement(sql)
    }

    /**
     * 关闭连接信息
     */
    override def close(): Unit = {
      if(conn != null) {
        conn.close()

      }
      if(ps != null) {
        ps.close()
      }
    }

    override def run(sourceContext: SourceFunction.SourceContext[Person]): Unit = {
      val resSet:ResultSet = ps.executeQuery()
      while(running & resSet.next()) {
        sourceContext.collect(Person(
          resSet.getInt("id"),
          resSet.getString("name"),
          resSet.getInt("age"),
          resSet.getInt("sex"),
          resSet.getString("email")
        ))
      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val mysqlStream = env.addSource(new MySqlSource)
    mysqlStream.print
    env.execute(this.getClass.getName)
  }
}
