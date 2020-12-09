package com.kino.source

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import java.sql.{Connection, PreparedStatement}
import org.apache.flink.configuration.Configuration
import java.sql.DriverManager
import java.sql.ResultSet
/**
 * mysql source
 *    <dependency>
 *       <groupId>mysql</groupId>
 *       <artifactId>mysql-connector-java</artifactId>
 *       <version>8.0.15</version>
 *    </dependency>
 * Created by kino on 2020/12/10.
 */
object MySQLSource {


    def main(args: Array[String]): Unit = {
        /**
         * 创建 mysql 表
         *      CREATE TABLE `test-sqlite-jdbc-person` (
         *      `pid` int(11) NOT NULL,
         *      `firstname` text,
         *      `age` int(11) DEFAULT NULL
         *      ) ENGINE=InnoDB DEFAULT CHARSET=latin
         */

        /**
         * 创建对应的样例类
         */
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val mysqlStream = env.addSource(new MySQLSource)
        mysqlStream.print
        env.execute(this.getClass.getName)
    }
    
    class MySQLSource extends RichSourceFunction[person] {
        var running: Boolean = true
        var ps: PreparedStatement = null
        var conn: Connection = null

        /**
         * 与MySQL建立连接信息
         * @return
         */
        def getConnection():Connection = {
            var conn: Connection = null
            val DB_URL: String = "jdbc:mysql://192.168.1.146:3308/kinodb1"
            val USER: String = "root"
            val PASS: String = "apioak"

            try{
                Class.forName("com.mysql.cj.jdbc.Driver")
                conn = DriverManager.getConnection(DB_URL, USER, PASS)
            } catch {
                case _: Throwable => println("due to the connect error then exit!")
            }
            conn
        }

        /**
         * open()方法初始化连接信息
         * @param parameters
         */
        override def open(parameters: Configuration): Unit = {
            super.open(parameters)
            conn = this.getConnection()
            val sql = "select * from `test-sqlite-jdbc-person`"
            ps = this.conn.prepareStatement(sql)
        }


        override def run(ctx: SourceFunction.SourceContext[person]): Unit = {
            val resSet:ResultSet = ps.executeQuery()
            while(running & resSet.next()) {
                ctx.collect(person(
                    resSet.getInt("pid"),
                    resSet.getString("firstname"),
                    resSet.getInt("age")
                ))
            }
        }

        override def cancel(): Unit = {
            running = false
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
    }
}

case class person(pid: Int, firstname: String, age: Int)