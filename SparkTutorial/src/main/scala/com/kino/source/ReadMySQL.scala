package com.kino.source

import java.util.Properties
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
/**
 * Created by kino on 2020/12/10.
 */
object ReadMySQL {

    def main(args: Array[String]): Unit = {
//        import org.apache.spark.sql.DataFrame
//        val appNameSuffix = UUID.randomUUID()
//
//        val config = new SparkConf().setMaster("local[*]").setAppName("SparkReadMySQL_"+appNameSuffix)
//        val spark = SparkSession.builder.config(config).getOrCreate()
//
//        val jdbcUrl = "jdbc:mysql://192.168.1.146:3308/kinodb?useUnicode=true&characterEncoding=utf-8&useSSL=false"
//        
//        def dbConnProperties(user:String, pass:String):Properties = {
//            val ConnProperties = new Properties();
//            ConnProperties.put("driver", "com.mysql.jdbc.Driver");
//            ConnProperties.put("user", user);
//            ConnProperties.put("password", pass);
//            ConnProperties.put("fetchsize", "1000");  //读取条数限制
//            ConnProperties.put("batchsize", "10000"); //写入条数限制
//            return ConnProperties;
//        }
//        val dbUser = "root"
//        val dbPass = "apioak"
//
//        val readConnProperties = dbConnProperties(dbUser,dbPass);
//
//        val sql = "select * from person"
//
//        val df: DataFrame = spark.read.jdbc(jdbcUrl, s"(${sql}) t",readConnProperties)
//
//        // 显示前100行
//        df.show(100)
//
//
//        //合并为一个文件输出
//        //df.coalesce(1).write.format("csv").option("charset","utf-8").option("delimiter", ',').mode(SaveMode.Append).save(toPath)
//        
//
//        spark.stop()

        writeData
    }

    def writeData(): Unit ={

        val connectionProperties = new Properties

        val config = new SparkConf().setMaster("local[*]").setAppName("SparkReadMySQL_"+this.getClass.getName)
        val spark = SparkSession.builder.config(config).getOrCreate()
        
        //增加数据库的用户名(user)密码(password),指定postgresql驱动(driver)
        connectionProperties.put("user", "kong")
        connectionProperties.put("password", "Kong@2020")
        connectionProperties.put("driver", "org.postgresql.Driver")
        val jdbcDF = spark.read
                .jdbc("jdbc:postgresql://192.168.1.146:5432/kong", "test", connectionProperties)
//        jdbcDF.show()


        jdbcDF.createOrReplaceTempView("kino")
        
        spark.sql("select '111' as id, 'zhangsan' as name, cast('2020-12-30 17:13:21' as Timestamp) as time from kino").show()
        
//        jdbcDF.write
//                .format("jdbc")
//                .mode("overwrite")
//                .option("url", "jdbc:postgresql://192.168.1.146:5432/kinodb")
//                .option("dbtable", "test")
//                .option("user", "kong")
//                .option("password", "Kong@2020")
//                .save()
        
        

    }
}
