package com.kino.dmp

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties


/**
 * 测试 spark 读取 mysql、pgsql、sqlserver数据到sparksql 的数据类型
 * create by kino on 2021/1/11
 */
object test {
  def main(args: Array[String]): Unit = {
    val s = "select id, src_datasource_id, target_datasource_id, src_table_name, target_table_name, type, connector_job_id, connector_json_data, src_topic_name, project_id, status, create_time, update_time, connector_url, parent_id, source_type_name, target_type_name, created_by, updated_by, desensitization_field, arithmetic, pk_name, src_database_type, src_database_name, target_database_type, target_database_name, src_datasource_name, target_datasource_name, store_type from dmp_web.dmp_realtime_sync_info) as t"
    val i = s.indexOf("from")
    val i1 = s.lastIndexOf(")")
    println(s.substring(i+5, i1))
  }
}
