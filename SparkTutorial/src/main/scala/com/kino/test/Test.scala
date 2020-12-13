package com.kino.test

/**
 * Created by kino on 2020/12/11.
 */
object Test {
    def main(args: Array[String]): Unit = {
        var sql = "create table if not exists dmp_project_role(id INTEGER not null comment '', project_id INTEGER null comment '', role_type VARCHAR(255) null comment '', role_name VARCHAR(255) null comment '', data_status VARCHAR(255) null comment '', create_user_id VARCHAR(255) null comment '', create_time TIMESTAMP null comment '', update_user_id VARCHAR(255) null comment '', update_time TIMESTAMP null comment '', etl_create_time varchar(30) comment 'etl_create_time', etl_update_time varchar(30) comment 'etl_update_time', primary key (id) )"
        val sqls = 
                    sql
                    .replaceAll("comment ", "")
                            .substring(sql.indexOf("(")+1)
                            .replace("' ,", "',")
                            .replaceAll("not ", "")
                            .replaceAll("null", "")
                            .split("',")
        
        var create_table_sql = ""
        for (i <- sqls) {
            val line = i.replaceAll("'", ",")
            create_table_sql += line.substring(0, line.indexOf(",")+1)
        }
        create_table_sql.substring(0, create_table_sql.lastIndexOf(",") - 1)
    }
}
