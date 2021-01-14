package com.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class mysql {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection("jdbc:mysql://hadoop2:3306/dolphinscheduler?characterEncoding=UTF-8&allowMultiQueries=true", "dolphinscheduler", "Kino123.");
        System.out.println(conn);
    }
}
