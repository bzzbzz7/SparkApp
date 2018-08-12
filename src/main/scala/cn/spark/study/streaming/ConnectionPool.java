package cn.spark.study.streaming;

import com.mysql.jdbc.Connection;

import java.util.LinkedList;

public class ConnectionPool {
    private static LinkedList<Connection> connections;
}
