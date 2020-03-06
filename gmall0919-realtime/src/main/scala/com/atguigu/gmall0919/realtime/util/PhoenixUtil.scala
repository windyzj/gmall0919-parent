package com.atguigu.gmall0919.realtime.util

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}
import java.util
import java.util.Map

import scala.collection.mutable.ListBuffer




object  PhoenixUtil {
  @throws[Exception]
  def main(args: Array[String]): Unit = {

    val list:  List[ Map[String, Any]] = queryList("select * from  CUSTOMER0919")
      println(list)
     // Thread.sleep(1000)


  }


  def   queryList(sql:String):List[Map[String,Any]]={
         Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
        val resultList: ListBuffer[Map[String,Any]] = new  ListBuffer[ Map[String,Any]]()
        val conn: Connection = DriverManager.getConnection("jdbc:phoenix:hadoop1,hadoop2,hadoop3:2181")
        val stat: Statement = conn.createStatement
        println(sql)
        val rs: ResultSet = stat.executeQuery(sql )
        val md: ResultSetMetaData = rs.getMetaData
        while (  rs.next ) {
          val rowData:  Map[String,Any]=new  util.HashMap[String ,Any]()
          for (i  <-1 to md.getColumnCount  ) {
              rowData.put(md.getColumnName(i), rs.getObject(i))
          }
          resultList+=rowData
        }

        stat.close()
        conn.close()
        resultList.toList

  }




}