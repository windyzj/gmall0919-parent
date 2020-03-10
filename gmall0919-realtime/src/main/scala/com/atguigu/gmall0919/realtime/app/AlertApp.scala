package com.atguigu.gmall0919.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0919.common.constant.GmallConstant
import com.atguigu.gmall0919.realtime.bean.{AlertInfo, EventInfo}
import com.atguigu.gmall0919.realtime.util.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object AlertApp {

  def main(args: Array[String]): Unit = {
      val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("alert_app")
       val ssc = new StreamingContext(sparkConf,Seconds(5))

      val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_EVENT,ssc)

    val eventInfoDstream: DStream[EventInfo] = inputDstream.map { record =>
      val jsonString: String = record.value()
      val eventInfo: EventInfo = JSON.parseObject(jsonString, classOf[EventInfo])
      eventInfo
    }

//    1 同一设备  -->按设备分组
//    2 5分钟内   --> window
//    3 三次及以上用不同账号登录并领取优惠劵    map
//    4 并且在登录到领劵过程中没有浏览商品    map
//    5 整理成给定格式日志             map
//    6 同一设备 每分钟 只记录一次  （去重）
//    7 保存日志  写入es中

    //    5分钟内   --> window
    //窗口大小：数据范围  滑动步长：出数频率
    val eventInfoWindowDstream: DStream[EventInfo] = eventInfoDstream.window(Seconds(300), Seconds(5))

    val groupbyMidDstream: DStream[(String, Iterable[EventInfo])] = eventInfoWindowDstream.map(eventInfo=>(eventInfo.mid,eventInfo)).groupByKey()


    val alterInfoWithIfDstream: DStream[(Boolean, AlertInfo)] = groupbyMidDstream.map { case (mid, eventInfoItr) =>
      //    3 三次及以上用不同账号登录并领取优惠劵    map
      //    4 并且在登录到领劵过程中没有浏览商品    map
      //    5 整理成给定格式日志             map
      var ifAlert = false
      var ifClickItem = false  //看是否在过程中有浏览商品
      val uidSet = new util.HashSet[String]() //EX ?  Driver?  =>>EX!!!
      val itemSet = new util.HashSet[String]()
      val eventList = new util.ArrayList[String]()
      import scala.util.control.Breaks._
      breakable(
        for (eventInfo: EventInfo <- eventInfoItr) {
          eventList.add(eventInfo.evid)
          if (eventInfo.evid == "coupon") {
            uidSet.add(eventInfo.uid)
            itemSet.add(eventInfo.itemid)
          }
          if (eventInfo.evid == "clickItem") {
            ifClickItem = true
            break
          }
        }
      )
      ifAlert = uidSet.size() >= 3 && !ifClickItem
      (ifAlert, AlertInfo(mid, uidSet, itemSet, eventList, System.currentTimeMillis()))
    }
   // alterInfoWithIfDstream.print(100)
    val alertInfoDstream: DStream[AlertInfo] = alterInfoWithIfDstream.filter(_._1).map(_._2)

    //6 同一设备 每分钟 只记录一次  （去重）
     // 利用目标数据库的幂等性进行去重 put upsert  // 要注意业务上的细节 put upsert 后面会覆盖前
     // 如何设计主键？ mid+分钟数
    val alertInfoWithIdDstream: DStream[(String, AlertInfo)] = alertInfoDstream.map(alertInfo=>(alertInfo.mid+"_"+System.currentTimeMillis()/1000/60 , alertInfo ))
    alertInfoWithIdDstream.foreachRDD{rdd=>
      rdd.foreachPartition(alertItr=>{
        val alertList: List[(String, AlertInfo)] = alertItr.toList
        val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date)
        MyEsUtil.bulkInsert(alertList,GmallConstant.ES_INDEX_ALERT+"_"+dateStr)
      }
      )
    }
    
    

    ssc.start()
    ssc.awaitTermination()






  }

}
