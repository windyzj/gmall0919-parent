package com.atguigu.gmall0919.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0919.common.constant.GmallConstant
import com.atguigu.gmall0919.realtime.bean.{OrderInfo, UserState}
import com.atguigu.gmall0919.realtime.util.{MyKafkaUtil, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object OrderApp {


  def main(args: Array[String]): Unit = {
     val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("order_app")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER,ssc)

    val orderInfoDstream: DStream[OrderInfo] = inputDstream.map { record =>
      val jsonString: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      val telTurple: (String, String) = orderInfo.consignee_tel.splitAt(3)
      val telLast4 = telTurple._2.splitAt(4)._2
      val newTel: String = telTurple._1 + "****" + telLast4
      orderInfo.consignee_tel = newTel

      val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = createTimeArr(0)
      orderInfo.create_hour = createTimeArr(1).split(":")(0)

      // 给orderInfo 增加一个字段    用来标识该笔订单是否是用户首次下单   ==> 还要知道用户是否下过单（redis/mysql/hbase）
      // 查询phoenix 中的这个状态表  判断凡是下过单的用户     该笔订单的首单标志设置为1  否则为0

      //最后保存到phoenix中的订单表 数据量是不会少  只是标志有所区分

      // 你要维护一个表 phoenix   保存了 用户id 和 是否是过单的用户的标志

      orderInfo
    }
    //不理想 ，会反复访问数据库
    orderInfoDstream.map{orderInfo=>
      val user_id: String = orderInfo.user_id
      PhoenixUtil.queryList("select user_id,if_ordered from user_state0919 where user_id='"+user_id+"'")
    }

    //优化后 以分区为单位批量访问数据   根据返回的结果进行 首单标识
    val orderWithIfFirstDstream: DStream[OrderInfo] = orderInfoDstream.mapPartitions { orderInfoItr =>
      val orderInfoList: List[OrderInfo] = orderInfoItr.toList
      if (orderInfoList.size > 0) {
        //针对分区中的订单中的所有客户 进行批量查询
        val userIds: String = orderInfoList.map("'" + _.user_id + "'").mkString(",")
        val userStateList: List[util.Map[String, Any]] = PhoenixUtil.queryList("select user_id,if_ordered from GMALL0919_USER_STATE where user_id in (" + userIds + ")")
        // [{userId:123, if_ordered:1 },{userId:2334, if_ordered:1 },{userId:4355, if_ordered:1 }]
        // 进行转换 把List[Map] 变成Map
        val userIfOrderedMap: Map[String, String] = userStateList.map(userStateDBMap => (userStateDBMap.get("USER_ID").asInstanceOf[String], userStateDBMap.get("IF_ORDERED").asInstanceOf[String])).toMap

        //进行判断 ，打首单表情
        for (orderInfo <- orderInfoList) {
          val ifOrderedUser: String = userIfOrderedMap.getOrElse(orderInfo.user_id, "0")
          //是下单用户不是首单   否->首单
          if (ifOrderedUser == "1") {
            orderInfo.if_first_order = "0"
          } else {
            orderInfo.if_first_order = "1"
          }
        }
        orderInfoList.toIterator
      } else {
        orderInfoItr
      }

    }
    // 以userId 进行分组
    val groupByUserDstream: DStream[(String, Iterable[OrderInfo])] = orderWithIfFirstDstream.map(orderInfo=>(orderInfo.user_id,orderInfo)).groupByKey()

    val orderInfoFinalDstream: DStream[OrderInfo] = groupByUserDstream.flatMap { case (userId, orderInfoItr) =>
      val orderList: List[OrderInfo] = orderInfoItr.toList
      //
      if (orderList.size > 1) { //   如果在这个批次中这个用户有多笔订单
        val sortedOrderList: List[OrderInfo] = orderList.sortWith((orderInfo1, orderInfo2) => orderInfo1.create_time < orderInfo2.create_time)
        if (sortedOrderList(0).if_first_order == "1") { //排序后，如果第一笔订单是首单，那么其他的订单都取消首单标志
          for (i <- 1 to sortedOrderList.size - 1) {
            sortedOrderList(i).if_first_order = "0"
          }

        }
        sortedOrderList
      } else {
        orderList
      }
    }

    orderInfoFinalDstream.cache()

    orderInfoFinalDstream.foreachRDD{rdd=>
      rdd.saveToPhoenix("GMALL0919_ORDER_INFO",Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR","IF_FIRST_ORDER"),
        new Configuration(),Some("hadoop1,hadoop2,hadoop3:2181"))

    }
    //同步到userState表中  只有标记了 是首单的用户 才需要同步到用户状态表中
    val userStateDstream: DStream[UserState] = orderInfoFinalDstream.filter(_.if_first_order=="1").map(orderInfo=>UserState(orderInfo.id,orderInfo.if_first_order))
    userStateDstream.foreachRDD{rdd=>
      rdd.saveToPhoenix("GMALL0919_USER_STATE",Seq("USER_ID","IF_ORDERED"),new Configuration(),Some("hadoop1,hadoop2,hadoop3:2181"))
    }

    ssc.start()
    ssc.awaitTermination()


  }

}
