package com.atguigu.gmall0919.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0919.common.constant.GmallConstant
import com.atguigu.gmall0919.realtime.bean.OrderInfo
import com.atguigu.gmall0919.realtime.util.MyKafkaUtil
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
    orderInfoDstream


    orderInfoDstream.foreachRDD{rdd=>
      rdd.saveToPhoenix("GMALL0919_ORDER_INFO",Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        new Configuration(),Some("hadoop1,hadoop2,hadoop3:2181"))

    }
    ssc.start()
    ssc.awaitTermination()


  }

}
