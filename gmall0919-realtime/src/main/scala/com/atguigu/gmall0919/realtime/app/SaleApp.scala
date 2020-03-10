package com.atguigu.gmall0919.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall0919.common.constant.GmallConstant
import com.atguigu.gmall0919.realtime.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.atguigu.gmall0919.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object SaleApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sale_app")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val inputOrderDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER,ssc)
    val inputOrderDetailDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER_DETAIL,ssc)

    //把订单和订单明细 转换为 case class的流
    val orderInfoDstream: DStream[OrderInfo] = inputOrderDstream.map { record =>
      val jsonString: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      val telTurple: (String, String) = orderInfo.consignee_tel.splitAt(3)
      val telLast4 = telTurple._2.splitAt(4)._2
      val newTel: String = telTurple._1 + "****" + telLast4
      orderInfo.consignee_tel = newTel

      val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = createTimeArr(0)
      orderInfo.create_hour = createTimeArr(1).split(":")(0)

      orderInfo
    }

    val orderDetailDstream: DStream[OrderDetail] = inputOrderDetailDstream.map( record=>  JSON.parseObject( record.value,classOf[OrderDetail]))

    // orderinfo 和 orderDetail 的双流join
    val orderInfoWithKeyDstream: DStream[(String, OrderInfo)] = orderInfoDstream.map(orderInfo=>(orderInfo.id,orderInfo))
    val orderDetailWithKeyDstream: DStream[(String, OrderDetail)] = orderDetailDstream.map(orderDetail=>(orderDetail.order_id,orderDetail))

    val orderJoinedDstream: DStream[(String, (OrderInfo, OrderDetail))] = orderInfoWithKeyDstream.join(orderDetailWithKeyDstream)


    val fulljoinedDstream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithKeyDstream.fullOuterJoin(orderDetailWithKeyDstream)
    // 三种情况  1 Some Some 关联上      2 Some  None    ,   3 None Some
    val saleDstream: DStream[SaleDetail] = fulljoinedDstream.flatMap { case (orderId, (orderInfoOption, orderDetailOption)) =>
      val jedis: Jedis = RedisUtil.getJedisClient
      val saleDetailList = new ListBuffer[SaleDetail]()
      if (orderInfoOption != None) { //1 主表明细表都不为NONE
        val orderInfo: OrderInfo = orderInfoOption.get
        if (orderDetailOption != None) { //1.1 主表明细表都不为NONE 则直接组合出结果
          val orderDetail: OrderDetail = orderDetailOption.get
          val saleDetail = new SaleDetail(orderInfo, orderDetail)
          saleDetailList += saleDetail;
        }
        //1.2  主表写入redis中
        //redis   type ?  string         key ?   order_info:[order_id]   value ? order_info_json
        val orderInfoKey = "order_info:" + orderInfo.id
        val orderInfoJson: String = JSON.toJSONString(orderInfo, new SerializeConfig(true))
        jedis.setex(orderInfoKey, 3600, orderInfoJson)
        // 1.3 主表查询redis中的从表信息
        val orderDetailKey = "order_detail:" + orderInfo.id
        val orderDetailSet: util.Set[String] = jedis.smembers(orderDetailKey)
        if (orderDetailSet != null && orderDetailSet.size() > 0) {
          import collection.JavaConversions._
          for (orderDetailJson <- orderDetailSet) {
            val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailList += saleDetail
          }
        }

      } else { //2 主表为空 从表不为空
        val orderDetail: OrderDetail = orderDetailOption.get
        // 2.1把自己写入redis
        // type ?    set      key ?  order_detail:[order_id]    value? order_detail_jsons .....
        val orderDetailKey = "order_detail:" + orderDetail.order_id
        val orderDetailJson: String = JSON.toJSONString(orderDetail)
        jedis.sadd(orderDetailKey, orderDetailJson)
        jedis.expire(orderDetailKey, 3600)
        // 2.2 查询redis中的主表信息
        val orderInfoKey = "order_info:" + orderDetail.order_id
        val orderInfoJson: String = jedis.get(orderInfoKey)
        if (orderInfoJson != null && orderInfoJson.size > 0) {
          val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
          val saleDetail = new SaleDetail(orderInfo, orderDetail)
          saleDetailList += saleDetail
        }

      }


      saleDetailList
    }
    saleDstream


    orderJoinedDstream.print(100)

    ssc.start()
    ssc.awaitTermination()

  }



}
