package com.atguigu.gmall0919.realtime.app

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0919.common.constant.GmallConstant
import com.atguigu.gmall0919.realtime.bean.StartUpLog
import com.atguigu.gmall0919.realtime.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._

object DauApp {

  def main(args: Array[String]): Unit = {
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("dau_app")
      val ssc = new StreamingContext(sparkConf,Seconds(5))

       //启动日志 --》日活
      val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP,ssc)

    // inputDstream.map(  _.value()).print(100)
    //1 把字符串转化为内存对象  补充一下时间字段
    val startUpLogDstream: DStream[StartUpLog] = inputDstream.map { record =>
      val jsonString: String = record.value()
      val startUpLog: StartUpLog = JSON.parseObject(jsonString, classOf[StartUpLog])

      val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
      val datetimeString: String = simpleDateFormat.format(new Date(startUpLog.ts))
      val datetimeArr: Array[String] = datetimeString.split(" ")
      startUpLog.logDate = datetimeArr(0)
      startUpLog.logHour = datetimeArr(1)
      startUpLog
    }

    //  1  去重操作

      // 利用这个mid清单进行过滤  凡是已经出现过的mid 一律筛除掉
   /* val jedis: Jedis = new Jedis("hadoop1", 6379)  //driver 只执行一次
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val todayString: String = simpleDateFormat.format(new Date())
    val dauKey="dau:"+todayString
    val dauSet: util.Set[String] = jedis.smembers(dauKey)
    jedis.close()
    val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)*/

     val filteredDstream: DStream[StartUpLog] = startUpLogDstream.transform { rdd =>
      ///...driver 按周期执行  查询redis中的当日用户访问清单
      val jedis: Jedis = new Jedis("hadoop1", 6379) //driver 只执行一次
      val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val todayString: String = simpleDateFormat.format(new Date())
      val dauKey = "dau:" + todayString
      val dauSet: util.Set[String] = jedis.smembers(dauKey)
      jedis.close()
      val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
      ///
      println("过滤前：" + rdd.count())
      val filteredRDD: RDD[StartUpLog] = rdd.filter { startuplog =>
        ///校验 ex中批次内的数据是否有跟广播变量中的清单重复
        val dauSet: util.Set[String] = dauBC.value
        //ex
        if (dauSet != null && dauSet.size() > 0 && dauSet.contains(startuplog.mid)) {
           false
        } else {
            true
        }

      }
      println("过滤后：" + filteredRDD.count())
      filteredRDD
    }

    // 本批次去重 ，根据mid进行分组  组内根据时间戳排序 取 top1
    val groupbyMidDstream: DStream[(String, Iterable[StartUpLog])] = filteredDstream.map(startuplog=>(startuplog.mid,startuplog)).groupByKey()
    val finalFilteredDstream: DStream[StartUpLog] = groupbyMidDstream.flatMap { case (mid, startuplogItr) =>
      if (startuplogItr.size == 1) {
        startuplogItr.take(1)
      } else {
        startuplogItr.toList.sortWith((startuplog1, startuplog2) => startuplog1.ts < startuplog2.ts).take(1)
      }
    }





//    val filteredDstream: DStream[StartUpLog] = startUpLogDstream.filter { startuplog =>
//
//      val dauSet: util.Set[String] = dauBC.value
//       //ex
//      if(dauSet!=null&& dauSet.size()>0 &&  dauSet.contains(startuplog.mid)){
//          return false
//      }else {
//          return true
//      }
//
//
//    }


    filteredDstream.cache()
    //  要把今天访问过的用户mid记录下来  保存到redis
    finalFilteredDstream.foreachRDD{ rdd=>
    // driver
      rdd.foreachPartition { startuplogItr =>
        val jedis: Jedis = new Jedis("hadoop1", 6379)
        for ( startuplog<- startuplogItr ) { //今天访问过的mid清单
           // redis     type?    set      key ?    "dau:2020-03-02"   value  mid
            println(startuplog)

           val dauKey="dau:"+startuplog.logDate
           jedis.sadd(dauKey,startuplog.mid)
           jedis.expire(dauKey,24*3600)
        }
        jedis.close()

      }

    }
    //最后的筛选结果 保存起来 到分析数据库

    finalFilteredDstream.foreachRDD{rdd=>
      rdd.saveToPhoenix("GMALL0919_DAU",Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        new Configuration(),Some("hadoop1,hadoop2,hadoop3:2181"))

    }



    ssc.start()
    ssc.awaitTermination()

  }


  //  转换 ->转换 -> .. -> 行动
}
