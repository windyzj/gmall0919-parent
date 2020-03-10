package com.atguigu.gmall0919.realtime.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

object MyEsUtil {

  private val ES_HOST = "http://hadoop1"
  private val ES_HTTP_PORT = 9200
  private var factory:JestClientFactory = null

  /**
    * 获取客户端
    *
    * @return jestclient
    */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject

  }

  /**
    * 关闭客户端
    */
  def close(client: JestClient): Unit = {
    if ( client!=null) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
    * 建立连接
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)

  }

  def main(args: Array[String]): Unit = {
    //单条提交保存
/*    val jest: JestClient = getClient
    val index: Index = new Index.Builder(Customer("1213","zhang3feng")).index("customer0919").`type`("customer").id("999").build()
    jest.execute(index)
    close(jest)*/
    //批量提交保存
    bulkInsert(List(("777",Customer("1214","qiuchuji"))
                   ,("888",Customer("1215","huangyaoshi"))),"customer0919")
  }

  // batch  bulk
  def bulkInsert(list:List[(String,AnyRef)],indexName:String): Unit ={
    val jest: JestClient = getClient
    val bulkBuilder = new Bulk.Builder
    bulkBuilder.defaultIndex(indexName).defaultType("_doc")
    for ((id,value) <- list ) {
      val index: Index = new Index.Builder(value).id(id).build()
      bulkBuilder.addAction(index)
    }
    val bulk: Bulk = bulkBuilder.build()
    val result: BulkResult = jest.execute(bulk)
    println("保存了"+result.getItems.size()+"条数据")
    close(jest)
  }


  case class Customer(id:String,name:String)

}
