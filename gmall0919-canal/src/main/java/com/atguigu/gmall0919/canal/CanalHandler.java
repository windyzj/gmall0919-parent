package com.atguigu.gmall0919.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall0919.canal.util.MyKafkaSender;
import com.atguigu.gmall0919.common.constant.GmallConstant;

import java.util.List;

public class CanalHandler {

    List<CanalEntry.RowData> rowDatasList ;
    String tableName;
    CanalEntry.EventType eventType ;

    public CanalHandler(CanalEntry.EventType eventType, String tableName, List<CanalEntry.RowData> rowDataList) {
        this.eventType = eventType;
        this.tableName = tableName;
        this.rowDatasList = rowDataList;
    }

    public  void handle(){
        if(rowDatasList!=null &rowDatasList.size()>0){
             if (this.eventType.equals(CanalEntry.EventType.INSERT)&&this.tableName.equals("order_info")){    //下单
                 sendToKafka(GmallConstant.KAFKA_TOPIC_ORDER);
             }else if(this.eventType.equals(CanalEntry.EventType.INSERT)&&this.tableName.equals("order_detail")){//下单
                 sendToKafka(GmallConstant.KAFKA_TOPIC_ORDER_DETAIL);

             }else if ((this.eventType.equals(CanalEntry.EventType.INSERT)||this.eventType.equals(CanalEntry.EventType.UPDATE))&&this.tableName.equals("user_info")) {
                 sendToKafka(GmallConstant.KAFKA_TOPIC_USER);
             }
        }
    }

    public void sendToKafka(String topicName){
        for (CanalEntry.RowData rowData : rowDatasList) {  //遍历行集
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {  //遍历列集
                System.out.println(column.getName()+"::==>"+column.getValue());
                jsonObject.put(column.getName(),column.getValue());
            }
            try {
                Thread.sleep(1500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //发送kafka
            MyKafkaSender.send(topicName,jsonObject.toJSONString());
        }
    }


}
