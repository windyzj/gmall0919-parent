package com.atguigu.gmall0919.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;

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
                 for (CanalEntry.RowData rowData : rowDatasList) {  //遍历行集
                     List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                     for (CanalEntry.Column column : afterColumnsList) {  //遍历列集
                         System.out.println(column.getName()+"::==>"+column.getValue());
                     }
                 }
             }
        }
    }


}
