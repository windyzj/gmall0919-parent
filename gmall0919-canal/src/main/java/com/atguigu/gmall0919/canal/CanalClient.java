package com.atguigu.gmall0919.canal;


import com.alibaba.otter.canal.client.CanalConnector;
import  com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {


    public static void main(String[] args) {
        //连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop1", 11111), "example", "", "");
         while (true){  //周期性的连接 抓取canalserver的数据
             canalConnector.connect();
             canalConnector.subscribe("gmall0919.*");
             Message message = canalConnector.get(100);//100个以 sql 单位的数据  => entry  因为某一条sql 影响的row集合
             int size = message.getEntries().size();
             if(size==0){
                 try {
                     System.out.println("没有数据,休息5秒.........");
                     Thread.sleep(5000);
                 } catch (InterruptedException e) {
                     e.printStackTrace();
                 }
             }else{
                 // 检查筛选一下数据
                 List<CanalEntry.Entry> entries = message.getEntries();
                 for (CanalEntry.Entry entry : entries) {
                    if( entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)){  //只收取真正的行数据，其他不要
                        ByteString storeValue = entry.getStoreValue();
                        CanalEntry.RowChange rowChange=null;
                        try {
                            rowChange= CanalEntry.RowChange.parseFrom(storeValue);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                            new RuntimeException("反序列化失败");
                        }
                        if(rowChange!=null){
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                            String tableName=entry.getHeader().getTableName();
                            CanalEntry.EventType eventType = rowChange.getEventType();

                            CanalHandler canalHandler = new CanalHandler(eventType, tableName, rowDatasList);
                            canalHandler.handle();
                        }

                    }






                 }






             }



         }



    }

}
