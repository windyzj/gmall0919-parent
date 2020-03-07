package com.atguigu.gmall0919.publisher.service.impl;

import com.atguigu.gmall0919.publisher.mapper.DauMapper;
import com.atguigu.gmall0919.publisher.mapper.OrderMapper;
import com.atguigu.gmall0919.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl  implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Override
    public Long getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauHourMap(String date) {
        List<Map> mapList = dauMapper.selectDauHourCount(date);  //[{LOGHOUR:16, CT:399},{LOGHOUR:13, CT:19},{LOGHOUR:17, ,CT:343}]
        //-->  {"16":399,"13":19, .....}
        Map hourMap=new HashMap();
        for (Map map : mapList) {
            hourMap.put(map.get("LOGHOUR"), map.get("CT"));

        }
        return hourMap;
    }

    @Override
    public Double getOrderAmountTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        List<Map> orderAmountHourList = orderMapper.selectOrderAmountHour(date);
        Map orderAmountHourMap= new HashMap();
        for (Map map : orderAmountHourList) {
            orderAmountHourMap.put(map.get("CREATE_HOUR"),map.get("TOTAL_AMOUNT"));
        }
        return orderAmountHourMap;
    }
}
