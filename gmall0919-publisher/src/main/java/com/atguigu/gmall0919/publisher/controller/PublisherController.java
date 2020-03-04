package com.atguigu.gmall0919.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0919.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import sun.java2d.pipe.SpanShapeRenderer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public  String  getTotal(@RequestParam("date") String date ){
        Long dauTotal = publisherService.getDauTotal(date);
        List<Map>  totalList= new  ArrayList();
        Map dauMap=new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);
        totalList.add(dauMap);

        Map newMidMap=new HashMap();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",233);
        totalList.add(newMidMap);

        String jsonString = JSON.toJSONString(totalList);
        return  jsonString;
    }

    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id ,@RequestParam("date")String date){
        Map<String,Map> hourMap= new HashMap<>();
        if("dau".equals(id)){
            Map  todayDauHourMap=    publisherService.getDauHourMap(date);
            String yd = getYd(date);
            Map  yesterdayDauHourMap=    publisherService.getDauHourMap(yd);
            hourMap.put("today",todayDauHourMap);
            hourMap.put("yesterday",yesterdayDauHourMap);
            return  JSON.toJSONString(hourMap);
        }


        return   null;
    }

    private String getYd(String td){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        try {
            Date tdDate = simpleDateFormat.parse(td);
            Date ydDate = DateUtils.addDays(tdDate, -1);
            String yd = simpleDateFormat.format(ydDate);
            return yd;
        } catch (ParseException e) {
                throw  new RuntimeException("日期格式不正确");

        }
    }

}
