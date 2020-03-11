package com.atguigu.gmall0919.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0919.publisher.bean.Option;
import com.atguigu.gmall0919.publisher.bean.Stat;
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

        Double orderAmountTotal = publisherService.getOrderAmountTotal(date);
        Map orderAmountMap=new HashMap();
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易额");
        orderAmountMap.put("value",orderAmountTotal!=null?orderAmountTotal:0.0D);
        totalList.add(orderAmountMap);

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
        }else if ("order_amount".equals(id)){
            Map  todayOrderAmountHourMap=    publisherService.getOrderAmountHour(date);
            String yd = getYd(date);
            Map  yesterdayOrderAmountHourMap=    publisherService.getOrderAmountHour(yd);
            hourMap.put("today",todayOrderAmountHourMap);
            hourMap.put("yesterday",yesterdayOrderAmountHourMap);
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

    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date ,@RequestParam("startpage") int startpage ,@RequestParam("size") int size ,@RequestParam("keyword") String keyword  ){

       Map saleDetailMap=  publisherService.getSaleDetail(date,keyword,startpage,size);
        Long total =(Long)saleDetailMap.get("total");
        List<Map> detailList =(List<Map>)saleDetailMap.get("detail");
        Map ageAgg =(Map) saleDetailMap.get("ageAgg");

        Long age20lt=0L;
        Long age20gte30lt=0L;
        Long age30gte=0L;
        for (Object o : ageAgg.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            String ageStr =(String) entry.getKey();
            Long ageCount =(Long) entry.getValue();
            Integer age = Integer.valueOf(ageStr);
            if (age<20){
                age20lt+=ageCount;
            }else if(age>=20&&age<30){
                age20gte30lt+=ageCount;
            }else{
                age30gte+=ageCount;
            }

        }
        Double age20ltRatio= Math.round( age20lt*1000D/total)/10D;
        Double age20gte30ltRatio=Math.round( age20gte30lt*1000D/total)/10D;;
        Double age30gteRatio=Math.round( age30gte*1000D/total)/10D;;


        Map  resultMap=new HashMap();

        List ageOptions=new ArrayList();
        ageOptions.add(new Option("20岁以下",age20ltRatio));
        ageOptions.add(new Option("20岁到30岁",age20gte30ltRatio));
        ageOptions.add(new Option("30岁及以上",age30gteRatio));

        List genderOptions=new ArrayList();
        genderOptions.add(new Option("男",27.5D));
        genderOptions.add(new Option("女",72.5D));

        List<Stat> statList=new ArrayList<>();
        statList.add(new Stat("年龄占比",ageOptions));
        statList.add(new Stat("性别占比",genderOptions));

        resultMap.put("stat",statList);
        resultMap.put("total",total);
        resultMap.put("detail",detailList);

        return JSON.toJSONString(resultMap);
    }

}
