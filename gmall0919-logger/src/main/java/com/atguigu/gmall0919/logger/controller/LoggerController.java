package com.atguigu.gmall0919.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall0919.common.constant.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@RestController  //ResponseBody+Controller
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    //@RequestMapping(value = "log" ,method = RequestMethod.POST)
    @PostMapping("/log")
    public String dolog(@RequestParam("logString") String logString ){
        System.out.println(logString);

        //1 加时间戳
        //456456456
        //BBBBBBBBBBBBbbb
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts",System.currentTimeMillis());
        String jsonString = jsonObject.toJSONString();
        //落盘日志  log4j logback log4j2  loging   -->>  slf4j
        log.info(jsonString);
        // 发送kafka
        if(jsonObject.getString("type").equals("startup")){
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP,jsonString);
        }else{
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT,jsonString);
        }


        return  "success";

    }

}
