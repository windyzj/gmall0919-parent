package com.atguigu.gmall0919.publisher.service;

import java.util.List;
import java.util.Map;

public interface PublisherService {

    public Long getDauTotal(String date);

    public Map getDauHourMap(String date);

    public Double  getOrderAmountTotal(String date);


    public Map getOrderAmountHour(String date);
}
