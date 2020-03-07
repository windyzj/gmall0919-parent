package com.atguigu.gmall0919.publisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {

    public Double  selectOrderAmountTotal(String date);


    public List<Map> selectOrderAmountHour(String date);

}
