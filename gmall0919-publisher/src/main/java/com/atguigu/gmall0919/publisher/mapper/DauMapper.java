package com.atguigu.gmall0919.publisher.mapper;

import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

public interface DauMapper {


    public  Long   selectDauTotal(String date);

    public List<Map> selectDauHourCount(String date );
}
