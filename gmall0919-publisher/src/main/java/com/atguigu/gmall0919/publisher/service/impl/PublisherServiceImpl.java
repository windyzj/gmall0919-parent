package com.atguigu.gmall0919.publisher.service.impl;

import com.atguigu.gmall0919.common.constant.GmallConstant;
import com.atguigu.gmall0919.publisher.mapper.DauMapper;
import com.atguigu.gmall0919.publisher.mapper.OrderMapper;
import com.atguigu.gmall0919.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl  implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Autowired
    JestClient jestClient;

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

    @Override
    public Map getSaleDetail(String date, String keyword, int startpage, int size ) {
      //TODO 如何构造查询条件
        String query="{\n" +
                "  \"query\": {\n" +
                "    \"match\": {\n" +
                "      \"sku_name\": {\n" +
                "         \"query\": \"小米手机\",   \n" +
                "         \"operator\": \"and\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"groupby_user_gender\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"user_gender\",\n" +
                "        \"size\": 2\n" +
                "      }\n" +
                "    },\n" +
                "    \"groupby_user_age\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"user_age\",\n" +
                "        \"size\": 100\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "  \n" +
                "}";

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //查询
        searchSourceBuilder.query(new MatchQueryBuilder("sku_name",keyword).operator(MatchQueryBuilder.Operator.AND));
        //聚合
        // 性别
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_user_gender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(genderAggs);
        // 年龄
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_user_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(ageAggs);

        int fromRowNo=(startpage-1)*size;
        searchSourceBuilder.from(fromRowNo);
        searchSourceBuilder.size(size);

        System.out.println(searchSourceBuilder.toString());
        if(date!=null) {
            date=  date.replace("-", "");
        }else{
            throw  new RuntimeException("日期不能为空");
        }
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_SALE+"_"+date+"-query" ).build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            //TODO 如何展开查询结果
             //明细数据

            List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
            List<Map> detailList=new ArrayList<>(hits.size());
            for (SearchResult.Hit<Map, Void> hit : hits) {
                detailList.add(hit.source);
            }
            // 聚合结果
            //年龄
            List<TermsAggregation.Entry> ageBuckets = searchResult.getAggregations().getTermsAggregation("groupby_user_age").getBuckets();
            Map ageAggMap=new HashMap(ageBuckets.size()*2);
            for (TermsAggregation.Entry ageBucket : ageBuckets) {
                ageAggMap.put(ageBucket.getKey(),ageBucket.getCount());
            }
           // 性别
            List<TermsAggregation.Entry> genderBuckets = searchResult.getAggregations().getTermsAggregation("groupby_user_gender").getBuckets();
            Map genderAggMap=new HashMap(genderBuckets.size()*2);
            for (TermsAggregation.Entry genderBucket : genderBuckets) {
                genderAggMap.put(genderBucket.getKey(),genderBucket.getCount());
            }


            Map  resultMap=new HashMap();
            resultMap.put("total",searchResult.getTotal());
            resultMap.put("ageAgg",ageAggMap);
            resultMap.put("genderAgg",genderAggMap);
            resultMap.put("detail",detailList);
            return  resultMap;

        } catch (IOException e) {
            e.printStackTrace();
            throw  new RuntimeException("查询ES异常");
        }

    }
}
