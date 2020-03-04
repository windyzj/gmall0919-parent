package com.atguigu.gmall0919.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.gmall0919.publisher.mapper")
public class Gmall0919PublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(Gmall0919PublisherApplication.class, args);
    }

}
