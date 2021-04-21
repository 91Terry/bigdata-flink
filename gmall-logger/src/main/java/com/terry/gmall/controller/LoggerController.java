package com.terry.gmall.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("/applog")
    public String logger(@RequestParam("param") String logJsonStr) {
        //打印到控制台
        //System.out.println(logJsonStr);

        //2.落盘(使用logback插件完成落盘)
        log.info(logJsonStr);

        //3.发送到Kafka对应的主题
        kafkaTemplate.send("ods_base_log", logJsonStr);
        return "success";
    }
}
