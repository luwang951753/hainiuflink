package com.zkdn.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.ArrayList;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-08-5:06 下午
 * @Description:
 */
public class KafkaRichParallelSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<String> source = env.socketTextStream("bigdata01", 6666);
        DataStreamSource<String> source = env.fromElements("lw", "yt", "hzz");

        Properties producerPropsSns = new Properties();
        producerPropsSns.setProperty("bootstrap.servers", "bigdata04:6667");
        producerPropsSns.setProperty("retries", "3");
        //FlinkKafkaProducer010类的构造函数支持自定义kafka的partitioner，
        FlinkKafkaProducer010 kafkaOut = new FlinkKafkaProducer010<String>("flink_event",
                new SimpleStringSchema(),
                producerPropsSns,new HainiuFlinkPartitioner());

        kafkaOut.setLogFailuresOnly(false);//不打日志，直接抛异常，导致应用重启（at-least-once）
        kafkaOut.setFlushOnCheckpoint(true); //默认是true（true保证at_least_once）

        source.addSink(kafkaOut);
        env.execute();
    }
}
