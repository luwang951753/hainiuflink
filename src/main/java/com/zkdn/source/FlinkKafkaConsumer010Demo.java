package com.zkdn.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-06-4:32 下午
 * @Description:
 */
public class FlinkKafkaConsumer010Demo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","bigdata04:6667");
        properties.setProperty("group.id","flink_test");
//        new FlinkKafkaConsumer010<HainiuKafkaRecord>("flink-event", new HainiuKafkaRecordSchema(), properties);
        FlinkKafkaConsumer010<HainiuKafkaRecord> myConsumer = new FlinkKafkaConsumer010<HainiuKafkaRecord>("flink_event",
                new HainiuKafkaRecordSchema(),
                properties);

        DataStreamSource<HainiuKafkaRecord> hainiuKafkaRecordDataStreamSource = env.addSource(myConsumer);

        hainiuKafkaRecordDataStreamSource.print();

        SingleOutputStreamOperator<HainiuKafkaRecord> str1 = hainiuKafkaRecordDataStreamSource.flatMap(new FlatMapFunction<HainiuKafkaRecord, HainiuKafkaRecord>() {
            @Override
            public void flatMap(HainiuKafkaRecord hainiuKafkaRecord, Collector<HainiuKafkaRecord> collector) throws Exception {
                collector.collect(hainiuKafkaRecord);
            }
        });
        DataStream<HainiuKafkaRecord> str2 = str1.rebalance();
        str2.print();

        env.execute("aaa");



    }
}
