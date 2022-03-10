package com.zkdn.state;


import com.zkdn.source.HainiuKafkaRecord;
import com.zkdn.source.HainiuKafkaRecordSchema;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-10-10:25 上午
 * @Description:
 */
public class CountryCodeConnectMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //必须设置不然匹配不上
        env.setParallelism(1);

        DataStreamSource<Map<String, String>> countryDictSource = env.addSource(new FileCountryDictSourceMapFunction());


        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty("bootstrap.servers", "bigdata04:6667");
        kafkaConsumerProps.setProperty("group.id", "qingniuflink");
        kafkaConsumerProps.setProperty("flink.partition-discovery.interval-millis", "30000");
        FlinkKafkaConsumer010<HainiuKafkaRecord> kafkaSource = new FlinkKafkaConsumer010<>("flink_event", new HainiuKafkaRecordSchema(), kafkaConsumerProps);
        //    kafkaSource.setStartFromEarliest()
        //    kafkaSource.setStartFromGroupOffsets()
        kafkaSource.setStartFromLatest();

        DataStreamSource<HainiuKafkaRecord> kafkainput = env.addSource(kafkaSource);

        ConnectedStreams<Map<String, String>, HainiuKafkaRecord> connect = countryDictSource.connect(kafkainput);

        SingleOutputStreamOperator<String> connectInput = connect.process(new CoProcessFunction<Map<String, String>, HainiuKafkaRecord, String>() {

            private Map<String, String> map = new HashMap<String, String>();

            @Override
            public void processElement1(Map<String, String> value, Context ctx, Collector<String> out) throws Exception {
                for (Map.Entry<String, String> entry : value.entrySet()) {
                    map.put(entry.getKey(), entry.getValue());
                }
                System.out.println("value:"+value);
                out.collect(value.toString());
            }

            @Override
            public void processElement2(HainiuKafkaRecord value, Context ctx, Collector<String> out) throws Exception {
                String countryCode = value.getRecord();
                String countryName = map.get(countryCode);
                String outStr = countryName == null ? "no match" : countryName;
                out.collect(outStr);
            }
        });

        connectInput.print();
        env.execute();
    }
}
