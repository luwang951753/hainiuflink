package com.zkdn.operater;

import com.zkdn.source.FileCountryDictSourceFunction;
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
 * @Date: 2022-02-07-2:09 下午
 * @Description:
 */
public class CountryCodeConnect {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        DataStreamSource<String> countryDictSource = env.addSource(new FileCountryDictSourceFunction());

        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty("bootstrap.servers", "bigdata04:6667");
        kafkaConsumerProps.setProperty("group.id", "qingniuflink");
        kafkaConsumerProps.setProperty("flink.partition-discovery.interval-millis", "30000");
        FlinkKafkaConsumer010<HainiuKafkaRecord> kafkaSource = new FlinkKafkaConsumer010<>("flink_event",
                new HainiuKafkaRecordSchema(),
                kafkaConsumerProps);
        //    kafkaSource.setStartFromEarliest()
        //    kafkaSource.setStartFromGroupOffsets()
        kafkaSource.setStartFromLatest();

        DataStreamSource<HainiuKafkaRecord> kafkainput = env.addSource(kafkaSource);

        ConnectedStreams<String, HainiuKafkaRecord> connect = countryDictSource.connect(kafkainput);

        SingleOutputStreamOperator<String> connectInput = connect.process(new CoProcessFunction<String, HainiuKafkaRecord, String>() {

            private Map<String, String> map = new HashMap<String, String>();

            @Override
            public void processElement1(String s, Context context, Collector<String> collector) throws Exception {
                String[] split = s.split("\t");
                map.put(split[0],split[1]);
                collector.collect(s);
            }

            @Override
            public void processElement2(HainiuKafkaRecord hainiuKafkaRecord, Context context, Collector<String> collector) throws Exception {
                String countryCode = hainiuKafkaRecord.getRecord();

                String countryName = map.get(countryCode);
                String outStr = countryName == null ? "no match" : countryName;
                collector.collect(outStr);
            }
        });

        connectInput.print();
        env.execute();

    }
}
