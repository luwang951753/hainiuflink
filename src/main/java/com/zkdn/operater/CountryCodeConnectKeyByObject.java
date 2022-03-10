package com.zkdn.operater;

import com.zkdn.source.FileCountryDictSourceFunction;
import com.zkdn.source.HainiuKafkaRecord;
import com.zkdn.source.HainiuKafkaRecordSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-07-3:05 下午
 * @Description:
 */
public class CountryCodeConnectKeyByObject {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> countryDictSource = env.addSource(new FileCountryDictSourceFunction());

        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty("bootstrap.servers", "bigdata04:6667");
        kafkaConsumerProps.setProperty("group.id", "qingniuflink");
        kafkaConsumerProps.setProperty("flink.partition-discovery.interval-millis", "30000");
        org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010<HainiuKafkaRecord> kafkaSource = new FlinkKafkaConsumer010<>("flink_event",
                new HainiuKafkaRecordSchema(),
                kafkaConsumerProps);
        //    kafkaSource.setStartFromEarliest()
        //    kafkaSource.setStartFromGroupOffsets()
        kafkaSource.setStartFromLatest();

        DataStreamSource<HainiuKafkaRecord> kafkainput = env.addSource(kafkaSource);

        KeyedStream<Tuple2<HainiuKafkaRecord, String>, HainiuKafkaRecord> countryDictKeyBy = countryDictSource.map(new MapFunction<String, Tuple2<HainiuKafkaRecord, String>>() {
            @Override
            public Tuple2<HainiuKafkaRecord, String> map(String value) throws Exception {
                String[] split = value.split("\t");
                return Tuple2.of(new HainiuKafkaRecord(new String(split[0])), split[1]);
            }
        }).keyBy(new KeySelector<Tuple2<HainiuKafkaRecord, String>, HainiuKafkaRecord>() {
            @Override
            public HainiuKafkaRecord getKey(Tuple2<HainiuKafkaRecord, String> value) throws Exception {
                return value.f0;
            }
        });

        KeyedStream<HainiuKafkaRecord, HainiuKafkaRecord> record = kafkainput.keyBy(new KeySelector<HainiuKafkaRecord, HainiuKafkaRecord>() {

            @Override
            public HainiuKafkaRecord getKey(HainiuKafkaRecord value) throws Exception {
                return value;
            }
        });

        ConnectedStreams<Tuple2<HainiuKafkaRecord, String>, HainiuKafkaRecord> connect = countryDictKeyBy.connect(record);

        SingleOutputStreamOperator<String> connectInput = connect.process(new KeyedCoProcessFunction<HainiuKafkaRecord, Tuple2<HainiuKafkaRecord, String>, HainiuKafkaRecord, String>() {

            private Map<String, String> map = new HashMap<String, String>();

            @Override
            public void processElement1(Tuple2<HainiuKafkaRecord, String> value, Context ctx, Collector<String> out) throws Exception {
                String currentKey = ctx.getCurrentKey().getRecord();
                map.put(currentKey, value.f1);
                out.collect(value.toString());
            }

            @Override
            public void processElement2(HainiuKafkaRecord value, Context ctx, Collector<String> out) throws Exception {
                HainiuKafkaRecord currentKey = ctx.getCurrentKey();
                String countryName = map.get(currentKey.getRecord());
                String outStr = countryName == null ? "no match" : countryName;
                out.collect(currentKey.toString() + "--" + outStr);
            }
        });

        connectInput.print();

        env.execute();


    }
}
