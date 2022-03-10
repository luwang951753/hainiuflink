package com.zkdn.operater;

import com.zkdn.source.FileCountryDictSourceFunction;
import com.zkdn.source.HainiuKafkaRecord;
import com.zkdn.source.HainiuKafkaRecordSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-08-3:18 下午
 * @Description:
 */
public class CountryCodeConnectKeyByCountryCountOutputTag {



    public static void main(String[] args) throws Exception {

        OutputTag<String> ot = new OutputTag<String>("china") {};

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> countryDictSource = env.addSource(new FileCountryDictSourceFunction());

        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty("bootstrap.servers", "bigdata04:6667");
        kafkaConsumerProps.setProperty("group.id", "qingniuflink");
        kafkaConsumerProps.setProperty("flink.partition-discovery.interval-millis", "30000");
        FlinkKafkaConsumer010<HainiuKafkaRecord> kafkaSource = new FlinkKafkaConsumer010<>("flink_event", new HainiuKafkaRecordSchema(), kafkaConsumerProps);
        //    kafkaSource.setStartFromEarliest()
        //    kafkaSource.setStartFromGroupOffsets()
        kafkaSource.setStartFromLatest();

        DataStreamSource<HainiuKafkaRecord> kafkainput = env.addSource(kafkaSource);

        KeyedStream<Tuple2<String, String>, String> countryDictKeyBy = countryDictSource.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] split = value.split("\t");
                return Tuple2.of(split[0], split[1]);
            }
        }).keyBy(new KeySelector<Tuple2<String, String>, String>() {
            @Override
            public String getKey(Tuple2<String, String> value) throws Exception {
                return value.f0;
            }
        });

        KeyedStream<HainiuKafkaRecord, String> record = kafkainput.keyBy(new KeySelector<HainiuKafkaRecord, String>() {
            @Override
            public String getKey(HainiuKafkaRecord value) throws Exception {
                return value.getRecord();
            }
        });

        ConnectedStreams<Tuple2<String, String>, HainiuKafkaRecord> connect = countryDictKeyBy.connect(record);

        SingleOutputStreamOperator<Tuple2<String, Integer>> connectInput = connect.process(new KeyedCoProcessFunction<String, Tuple2<String, String>, HainiuKafkaRecord, Tuple2<String, Integer>>() {

            private Map<String, String> map = new HashMap<String, String>();

            @Override
            public void processElement1(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                map.put(ctx.getCurrentKey(), value.f1);
                out.collect(Tuple2.of(value.f0, 1));
            }

            @Override
            public void processElement2(HainiuKafkaRecord value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                String countryCode = ctx.getCurrentKey();
                String countryName = map.get(countryCode);
                String outStr = countryName == null ? "no match" : countryName;

                if (outStr.contains("中国")) {
                    ctx.output(ot, outStr);
                }

                out.collect(Tuple2.of(countryName.substring(0, countryName.indexOf(" ")), 1));
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = connectInput.keyBy(1).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        });

        reduce.print();

        connectInput.getSideOutput(ot).print();

        env.execute();

    }
}
