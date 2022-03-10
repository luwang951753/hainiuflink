package com.zkdn.operater;

import com.zkdn.source.FileCountryDictSourceFunction;
import com.zkdn.source.HainiuKafkaRecord;
import com.zkdn.source.HainiuKafkaRecordSchema;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-08-11:03 上午
 * @Description:
 */
public class CountryCodeConnectCustomPartitionerShuJuQingXie {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty("bootstrap.servers", "bigdata04:6667");
        kafkaConsumerProps.setProperty("group.id", "qingniuflink");
        kafkaConsumerProps.setProperty("flink.partition-discovery.interval-millis", "30000");

        FlinkKafkaConsumer010<HainiuKafkaRecord> kafkaSource = new FlinkKafkaConsumer010<>("flink_event", new HainiuKafkaRecordSchema(), kafkaConsumerProps);
//        kafkaSource.setStartFromLatest();

        DataStreamSource<HainiuKafkaRecord> kafkaInput = env.addSource(kafkaSource);

        DataStream<HainiuKafkaRecord> kafka = kafkaInput.map(new MapFunction<HainiuKafkaRecord, HainiuKafkaRecord>() {
            @Override
            public HainiuKafkaRecord map(HainiuKafkaRecord value) throws Exception {
                String record = value.getRecord();
                Random random = new Random();
                int i = random.nextInt(6);
                return new HainiuKafkaRecord(i + "_" + record);
            }
        }).partitionCustom(new Partitioner<HainiuKafkaRecord>() {
            @Override
            public int partition(HainiuKafkaRecord key, int numPartitions) {
                String[] s = key.getRecord().split("_");
                String randomId = s[0];
                return new Integer(randomId);
            }
        }, new KeySelector<HainiuKafkaRecord, HainiuKafkaRecord>() {
            @Override
            public HainiuKafkaRecord getKey(HainiuKafkaRecord value) throws Exception {
                return value;
            }
        });

        DataStreamSource<String> countryDictSource = env.addSource(new FileCountryDictSourceFunction());
        DataStream<Tuple2<HainiuKafkaRecord, String>> countryDict = countryDictSource.flatMap(new FlatMapFunction<String, Tuple2<HainiuKafkaRecord, String>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<HainiuKafkaRecord, String>> out) throws Exception {
                String[] split = value.split("\t");
                String key = split[0];
                String values = split[1];

                for (int i = 0; i < 6; i++) {
                    System.out.println("i"+i);
                    HainiuKafkaRecord randomKey = new HainiuKafkaRecord(i + "_" + key);
                    Tuple2<HainiuKafkaRecord, String> t2 = Tuple2.of(randomKey, values);
                    out.collect(t2);
                }

            }

        })

        .partitionCustom(new Partitioner<HainiuKafkaRecord>() {
            @Override
            public int partition(HainiuKafkaRecord key, int numPartitions) {
                String[] s = key.getRecord().split("_");
                String randomId = s[0];
                return new Integer(randomId);
            }
        }, new KeySelector<Tuple2<HainiuKafkaRecord, String>, HainiuKafkaRecord>() {
            @Override
            public HainiuKafkaRecord getKey(Tuple2<HainiuKafkaRecord, String> value) throws Exception {
                System.out.println("value"+value);
                return value.f0;
            }
        });

        ConnectedStreams<Tuple2<HainiuKafkaRecord, String>, HainiuKafkaRecord> connect = countryDict.connect(kafka);

        SingleOutputStreamOperator<String> connectInput = connect.process(new CoProcessFunction<Tuple2<HainiuKafkaRecord, String>, HainiuKafkaRecord, String>() {

            private Map<String, String> map = new HashMap<String, String>();

            @Override
            public void processElement1(Tuple2<HainiuKafkaRecord, String> value, Context ctx, Collector<String> out) throws Exception {
                map.put(value.f0.getRecord(), value.f1);
                out.collect(value.toString());
            }

            @Override
            public void processElement2(HainiuKafkaRecord value, Context ctx, Collector<String> out) throws Exception {
                String countryName = map.get(value.getRecord());
                String outStr = countryName == null ? "no match" : countryName;
                out.collect(outStr);
            }
        });

        connectInput.print();
        env.execute();


    }
}
