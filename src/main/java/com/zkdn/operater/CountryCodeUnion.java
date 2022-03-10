package com.zkdn.operater;

import com.zkdn.source.FileCountryDictSourceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-07-9:46 上午
 * @Description:
 */
public class CountryCodeUnion {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        DataStreamSource<String> countryDictSource = env.addSource(new FileCountryDictSourceFunction());

        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty("bootstrap.servers", "bigdata04:6667");
        kafkaConsumerProps.setProperty("group.id", "qingniuflink");
        kafkaConsumerProps.setProperty("flink.partition-discovery.interval-millis", "30000");
        FlinkKafkaConsumer010<String> kafkaSource = new FlinkKafkaConsumer010<String>("flink_event", new SimpleStringSchema(), kafkaConsumerProps);
        //    kafkaSource.setStartFromEarliest()
        //    kafkaSource.setStartFromGroupOffsets()
        kafkaSource.setStartFromLatest();

        DataStreamSource<String> kafkainput = env.addSource(kafkaSource);

        DataStream<String> union = countryDictSource.union(kafkainput);


        SingleOutputStreamOperator<String> process = union.process(new ProcessFunction<String, String>() {

            private Map<String, String> map = new HashMap<>();

            @Override
            public void processElement(String value, Context context, Collector<String> out) throws Exception {
                String[] split = value.split("\t");
                if (split.length > 1){
                    map.put(split[0],split[1]);
                    out.collect(value);
                }else{
                    String countryName = map.get(value);
                    String outStr = countryName == null ? "no match" : countryName;

                    out.collect(outStr);
                }
            }
        });

        process.print();
        env.execute();
    }
}
