package com.zkdn.state;


import com.zkdn.source.HainiuKafkaRecord;
import com.zkdn.source.HainiuKafkaRecordSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-10-10:51 上午
 * @Description:
 */
public class CountryCodeConnectMapKeyedBroadCast {
    private static final MapStateDescriptor<String, String> msd = new MapStateDescriptor<>("countryCodeMap", String.class, String.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(1000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //保存EXACTLY_ONCE
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //每次ck之间的间隔，不会重叠
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);
        //每次ck的超时时间
        checkpointConfig.setCheckpointTimeout(20000L);
        //如果ck执行失败，程序是否停止
        checkpointConfig.setFailOnCheckpointingErrors(true);
        //job在执行CANCE的时候是否删除ck数据
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //指定保存ck的存储模式，这个是默认的
        MemoryStateBackend stateBackend = new MemoryStateBackend(10 * 1024 * 1024, false);
        //指定保存ck的存储模式
//        FsStateBackend stateBackend = new FsStateBackend("file:///Users/leohe/Data/output/flink/checkpoints", true);

//        RocksDBStateBackend stateBackend = new RocksDBStateBackend("file:///Users/leohe/Data/output/flink/checkpoints", true);
        env.setStateBackend(stateBackend);

        //恢复策略
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        3, // number of restart attempts
                        Time.of(0, TimeUnit.SECONDS) // delay
                )
        );

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

        KeyedStream<HainiuKafkaRecord, String> record = kafkainput.keyBy(new KeySelector<HainiuKafkaRecord, String>() {
            @Override
            public String getKey(HainiuKafkaRecord value) throws Exception {
                return value.getRecord();
            }
        });

        BroadcastStream<Map<String, String>> broadcastinput = countryDictSource.broadcast(msd);
        BroadcastConnectedStream<HainiuKafkaRecord, Map<String, String>> broadcastConnect = record.connect(broadcastinput);

        SingleOutputStreamOperator<String> broadcastConnectInput = broadcastConnect.process(new KeyedBroadcastProcessFunction<String, HainiuKafkaRecord, Map<String, String>, String>() {
            @Override
            public void processElement(HainiuKafkaRecord value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                String countryCode = ctx.getCurrentKey();
                System.out.println("countryCode:" + countryCode);
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(msd);
                String countryName = broadcastState.get(countryCode);
                String outStr = countryName == null ? "no match" : countryName;
                out.collect(outStr);
            }

            @Override
            public void processBroadcastElement(Map<String, String> value, Context ctx, Collector<String> out) throws Exception {
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(msd);
                for (Map.Entry<String, String> entry : value.entrySet()) {
                    broadcastState.put(entry.getKey(), entry.getValue());
                }

                out.collect(value.toString());
            }
        });

        broadcastConnectInput.print();

        env.execute();


    }

}
