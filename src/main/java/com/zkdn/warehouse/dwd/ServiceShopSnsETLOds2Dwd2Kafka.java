package com.zkdn.warehouse.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.fastjson.util.TypeUtils;
import com.zkdn.config.MyConfig;
import com.zkdn.utils.IPUtil;
import com.zkdn.warehouse.dwd.warehousemodel.PageSupport;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-03-01-5:03 下午
 * @Description:
 */
public class ServiceShopSnsETLOds2Dwd2Kafka {

    private static final OutputTag<String> shopTag = new OutputTag<String>("shop"){};
    private static final OutputTag<String> snsTag = new OutputTag<String>("sns"){};

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(MyConfig.CHECKPOINT_INTERVAL);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //保存EXACTLY_ONCE
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //每次ck之间的间隔，不会重叠
        checkpointConfig.setMinPauseBetweenCheckpoints(MyConfig.CHECKPOINT_BETWEEN);
        //每次ck的超时时间
        checkpointConfig.setCheckpointTimeout(MyConfig.CHECKPOINT_TIMEOUT);
        checkpointConfig.setMaxConcurrentCheckpoints(MyConfig.CHECKPOINT_MAX_CONCURRENT);
        //如果ck执行失败，程序是否停止
        checkpointConfig.setFailOnCheckpointingErrors(true);
        //job在执行CANCE的时候是否删除ck数据
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //恢复策略
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        MyConfig.RESTART_NUM, // number of restart attempts
                        Time.of(0, TimeUnit.SECONDS) // delay
                )
        );

        //step3 statebackend
        MemoryStateBackend stateBackend = new MemoryStateBackend(MyConfig.MEMORY_STATE_BACKEND_SIZE, false);
        env.setStateBackend(stateBackend);

        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty("bootstrap.servers", MyConfig.KAFKA_BROKER);
        kafkaConsumerProps.setProperty("group.id", MyConfig.KAFKA_GROUP);
        kafkaConsumerProps.setProperty("flink.partition-discovery.interval-millis", MyConfig.KAFKA_PARTITION_DISCOVERY_INTERVAL);

        FlinkKafkaConsumer010<String> serviceKafkaInput = new FlinkKafkaConsumer010<String>(MyConfig.KAFKA_TOPIC_SERVICE_ALL, new SimpleStringSchema(), kafkaConsumerProps);
        serviceKafkaInput.setStartFromLatest();
        DataStreamSource<String> serviceKafkaSource = env.addSource(serviceKafkaInput);

        serviceKafkaSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                String[] split = value.split("Q");
//                System.out.println(split[10]);
//                if(split.length != 11 || split[10].indexOf("con.hainiu") < 0){
//                    return false;
//                }else{
//                    return true;
//                }
                return true;
            }
        }).process(new ProcessFunction<String, String>() {

            SimpleDateFormat sf = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                sf = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss");
            }

            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                String[] strings = value.split("Q");
                String jsonData = strings[5];


                String timeRequestIp = strings[0];
                int i1 = timeRequestIp.lastIndexOf(" ");
                String timeRequest = timeRequestIp.substring(0, i1);
                String ip = timeRequestIp.substring(i1 + 1);
                long longIp = IPUtil.ip2long(ip);
                String longIpStr = String.valueOf(longIp);
                long time = sf.parse(timeRequest).getTime();
                String timeStr = String.valueOf(time);

                String userId = strings[1];
                String loginName = strings[2];
                String requestURL = strings[3];

                String requestAction = strings[6];

                PageSupport ps = JSON.parseObject(jsonData, PageSupport.class);
                String classNameStr = strings[10];
                List<List<Object>> reList = new ArrayList<>();
                if(classNameStr.endsWith(">")){
                    JSONArray jsonArray = (JSONArray) ps.getItems();
                    String str = classNameStr.substring(classNameStr.indexOf("<") + 1);
                    String className = str.substring(0, str.length() - 1);
                    for (int i = 0; i < jsonArray.size(); i++) {
                        Object object = jsonArray.getObject(i, Class.forName(className));
                        ArrayList<Object> listT = new ArrayList();
                        listT.add(time);
                        listT.add(requestURL);
                        listT.add(className);
                        listT.add(longIp);
                        listT.add(userId);
                        listT.add(loginName);
                        listT.add(object);
                        reList.add(listT);
                    }
                }else{
                    Object object = TypeUtils.castToJavaBean(ps.getItems(), Class.forName(classNameStr));
                    ArrayList<Object> listT = new ArrayList();
                    listT.add(time);
                    listT.add(requestURL);
                    listT.add(classNameStr);
                    listT.add(longIp);
                    listT.add(userId);
                    listT.add(loginName);
                    listT.add(object);
                    reList.add(listT);
                }

                for (List<Object> l : reList) {
                    if(requestAction.startsWith("com.hainiu.shop")){
                        ctx.output(shopTag, JSON.toJSONString(l,
                                SerializerFeature.WriteNullBooleanAsFalse,
                                SerializerFeature.WriteNullStringAsEmpty,
                                SerializerFeature.WriteNullNumberAsZero,
                                SerializerFeature.WriteNullListAsEmpty));

                    }else if(requestAction.startsWith("com.hainiu.sns")){
                        ctx.output(snsTag, JSON.toJSONString(l,
                                SerializerFeature.WriteNullBooleanAsFalse,
                                SerializerFeature.WriteNullStringAsEmpty,
                                SerializerFeature.WriteNullNumberAsZero,
                                SerializerFeature.WriteNullListAsEmpty));
                    }
                }
            }
        }).print();


        env.execute();


    }
}
