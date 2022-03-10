package com.zkdn.warehouse.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.zkdn.config.MyConfig;
import com.zkdn.source.FileCountryDictSourceFunction;
import com.zkdn.utils.IPUtil;
import com.zkdn.warehouse.dwd.warehousemodel.AccessModel;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-22-4:27 下午
 * @Description:
 */
public class AccessShopSnsETLOds2Dwd2Kafka {

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

        //192.168.200.101\Q-Q15/Sep/2020:14:56:14 +0800QGET /getSessionGoodsNum.action?name=lw&age=12&sex=1 HTTP/1.1Q200Q1Q-Qhttp://www.baidu.com/index.phpQMozilla/5.0 (MacintoshIntel Mac OS.X 10_15_6) AppleWebKit/537.36 (KHTML,like gocko) Chrome/85.0.4183.102 Safari/537.36Q-Qleaf4810635744c7b0ca3b173c760d1eQ-

        FlinkKafkaConsumer010<String> shopKafkaInput = new FlinkKafkaConsumer010<String>(MyConfig.KAFKA_TOPIC_ACCESS_SHOP, new SimpleStringSchema(), kafkaConsumerProps);
        shopKafkaInput.setStartFromGroupOffsets();
        DataStreamSource<String> shopKafkaSource = env.addSource(shopKafkaInput);


        FlinkKafkaConsumer010<String> snsKafkaInput = new FlinkKafkaConsumer010<String>(MyConfig.KAFKA_TOPIC_ACCESS_SNS, new SimpleStringSchema(), kafkaConsumerProps);
        snsKafkaInput.setStartFromGroupOffsets();
        DataStreamSource<String> snsKafkaSource = env.addSource(snsKafkaInput);

        ConnectedStreams<String, String> connect = shopKafkaSource.connect(snsKafkaSource);

        SingleOutputStreamOperator<String> formatDS = connect.process(new CoProcessFunction<String, String, String>() {

            SimpleDateFormat sf = null;

            @Override
            public void open(Configuration parameters) throws Exception {

                sf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss +0800", Locale.ENGLISH);
            }

            public String process(String value) throws Exception {

                String[] split = value.split("Q");
                if (split.length != 12) {
                    return null;
                }


                String ip = split[0];
                String timeStr = split[2];
                String request = split[3];
                String status = split[4];
                String request_body = split[6];
                String http_refer = split[7];
                String http_user_agent = split[8];
                String userId = split[10];
                String loginName = split[11];

                long longIp = IPUtil.ip2long(ip);
                long time = sf.parse(timeStr).getTime();
                String[] requests = request.split(" ");
                String requestType = requests[0];
                String requestURL = requests[1];

                HashMap<String, String> requestAttr = new HashMap<>();
                if (requestURL.indexOf("?") > 0) {
                    String urlAttr = requestURL.substring(requestURL.indexOf("?") + 1);
                    String[] kvs = urlAttr.split("&");
                    for (String kv : kvs) {
                        String[] split1 = kv.split("=");
                        String k = split1[0];
                        String v = split1.length > 1 ? split1[1] : "";
                        requestAttr.put(k, v);
                    }
                }

                if (!request_body.equals("-")) {
                    String[] kvs = request_body.split("&");
                    for (String kv : kvs) {
                        String[] split1 = kv.split("=");
                        String k = split1[0];
                        String v = split1.length > 1 ? split1[1] : "";
                        requestAttr.put(k, v);
                    }
                }

                AccessModel accessModel = new AccessModel(longIp, time, requestType, requestURL, status, http_refer, http_user_agent, requestAttr, userId, loginName);

                String json = JSON.toJSONString(accessModel,
                        SerializerFeature.WriteNullBooleanAsFalse,
                        SerializerFeature.WriteNullStringAsEmpty,
                        SerializerFeature.WriteNullNumberAsZero,
                        SerializerFeature.WriteNullListAsEmpty);
                System.out.println("清洗后数据准备写入Kafka:"+json);
                return json;

            }

            @Override
            public void processElement1(String value, Context ctx, Collector<String> out) throws Exception {

                String process = process(value);
                if (process != null) {
                    ctx.output(shopTag, process);
                }
            }

            @Override
            public void processElement2(String value, Context ctx, Collector<String> out) throws Exception {

                String process = process(value);
                if (process != null) {
                    ctx.output(snsTag, process);
                }
            }
        });

        DataStream<String> shopDS = formatDS.getSideOutput(shopTag);
        DataStream<String> snsDS = formatDS.getSideOutput(snsTag);

        Properties producerPropsSns = new Properties();
        producerPropsSns.setProperty("bootstrap.servers", MyConfig.KAFKA_BROKER);
        producerPropsSns.setProperty("retries", String.valueOf(MyConfig.RESTART_NUM));
        FlinkKafkaProducer010 kafkaProducerShop = new FlinkKafkaProducer010<String>(MyConfig.KAFKA_TOPIC_ACCESS_SHOP_DWD,new SimpleStringSchema(),producerPropsSns);
        kafkaProducerShop.setLogFailuresOnly(false);
        kafkaProducerShop.setFlushOnCheckpoint(true);
        shopDS.addSink(kafkaProducerShop);


        FlinkKafkaProducer010 kafkaProducerSns = new FlinkKafkaProducer010<String>(MyConfig.KAFKA_TOPIC_ACCESS_SNS_DWD,new SimpleStringSchema(),producerPropsSns);
        kafkaProducerSns.setLogFailuresOnly(false);
        kafkaProducerSns.setFlushOnCheckpoint(true);
        snsDS.addSink(kafkaProducerSns);

        env.execute();

    }
}
