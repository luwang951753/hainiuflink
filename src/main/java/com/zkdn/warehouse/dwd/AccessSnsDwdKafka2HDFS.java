package com.zkdn.warehouse.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.zkdn.config.MyConfig;
import com.zkdn.utils.BeanPathBucketAssigner;
import com.zkdn.utils.CompressionParquetAvroWriter;
import com.zkdn.warehouse.dwd.warehousemodel.AccessModel;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;


import java.time.ZoneId;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-03-03-11:25 上午
 * @Description:
 */
public class AccessSnsDwdKafka2HDFS {
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

        FlinkKafkaConsumer010<String> snsKafkaInput = new FlinkKafkaConsumer010<String>(MyConfig.KAFKA_TOPIC_ACCESS_SNS_DWD, new SimpleStringSchema(), kafkaConsumerProps);
        snsKafkaInput.setStartFromLatest();
        DataStreamSource<String> snsKafkaSource = env.addSource(snsKafkaInput);

        SingleOutputStreamOperator<AccessModel> process = snsKafkaSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                try {
                    Object object = JSON.parse(value);
                    if (object instanceof JSONObject) {
                        return true;
                    } else {
                        return false;
                    }
                } catch (JSONException e) {
                    return false;
                }
            }
        }).process(new ProcessFunction<String, AccessModel>() {
            @Override
            public void processElement(String value, Context ctx, Collector<AccessModel> out) throws Exception {
                AccessModel accessModel = JSON.parseObject(value, AccessModel.class);
                out.collect(accessModel);
            }
        });

        BeanPathBucketAssigner<AccessModel> bucketAssigner = new BeanPathBucketAssigner<>("sns_shop", "yyyy/MMdd/HH", ZoneId.of("Asia/Shanghai"));
        StreamingFileSink<AccessModel> sink = StreamingFileSink.forBulkFormat(new Path(MyConfig.HDFS_SAVE_PATH),
                CompressionParquetAvroWriter.forReflectRecord(AccessModel.class, CompressionCodecName.SNAPPY)
        ).withBucketAssigner(bucketAssigner).build();


        process.addSink(sink);

        env.execute();
    }

}
