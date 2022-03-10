package com.zkdn.file;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

import java.time.ZoneId;
/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-15-5:22 下午
 * @Description:
 */
public class Streaming2RowFormatFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> input = env.socketTextStream("localhost", 6666);

        BucketingSink<String> hadoopSink = new BucketingSink<>("file:///Users/luwang/Desktop/testwordcount/data/file");

        hadoopSink.setBucketer(new DateTimeBucketer<>("yyyy-MM-dd-HH",ZoneId.of("Asia/Shanghai")));

        hadoopSink.setBatchSize(1024*1024*100);
        hadoopSink.setBatchRolloverInterval(1000);
        hadoopSink.setPartPrefix("aaa");
        hadoopSink.setPartPrefix("bbb");
        hadoopSink.setPendingSuffix("...");
        input.addSink(hadoopSink);

        env.execute();


    }
}
