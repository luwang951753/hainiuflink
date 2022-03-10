package com.zkdn.windows;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-11-3:52 下午
 * @Description:
 */
public class TimestampWatermarkMethod2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> dss = env.addSource(new SourceFunction<String>() {

            private Boolean isCancel = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (isCancel) {
                    long currentTime = System.currentTimeMillis();
                    String testStr = currentTime + "\thainiu";
                    ctx.collect(testStr);
                }
            }

            @Override
            public void cancel() {
                isCancel = false;
            }
        });

        dss.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {

            private long maxOutOfOrderness = 1000;
            private long waterMarkTime;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(waterMarkTime - maxOutOfOrderness);
            }

            @Override
            public long extractTimestamp(String element, long previousElementTimestamp) {
                String[] split = element.split("\t");
                Long timestamp = Long.valueOf(split[0]);
                waterMarkTime = timestamp;
                return timestamp;
            }
        }).print();
        env.execute();
    }
}
