package com.zkdn.windows;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-11-4:10 下午
 * @Description:
 */
public class TimestampWatermarkMethod3 {
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
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                isCancel = false;
            }
        });


        dss.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<String>() {

            private long maxOutOfOrderness = 1000;

            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
                String[] split = lastElement.split("\t");
                String data = split[1];
                if(data.equals("hainiu")){
                    return new Watermark(extractedTimestamp - maxOutOfOrderness);
                }else{
                    return null;
                }
            }

            @Override
            public long extractTimestamp(String element, long previousElementTimestamp) {
                String[] split = element.split("\t");
                Long timestamp = Long.valueOf(split[0]);
                return timestamp;
            }
        }).print();

        env.execute();


    }
}
