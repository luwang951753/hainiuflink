package com.zkdn.windows;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-14-5:04 下午
 * @Description:
 */
public class SessionWindows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple3<String, Long, Long>> hainiu = env.addSource(new SourceFunction<Tuple3<String, Long, Long>>() {
            private boolean isCancel = true;

            @Override
            public void run(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
                long num = 0;
                while (isCancel) {
                    num += 1;
                    ctx.collect(new Tuple3<>("hainiu", num, System.currentTimeMillis()));
                    if (num % 5 == 0) {
                        Thread.sleep(3000);
                    } else {
                        Thread.sleep(1000);
                    }
                }
            }

            @Override
            public void cancel() {
                isCancel = false;
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, Long, Long>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<String, Long, Long> element) {
                return element.f2;
            }
        });


        KeyedStream<Tuple3<String, Long, Long>, String> keyBy = hainiu.keyBy(new KeySelector<Tuple3<String, Long, Long>, String>() {
            @Override
            public String getKey(Tuple3<String, Long, Long> value) throws Exception {
                return value.f0;
            }
        });

//        SingleOutputStreamOperator<Tuple3<String, Long, Long>> sum = keyBy.window(EventTimeSessionWindows.withGap(Time.seconds(2)))
//                .reduce(new ReduceFunction<Tuple3<String, Long, Long>>() {
//                    @Override
//                    public Tuple3<String, Long, Long> reduce(Tuple3<String, Long, Long> value1, Tuple3<String, Long, Long> value2) throws Exception {
//                        return Tuple3.of(value1.f0, value1.f1 + value2.f1, value2.f2);
//                    }
//                });

        SingleOutputStreamOperator<Tuple3<String, Long, Long>> sum = keyBy.window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple3<String, Long, Long>>() {
            @Override
            public long extract(Tuple3<String, Long, Long> element) {
                if (element.f1 % 5 == 0) {
                    return 2500L;
                } else {
                    return 2000L;
                }
            }
        })).reduce(new ReduceFunction<Tuple3<String, Long, Long>>() {
            @Override
            public Tuple3<String, Long, Long> reduce(Tuple3<String, Long, Long> value1, Tuple3<String, Long, Long> value2) throws Exception {
                return Tuple3.of(value1.f0, value1.f1 + value2.f1, value2.f2);
            }
        });

        sum.print();
        env.execute();
    }
}
