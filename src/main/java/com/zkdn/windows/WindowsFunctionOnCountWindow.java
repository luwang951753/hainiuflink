package com.zkdn.windows;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-14-7:06 下午
 * @Description:
 */
public class WindowsFunctionOnCountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple3<String, Long, Long>> hainiu = env.addSource(new SourceFunction<Tuple3<String, Long, Long>>() {
            private Boolean isCancel = true;

            @Override
            public void run(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
                long num = 0;
                while (isCancel) {
                    num += 1;
                    ctx.collect(new Tuple3<>("hainiu", num, System.currentTimeMillis()));
                    Thread.sleep(1000);
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

        keyBy.window(GlobalWindows.create())
                .trigger(CountTrigger.of(5))
                .apply(new WindowFunction<Tuple3<String, Long, Long>, Tuple2<String, Long>, String, GlobalWindow>() {

                    @Override
                    public void apply(String s, GlobalWindow window, Iterable<Tuple3<String, Long, Long>> input, Collector<Tuple2<String, Long>> out) throws Exception {
                        long sum = 0;
                        Iterator<Tuple3<String, Long, Long>> iterator = input.iterator();
                        for (input.iterator(); iterator.hasNext();){
                            Tuple3<String, Long, Long> next = iterator.next();
                            System.out.println(next);
                            sum+=next.f1;
                        }
                        System.out.println(sum);
                        out.collect(Tuple2.of(s,sum));
                    }
                }).print();

                env.execute();


    }
}
