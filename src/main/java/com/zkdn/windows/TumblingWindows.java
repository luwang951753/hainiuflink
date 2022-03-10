package com.zkdn.windows;


import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-14-2:12 下午
 * @Description:
 */
public class TumblingWindows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = env.addSource(new SourceFunction<String>() {

            private Boolean isCancel = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int num = 1;
                while (isCancel) {
                    long currentTime = System.currentTimeMillis();
                    String testStr = currentTime + "\thainiu\t" + num;
                    num++;
                    ctx.collect(testStr);
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                isCancel = false;
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String element) {
                String[] split = element.split("\t");
                String s = split[0];
                Long eventTime = Long.valueOf(s);
                return eventTime;
            }
        });

        KeyedStream<String, String> keyedStream = stringSingleOutputStreamOperator.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                String[] split = value.split("\t");
                return split[1];
            }
        });

//        keyedStream.print();
//        stringSingleOutputStreamOperator.print();
        WindowedStream<String, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10), Time.seconds(9)));

        window.process(new ProcessWindowFunction<String, String, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<String> elements, Collector<String> out) throws Exception {

                System.out.println("subtask:"+getRuntimeContext().getIndexOfThisSubtask()+
                        ",start:"+context.window().getStart()+
                        ",end:"+context.window().getEnd()+
                        ",waterMarks:"+context.currentWatermark()+
                        ",currentTime"+System.currentTimeMillis()
                );
                Iterator<String> iterator = elements.iterator();
                Integer sum = 0;
                for (;iterator.hasNext();){
                    String next = iterator.next();
                    System.out.println(next);
                    String[] split = next.split("\t");
                    String s1 = split[2];
                    Integer integer = Integer.valueOf(s1);
                    sum+=integer;
                }
                out.collect("sum:"+sum);
            }
        }).print();


        env.execute();


    }
}
