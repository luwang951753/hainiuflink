package com.zkdn.windows;


import com.sun.org.apache.regexp.internal.RE;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-15-9:52 上午
 * @Description:
 */
public class CoGroupOnAndJoinSessionWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> s1 = env.socketTextStream("localhost", 6666);
        DataStreamSource<String> s2 = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<Tuple2<String, Integer>> input1 = s1.map(f -> Tuple2.of(f, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));
        SingleOutputStreamOperator<Tuple2<String, Integer>> input2 = s2.map(f -> Tuple2.of(f, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));

//        input1.coGroup(input2)
//                .where(new KeySelector<Tuple2<String, Integer>, String>() {
//                    @Override
//                    public String getKey(Tuple2<String, Integer> value) throws Exception {
//                        return value.f0;
//                    }
//                })
//                .equalTo(new KeySelector<Tuple2<String, Integer>, String>() {
//                    @Override
//                    public String getKey(Tuple2<String, Integer> value) throws Exception {
//                        return value.f0;
//                    }
//                })
//                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
//                .trigger(CountTrigger.of(1))
//                .apply(new CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {
//                    @Override
//                    public void coGroup(Iterable<Tuple2<String, Integer>> first, Iterable<Tuple2<String, Integer>> second, Collector<String> out) throws Exception {
//                        StringBuilder sb = new StringBuilder();
//                        sb.append("Data in stream1: \n");
//                        Iterator<Tuple2<String, Integer>> iterator = first.iterator();
//
//                        for(;iterator.hasNext();){
//                            Tuple2<String, Integer> next = iterator.next();
//                            sb.append(next.f0).append("<->").append(next.f1).append("\n");
//                        }
//                        sb.append("Data in stream2:\n");
//                        Iterator<Tuple2<String, Integer>> iterator1 = second.iterator();
//                        for(;iterator1.hasNext();){
//                            Tuple2<String, Integer> next = iterator1.next();
//                            sb.append(next.f0).append("<->").append(next.f1).append("\n");
//
//                        }
//                        out.collect(sb.toString());
//                    }
//                }).print();


        input1.join(input2)
                .where(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .equalTo(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                .trigger(CountTrigger.of(1))
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {
                    @Override
                    public String join(Tuple2<String, Integer> first, Tuple2<String, Integer> second) throws Exception {
                        return first.f0 + "==" + second.f0;
                    }
                }).print();



        env.execute();


    }

}
