package com.zkdn.windows;


import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-15-3:56 下午
 * @Description:
 */
public class IntCounterUnboundedWindows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> input1 = env.socketTextStream("localhost", 6666);

        SingleOutputStreamOperator<Integer> reduce = input1.flatMap(new RichFlatMapFunction<String, Integer>() {
            @Override
            public void flatMap(String value, Collector<Integer> out) throws Exception {
                String[] strs = value.split(" ");
                for (String s : strs) {
                    out.collect(1);
                }
            }
        }).timeWindowAll(Time.seconds(5))
                .reduce(new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                        return value1 + value2;
                    }
                });

        Iterator<Integer> collect = DataStreamUtils.collect(reduce);
        for (;collect.hasNext();){
            Integer next = collect.next();
            System.out.println("count:"+next);
        }

        env.execute();


    }
}
