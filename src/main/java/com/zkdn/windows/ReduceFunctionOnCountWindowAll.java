package com.zkdn.windows;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-14-7:30 下午
 * @Description:
 */
public class ReduceFunctionOnCountWindowAll {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Tuple2<String, Long>> list = new ArrayList<>();
        list.add(Tuple2.of("hainiu", 1L));
        list.add(Tuple2.of("hainiu2", 2L));
        list.add(Tuple2.of("hainiu", 3L));
        list.add(Tuple2.of("hainiu2", 4L));
        list.add(Tuple2.of("hainiu3", 100L));

        DataStreamSource<Tuple2<String, Long>> input = env.fromCollection(list);

        KeyedStream<Tuple2<String, Long>, String> keyBy = input.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        });


        SingleOutputStreamOperator<Tuple2<String, Long>> reduce = keyBy.countWindowAll(2).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                System.out.println("value1"+value1);
                System.out.println("value2"+value2);
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        });


        reduce.print();
        env.execute();

    }


}
