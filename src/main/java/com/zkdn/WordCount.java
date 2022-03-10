package com.zkdn;



import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-01-30-11:37 下午
 * @Description:
 */
public class WordCount {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setString("taskmanager.numberOfTaskSlots","16");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<String> ds = env.fromCollection(Arrays.asList("hadoop spark flink"));
//        DataStreamSource<String> ds = env.socketTextStream("bigdata01", 4048);


//        DataStreamSource<List<String>> ds = env.fromElements(Arrays.asList("a", "b", "b", "d", "e", "f", "g"));
        DataStreamSource<String> ds = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] s = value.split(" ");
                for (String ss : s) {
                    out.collect(Tuple2.of(ss, 1));
                }
            }
        }).setParallelism(2);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = flatMap.keyBy(f -> f.f0).sum(1).setParallelism(2);

        sum.filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> value) throws Exception {
                return true;
            }
        }).setParallelism(2).startNewChain().print().slotSharingGroup("luwang").setParallelism(2);

        System.out.println(env.getExecutionPlan());



//        ds.print();

        env.execute("WordCount");



    }
}
