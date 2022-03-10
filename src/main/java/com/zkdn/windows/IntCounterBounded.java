package com.zkdn.windows;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-15-2:34 下午
 * @Description:
 */
public class IntCounterBounded {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

//        DataStreamSource<String> input1 = env.socketTextStream("localhost", 6666);
        DataStreamSource<String> input1 = env.fromElements("a a a", "b b b");

        input1.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
            private IntCounter inc = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                inc = new IntCounter();
                getRuntimeContext().addAccumulator("hainiu", this.inc);
            }

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] strs = value.split(" ");
                for (String s : strs) {
                    this.inc.add(1);
                    out.collect(Tuple2.of(s,1));
                }
            }
        }).print();

        JobExecutionResult res = env.execute("intCount");
        Integer num = res.getAccumulatorResult("hainiu");
        System.out.println(num);

    }


}
