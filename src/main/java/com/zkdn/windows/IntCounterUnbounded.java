package com.zkdn.windows;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-15-2:42 下午
 * @Description:
 */
public class IntCounterUnbounded {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
env.setParallelism(1);
        DataStreamSource<String> input1 = env.socketTextStream("localhost", 6666);

        input1.flatMap(new RichFlatMapFunction<String, IntCounter>() {

            private IntCounter inc = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                inc = new IntCounter();
                getRuntimeContext().addAccumulator("hainiu", this.inc);
            }

            @Override
            public void flatMap(String value, Collector<IntCounter> out) throws Exception {
                String[] strs = value.split(" ");
                for (String s : strs) {
                    this.inc.add(1);
                }
                out.collect(this.inc);
            }
        }).print();

        env.execute("intCount");

    }
}
