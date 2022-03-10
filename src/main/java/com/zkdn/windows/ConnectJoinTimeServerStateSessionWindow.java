package com.zkdn.windows;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-15-12:42 下午
 * @Description:
 */
public class ConnectJoinTimeServerStateSessionWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> s1 = env.socketTextStream("localhost", 6666);
        DataStreamSource<String> s2 = env.socketTextStream("localhost", 7777);

        KeyedStream<Tuple3<String, Integer, Long>, Tuple> input1 = s1.map(f -> Tuple3.of(f, 1, System.currentTimeMillis())).returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG)).keyBy("f0");
        KeyedStream<Tuple3<String, Integer, Long>, Tuple> input2 = s2.map(f -> Tuple3.of(f, 1, System.currentTimeMillis())).returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG)).keyBy("f0");

        ConnectedStreams<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>> connect = input1.connect(input2);
        SingleOutputStreamOperator<String> process = connect.process(new KeyedCoProcessFunction<String, Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>, String>() {

            private int intervalTime = 3000;
            private ReducingState<String> rs = null;
            private ValueState<Long> vs = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ReducingStateDescriptor<String> rsd = new ReducingStateDescriptor<>("rsd", new ReduceFunction<String>() {
                    @Override
                    public String reduce(String value1, String value2) throws Exception {
                        return value1 + "\t" + value2;
                    }
                }, String.class);

                rs = getRuntimeContext().getReducingState(rsd);

                ValueStateDescriptor<Long> vsd = new ValueStateDescriptor<>("vsd", Long.class);
                vs = getRuntimeContext().getState(vsd);

            }

            @Override
            public void processElement1(Tuple3<String, Integer, Long> value, Context ctx, Collector<String> out) throws Exception {
                rs.add(value.f0);
                vs.update(value.f2);
                ctx.timerService().registerProcessingTimeTimer(vs.value() + intervalTime);

                System.out.println("subTaskId:" +
                        getRuntimeContext().getIndexOfThisSubtask() +
                        ",value:" +
                        value.f0);
            }

            @Override
            public void processElement2(Tuple3<String, Integer, Long> value, Context ctx, Collector<String> out) throws Exception {
                rs.add(value.f0);
                vs.update(value.f2);
                ctx.timerService().registerProcessingTimeTimer(vs.value() + intervalTime);
                System.out.println("subTaskId:" +
                        getRuntimeContext().getIndexOfThisSubtask() +
                        ",value:" +
                        value.f0);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                System.out.println("timerService start");
                Long dataTime = vs.value();
                if (timestamp == dataTime + intervalTime) {
                    out.collect(rs.get());
                    rs.clear();
                }
            }
        });

        process.print();
        env.execute();

    }
}
