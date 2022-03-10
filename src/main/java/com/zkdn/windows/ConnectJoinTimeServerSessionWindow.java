package com.zkdn.windows;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
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
 * @Date: 2022-02-15-12:27 下午
 * @Description:
 */
public class ConnectJoinTimeServerSessionWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> s1 = env.socketTextStream("localhost", 6666);
        DataStreamSource<String> s2 = env.socketTextStream("localhost", 7777);
        KeyedStream<Tuple3<String, Integer, Long>, Tuple> input1 = s1.map(f -> Tuple3.of(f, 1, System.currentTimeMillis())).returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG)).keyBy("f0");
        KeyedStream<Tuple3<String, Integer, Long>, Tuple> input2 = s2.map(f -> Tuple3.of(f, 1, System.currentTimeMillis())).returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG)).keyBy("f0");


        ConnectedStreams<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>> connect = input1.connect(input2);

        SingleOutputStreamOperator<String> process = connect.process(new KeyedCoProcessFunction<String, Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>, String>() {

            private Long datatime = null;
            private String outString = "";
            private int intervalTime = 3000;

            @Override
            public void processElement1(Tuple3<String, Integer, Long> value, Context ctx, Collector<String> out) throws Exception {
                outString += value.f0 + "\t";
                datatime = value.f2;
                ctx.timerService().registerProcessingTimeTimer(datatime + intervalTime);
                System.out.println("subTaskId:" +
                        getRuntimeContext().getIndexOfThisSubtask() +
                        ",value:" +
                        value.f0);
            }

            @Override
            public void processElement2(Tuple3<String, Integer, Long> value, Context ctx, Collector<String> out) throws Exception {
                outString += value.f0 + "\t";
                datatime = value.f2;
                ctx.timerService().registerProcessingTimeTimer(datatime + intervalTime);
                System.out.println("subTaskId:" +
                        getRuntimeContext().getIndexOfThisSubtask() +
                        ",value:" +
                        value.f0);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                System.out.println("timerService start");
                if (timestamp == datatime + intervalTime) {
                    out.collect(outString);
                    outString = "";
                }
            }
        });

        process.print();
        env.execute();

    }
}
