package com.zkdn.sink;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-08-5:05 下午
 * @Description:
 */
public class HainiuFlinkPartitioner extends FlinkKafkaPartitioner {
    @Override
    public int partition(Object o, byte[] bytes, byte[] bytes1, String s, int[] ints) {
        return 0;
    }
}
