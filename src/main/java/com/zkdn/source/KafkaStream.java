//package com.zkdn;
//
//import lombok.AllArgsConstructor;
//import lombok.Data;
//import lombok.NoArgsConstructor;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.TableResult;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.types.Row;
//
///**
// * Created with IntelliJ IDEA.
// *
// * @Auther: lw
// * @Date: 2022-02-03-1:27 下午
// * @Description:
// */
//public class KafkaStream {
//
//    public static void main(String[] args) throws Exception {
//        //TODO 0.env
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
//        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);
//
//        //TODO 1.source
//        TableResult inputTable = tenv.executeSql(
//                "CREATE TABLE input_kafka (\n" +
//                        "  `user_id` String,\n" +
//                        "  `page_id` String,\n" +
//                        "  `status` String\n" +
//                        ") WITH (\n" +
//                        "  'connector' = 'kafka',\n" +
//                        "  'topic' = 'input_kafka',\n" +
//                        "  'properties.bootstrap.servers' = 'bigdata04:6667',\n" +
//                        "  'properties.group.id' = 'testGroup',\n" +
//                        "  'scan.startup.mode' = 'latest-offset',\n" +
//                        "  'format' = 'json'\n" +
//                        ")"
//        );
//
//
//        //TODO 2.transformation
//        //编写sql过滤出状态为success的数据
//        String sql = "select status,sum(cast(page_id as int)) as page_id_sum from input_kafka group by status";
//        Table etlResult = tenv.sqlQuery(sql);
//
//
//        //TODO 3.sink
//        DataStream<Tuple2<Boolean, Row>> resultDS = tenv.toRetractStream(etlResult, Row.class);
//        resultDS.print();
////
////        TableResult outputTable = tenv.executeSql(
////                "CREATE TABLE output_kafka (\n" +
////                        "  `user_id` String,\n" +
////                        "  `page_id` String,\n" +
////                        "  `status` STRING\n" +
////                        ") WITH (\n" +
////                        "  'connector' = 'kafka',\n" +
////                        "  'topic' = 'output_kafka',\n" +
////                        "  'properties.bootstrap.servers' = 'bigdata04:6667',\n" +
////                        "  'format' = 'json',\n" +
////                        "  'sink.partitioner' = 'round-robin'\n" +
////                        ")"
////        );
////
//////        tenv.executeSql("insert into output_kafka select * from "+ etlResult);
////        tenv.executeSql("insert into output_kafka select * from input_kafka");
////
//
//        //TODO 4.execute
//        env.execute();
//
//    }
//    @Data
//    @AllArgsConstructor
//    @NoArgsConstructor
//    public static class Order {
//        private String orderId;
//        private Integer userId;
//        private Integer money;
//        private Long createTime;//事件时间
//    }
//
////准备kafka主题
/////export/server/kafka/bin/kafka-topics.sh --create --zookeeper node1:2181 --replication-factor 2 --partitions 3 --topic input_kafka
/////export/server/kafka/bin/kafka-topics.sh --create --zookeeper node1:2181 --replication-factor 2 --partitions 3 --topic output_kafka
/////export/server/kafka/bin/kafka-console-producer.sh --broker-list node1:9092 --topic input_kafka
////{"user_id": "1", "page_id":"1", "status": "success"}
////{"user_id": "1", "page_id":"1", "status": "success"}
////{"user_id": "1", "page_id":"1", "status": "success"}
////{"user_id": "1", "page_id":"1", "status": "success"}
////{"user_id": "1", "page_id":"1", "status": "fail"}
/////export/server/kafka/bin/kafka-console-consumer.sh --bootstrap-server node1:9092 --topic output_kafka --from-beginning
//}
