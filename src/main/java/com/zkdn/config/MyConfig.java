package com.zkdn.config;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-22-3:53 下午
 * @Description:
 */
public class MyConfig {

    //KAFKA的组
    public static final String KAFKA_GROUP = "qingniu_hainiu_shop";

    //KAFKA的topic
    public static final String KAFKA_TOPIC_ACCESS_SHOP = "hainiu_shop_access_shop";
    public static final String KAFKA_TOPIC_ACCESS_SHOP_DWD = "hainiu_shop_access_shop_dwd";
    public static final String KAFKA_TOPIC_ACCESS_SNS = "hainiu_shop_access_sns";
    public static final String KAFKA_TOPIC_ACCESS_SNS_DWD = "hainiu_shop_access_sns_dwd";
    public static final String KAFKA_TOPIC_SERVICE_ALL = "hainiu_shop_service_all";
    public static final String KAFKA_TOPIC_SERVICE_SHOP_DWD = "hainiu_shop_service_shop_dwd";
    public static final String KAFKA_TOPIC_SERVICE_SNS_DWD = "hainiu_shop_service_sns_dwd";

    //hdfs的输出目录配置
//    public static final String HDFS_SAVE_PATH = "/Users/luwang/Desktop/testwordcount/data2";
    public static final String HDFS_SAVE_PATH = "hdfs://zkdnserver/data/hainiu/hainiu_shop_streaming";

    //kafka的topic动态发现时间
    public static final String KAFKA_PARTITION_DISCOVERY_INTERVAL = "30000";

    //KAFKA的broker
    public static final String KAFKA_BROKER = "bigdata04:6667,bigdata05:6667,bigdata06:6667,bigdata08:6667";

    //checkpoint周期
    public static final Long CHECKPOINT_INTERVAL = 5000L;

    //checkpoint间隔
    public static final Long CHECKPOINT_BETWEEN = 2000L;

    //checkpoint超时
    public static final Long CHECKPOINT_TIMEOUT = 20000L;

    //
    public static final Integer CHECKPOINT_MAX_CONCURRENT = 1;

    //容错次数
    public static final Integer RESTART_NUM = 3;

    //容错延时时间
    public static final Integer RESTART_DELAY_TIME = 0;

    //memoryStateBackend存储状态的内存大小
    public static final Integer MEMORY_STATE_BACKEND_SIZE = 10 * 1024 * 1024;







}
