package com.zkdn.warehouse.dwd;

import org.apache.hadoop.util.ProgramDriver;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-03-03-5:05 下午
 * @Description:
 */
public class Driver {
    public static void main(String[] args) throws Throwable {


        ProgramDriver driver = new ProgramDriver();
        driver.addClass("access_all_etl_dwd_kafka", AccessShopSnsETLOds2Dwd2Kafka.class, "清洗shop、sns数据入kafka");
        driver.addClass("access_shop_dwd_hdfs", AccessShopDwdKafka2HDFS.class, "将kafkadwd数据写入hdfs");
        driver.run(args);

    }
}



















