package com.zkdn.warehouse.dwd;

import com.zkdn.config.MyConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-03-01-3:30 下午
 * @Description:
 */
public class ProducerLog2Kafka {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        while(true){
            Properties producerPropsSns = new Properties();
            producerPropsSns.setProperty("bootstrap.servers", MyConfig.KAFKA_BROKER);
            producerPropsSns.setProperty("retries", String.valueOf(MyConfig.RESTART_NUM));
            //FlinkKafkaProducer010类的构造函数支持自定义kafka的partitioner，
            Random random = new Random();
            int i = random.nextInt(3);
            String topic = "";
            String content = "";
            if(1 == i){
                topic = MyConfig.KAFKA_TOPIC_ACCESS_SHOP;
                content = "192.168.200.101Q-Q15/Sep/2020:14:56:14 +0800QGET /getSessionGoodsNum.action?name=lw&age=12&sex=1 HTTP/1.1Q200Q1Q-Qhttp://www.baidu.com/index.phpQMozilla/5.0 (MacintoshIntel Mac OS.X 10_15_6) AppleWebKit/537.36 (KHTML,like gocko) Chrome/85.0.4183.102 Safari/537.36Q-Qleaf4810635744c7b0ca3b173c760d1eQ-";
            }else if(0 == i){
                topic = MyConfig.KAFKA_TOPIC_ACCESS_SNS;
                content = "192.168.200.101Q-Q15/Sep/2020:14:56:14 +0800QGET /getSessionGoodsNum.action?name=lw&age=12&sex=1 HTTP/1.1Q200Q1Q-Qhttp://www.baidu.com/index.phpQMozilla/5.0 (MacintoshIntel Mac OS.X 10_15_6) AppleWebKit/537.36 (KHTML,like gocko) Chrome/85.0.4183.102 Safari/537.36Q-Qleaf4810635744c7b0ca3b173c760d1eQ-";
            }else if(2 == i){
                topic = MyConfig.KAFKA_TOPIC_SERVICE_ALL;
//                content = "2020-09-15 12:01:20 192.168.137.2Q388212645d904370b4fdec2a0bbd6ecQhainiushopTesQ/goods/goods_detail.jps?id=11Q{'id':2}Q{\"type\":\"success\",\"item\":{\"id\":2,\"brandCname\":\"胖哥哥\",\"brandEname\":\"BARPlus\",\"addTime\":\"2020-06-17 14:15:49\",\"brandType\":2,\"logoImg\":\"2020/06/17/2020_06_17_092694.png_254x54\",\"homeImg\":\"2020/06/17/2020_06_17_599638.jpg_1518x1100\"}}Qcom.hainiu.shop.action.brand.BrandCenterActionQgetBrandDetailByIdQcom.hainiu.service.shopBrand.action.brand.queryActionQlookQcom.hainiu.global.model.shop.brand.BrandModel";
//                content = "2020-09-15 12:01:20 192.168.137.2Q38831264d904270b4fdec2a0bbd6eceQhainiushopTestQ/goods/goods_detail.jsp?id=11Q{'brandId':2,'isOpen':1}Q{\"type\":\"success\",\"items\":[]}Qcom.hainiu.shop.action.brand.BrandCenterActionQgetCatalogListByBrandIdQcom.hainiu.service.shopBrand.action.brandCatalog.QueryActionQlistQjava.util.ArrayList<>";
                content = "2020-09-15 12:01:20 192.168.137.2Q38831264d904270b4fdec2a0bbd6eceQhainiushopTestQ/goods/goods_detail.jsp?id=11Q{\"brandId\":2,\"remainHour\":0,\"pageSize\":5,\"curPage\":1}Q{\"type\":\"success\",\"items\":[{\"goodsSn\":\"110001\",\"goodsId\":8,\"goodsCname\":\"大衣大衣大衣\",\"goodsEname\":\"\",\"goodsImg\":\"2020/06/17/2020_06_17_389760.jpg_323x430\",\"goodsPrice\":118.0,\"remainHour\":0},{\"goodsSn\":\"110003\",\"goodsId\":11,\"goodsCname\":\"大衣2大衣2\",\"goodsEname\":\"\",\"goodsImg\":\"2020/06/17/2020_06_17_753440.jpg_430x430\",\"goodsPrice\":0.1,\"remainHour\":0}],\"totalCount\":2,\"pageCount\":1,\"pageSize\":5,\"curPage\":1,\"num\":5}Qcom.hainiu.shop.action.goods.GoodsActionQlistBrandHotGoodsQcom.hainiu.service.shopGoods.action.goods.QueryActionQlistBrandHotGoodsQjava.util.ArrayList<com.hainiu.global.model.shop.goods.GoodsSearchModel>";

            }

            System.out.println("生产数据:"+"topic:"+topic+"content:"+content);
            FlinkKafkaProducer010 kafkaOut = new FlinkKafkaProducer010<String>(topic,new SimpleStringSchema(),producerPropsSns);
            kafkaOut.setLogFailuresOnly(false);
            kafkaOut.setFlushOnCheckpoint(true);

            DataStreamSource<String> source = env.fromElements(content);
            source.addSink(kafkaOut);
            env.execute();
        }

    }
}
