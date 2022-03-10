package com.zkdn.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-06-4:29 下午
 * @Description:
 */
public class HainiuKafkaRecordSchema implements DeserializationSchema<HainiuKafkaRecord> {
    @Override
    public HainiuKafkaRecord deserialize(byte[] message) throws IOException {
        HainiuKafkaRecord hainiuKafkaRecord = new HainiuKafkaRecord(new String(message));
        return hainiuKafkaRecord;
    }
    @Override
    public boolean isEndOfStream(HainiuKafkaRecord nextElement) {
        return false;
    }
    @Override
    public TypeInformation<HainiuKafkaRecord> getProducedType() {
        return TypeInformation.of(HainiuKafkaRecord.class);
    }
}
