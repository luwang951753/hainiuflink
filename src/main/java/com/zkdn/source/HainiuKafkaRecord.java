package com.zkdn.source;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-06-4:28 下午
 * @Description:
 */
public class HainiuKafkaRecord {
    private String record;
    public HainiuKafkaRecord(String record) {
        this.record = record;
    }
    public String getRecord() {
        return record;
    }
    public void setRecord(String record) {
        this.record = record;
    }

    @Override
    public String toString() {
        return "HainiuKafkaRecord{" +
                "record='" + record + '\'' +
                '}';
    }

    @Override
    public int hashCode(){
        final int prime = 31;
        int result = 1;
        result = prime * result + ((record == null) ? 0 : record.hashCode());
        return result;

    }
}