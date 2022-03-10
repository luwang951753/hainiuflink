package com.zkdn.sink;



import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-08-4:09 下午
 * @Description:
 */
public class HDFSSinkFunction extends RichSinkFunction<String> {
    private FileSystem fs = null;
    private SimpleDateFormat sf = null;
    private String pathStr = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        fs = FileSystem.get(conf);
        sf = new SimpleDateFormat("yyyyMMddHH");
        pathStr = "hdfs://zkdnserver/user/qinniu/flinkstreaminghdfs";
    }

    @Override
    public void close() throws Exception {
        fs.close();
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        if(null != value){
            String format = sf.format(new Date());
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            StringBuilder sb = new StringBuilder();
            sb.append(pathStr).append("/").append(indexOfThisSubtask).append("_").append(format);

            Path path = new Path(sb.toString());
            FSDataOutputStream fsd = null;
            if(fs.exists(path)){
                fsd = fs.append(path);
            }else{
                fsd = fs.create(path);
            }

            fsd.write((value+"\n").getBytes("UTF-8"));
            fsd.close();
        }
    }
}
