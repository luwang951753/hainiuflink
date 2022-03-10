package com.zkdn.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-07-9:48 上午
 * @Description:
 */
public class FileCountryDictSourceFunction implements SourceFunction<String> {

    private String md5 = null;
    private Boolean isCancel = true;
    private Integer interval = 10000;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
//        String filePath = "file:///Users/luwang/Desktop/testwordcount/data/country_data";
        String filePath = "hdfs://zkdnserver/user/qingnui/country_data";
        Path pathString = new Path(filePath);
        Configuration hadoopConf = new Configuration();
        FileSystem fs = FileSystem.get(hadoopConf);
        while(isCancel){
            if(!fs.exists(pathString)){
                System.out.println("文件不存在"+filePath);
                Thread.sleep(interval);
                continue;
            }

            FileChecksum fileChecksum = fs.getFileChecksum(pathString);
            String md5Str = fileChecksum.toString();
            String currentMd5 = md5Str.substring(md5Str.indexOf(":") + 1);
            if(!currentMd5.equals(md5)){
                FSDataInputStream open = fs.open(pathString);
                BufferedReader reader = new BufferedReader(new InputStreamReader(open));
                String line = reader.readLine();
                while (line != null){
                    ctx.collect(line);
                    line = reader.readLine();
                }
                reader.close();
                md5 = currentMd5;
            }

        }
    }

    @Override
    public void cancel() {
        isCancel = false;
    }
}
