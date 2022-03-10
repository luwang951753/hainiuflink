package com.zkdn.state;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-10-10:16 上午
 * @Description:
 */
public class FileCountryDictSourceMapFunction implements SourceFunction<Map<String, String>> {

    private String md5 = null;
    private Boolean isCancel = true;
    private Integer interval = 10000;

    @Override
    public void run(SourceContext<Map<String, String>> ctx) throws Exception {
        Path pathString = new Path("hdfs://zkdnserver/user/qingnui/country_data");
        Configuration hadoopConf = new Configuration();
        FileSystem fs = FileSystem.get(hadoopConf);
        while (isCancel) {
            if (!fs.exists(pathString)) {
                Thread.sleep(interval);
                continue;
            }
            FileChecksum fileChecksum = fs.getFileChecksum(pathString);
            String md5Str = fileChecksum.toString();
            String currentMd5 = md5Str.substring(md5Str.indexOf(":") + 1);
            if (!currentMd5.equals(md5)) {
                FSDataInputStream open = fs.open(pathString);
                BufferedReader reader = new BufferedReader(new InputStreamReader(open));
                String line = reader.readLine();
                HashMap<String, String> map = new HashMap<>();
                while(line != null){
                    String[] split = line.split("\t");
                    map.put(split[0],split[1]);
                    line = reader.readLine();
                }
                ctx.collect(map);
                reader.close();
                md5 = currentMd5;
            }
            Thread.sleep(interval);
        }
    }

    @Override
    public void cancel() {
        isCancel = false;
    }
}
