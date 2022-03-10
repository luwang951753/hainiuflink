package com.zkdn.state;

import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-09-3:57 下午
 * @Description:
 */
public class FileCountryDictSourceOperatorStateListCheckpointedFunction implements SourceFunction<String>, ListCheckpointed<String> {

    private String md5 = null;

    private Boolean isCancel = true;

    private Integer interval = 1000;

    @Override
    public List<String> snapshotState(long checkpointId, long timestamp) throws Exception {
        List<String> list = new ArrayList<>();
        list.add(md5);
        System.out.println("snapshotState");
        return list;
    }

    @Override
    public void restoreState(List<String> state) throws Exception {
        md5 = state.get(0);
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        Path pathString = new Path("hdfs://zkdnserver/user/qingnui/country_data");
        Configuration hadoopConf = new Configuration();
        FileSystem fs = FileSystem.get(hadoopConf);
        while (isCancel) {
            if (!fs.exists(pathString)) {
                Thread.sleep(interval);
                continue;
            }
            System.out.println(md5);
            FileChecksum fileChecksum = fs.getFileChecksum(pathString);
            String md5Str = fileChecksum.toString();
            String currentMd5 = md5Str.substring(md5Str.indexOf(":") + 1);
            if (!currentMd5.equals(md5)) {
                FSDataInputStream open = fs.open(pathString);
                BufferedReader reader = new BufferedReader(new InputStreamReader(open));
                String line = reader.readLine();
                while (line != null) {
                    ctx.collect(line);
                    line = reader.readLine();
                }
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
