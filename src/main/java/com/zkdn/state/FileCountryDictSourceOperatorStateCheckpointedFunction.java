package com.zkdn.state;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

public class FileCountryDictSourceOperatorStateCheckpointedFunction implements SourceFunction<String>,CheckpointedFunction {

    private String md5 = null;
    private ListState<String> ls = null;

    private Boolean isCancel = true;

    private Integer interval = 1000;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        Path pathString = new Path("hdfs://zkdnserver/user/qingnui/country_data");
        Configuration hadoopConf = new Configuration();
        FileSystem fs = FileSystem.get(hadoopConf);
        while (isCancel) {
            if(!fs.exists(pathString)){
                Thread.sleep(interval);
                continue;
            }
            System.out.println("md5值:"+md5);
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
        System.out.println("cancel");
        isCancel = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("snapshotState");
        ls.clear();
        ls.add(md5);

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println("initializeState");
        ListStateDescriptor<String> lsd = new ListStateDescriptor<String>("md5",String.class);
        ls = context.getOperatorStateStore().getListState(lsd);
        if (context.isRestored()){
            System.out.println("重启获取状态值");
            Iterable<String> strings = ls.get();
            String next = strings.iterator().next();

            md5 = next;
        }
    }
}