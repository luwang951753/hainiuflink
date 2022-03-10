//package com.zkdn.utils;
//
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.core.fs.Path;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
//
//import java.util.Set;
//
///**
// * Created with IntelliJ IDEA.
// *
// * @Auther: lw
// * @Date: 2022-03-03-4:12 下午
// * @Description:
// */
//public class HDFSUtil {
//
//    public static String getInputFiles(Configuration conf, String startTimeStr, String endTimeStr, Path fileStatus, FileSystem fs ){
//        long startTime = 0, endTime=  0;
//        if(startTimeStr != null ){
//            startTime = Long.valueOf(startTimeStr);
//        }
//        if(endTimeStr != null){
//            endTime = Long.valueOf(endTimeStr);
//        }
//
//        if(fileStatus.isDirectory){
//            listFile(fs,fileStatus.getPath(), set);
//        }else{
//            System.out.println("=====>"+path.toString());
//            set.add(path.toString());
//        }
//    }
//
//
//    public static void listFile(FileSystem fs, Path path, Set<String> set){
//        if(fs.isFile(path)){
//            return;
//        }
//
//        FileStatus[] listStatus = fs.listStatus(path);
//        if(listStatus.length == 0){
//            return;
//        }
//    }
//    public static void main(String[] args) {
//
//        getInputFiles(new Configuration(),
//                "2020071500",
//                null,
//                new Path("hdfs://zkdnserver/data/hainiu/hainiu_shop_streaming"),
//                "dwd_xxx"
//
//        );
//
//
//    }
//}
