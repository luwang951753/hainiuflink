package com.zkdn.utils;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-25-3:43 下午
 * @Description:
 */
public class IPUtil {

    public static long ip2long(String ip){
        String[] fields = ip.split("\\.");
        if(fields.length != 4){
            return 0L;
        }

        long r = Long.parseLong(fields[0]) << 24;
        r |= Long.parseLong(fields[1]) << 16;
        r |= Long.parseLong(fields[2]) << 8;
        r |= Long.parseLong(fields[3]);
        return r;
    }

}
