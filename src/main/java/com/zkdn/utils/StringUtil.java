package com.zkdn.utils;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-03-03-11:04 上午
 * @Description:
 */
public class StringUtil {
    public static String camel2under(String c){
        String separator = "_";
        c = c.replaceAll("([a-z])([A-Z])", "$1" + separator + "$2").toLowerCase();
        return c;
    }
}
