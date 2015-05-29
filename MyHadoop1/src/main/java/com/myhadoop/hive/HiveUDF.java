/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.myhadoop.hive;
import org.apache.hadoop.hive.ql.exec.UDF;
/**
 *
 * @author jw362j
 * 
 * http://my.oschina.net/wangjiankui/blog/64230
 * 
 * 
 *  add jar /tmp/NEWIP2Long.jar;
    drop temporary function ip2long;
    create temporary function ip2long as 'com.myhadoop.hive.HiveUDF';
    select ip2long(ip) from XXX ;
 * 
 */
public class HiveUDF extends UDF {
     public static long ip2long(String ip) {
 
        String[] ips = ip.split("[.]");
        long ipNum = 0;
        if (ips == null) {
            return 0;
        }
        for (int i = 0; i < ips.length; i++) {
            ipNum = ipNum << Byte.SIZE | Long.parseLong(ips[i]);
        }
 
        return ipNum;
    }
 
    public long evaluate(String ip) {
        if (ip.matches("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}")) {
            try {
                long ipNum = ip2long(ip);
                return ipNum;
            } catch (Exception e) {
                return 0;
            }
        } else {
            return 0;
        }
    }
 
    public static void main(String[] argvs) {
        HiveUDF ipl = new HiveUDF();
        System.out.println(ip2long("112.64.106.238"));
        System.out.println(ipl.evaluate("58.35.186.62"));
    }
}
