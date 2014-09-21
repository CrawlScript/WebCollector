/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.util;

/**
 * 全局配置
 * @author hu
 */
public class Config {
    public static final String old_info_path="crawldb/old/info.avro";
    public static final String current_info_path="crawldb/current/info.avro";
    public static final String lock_path="crawldb/lock";

    /**
     * 网页/文件爬取时大小上限(字节)
     */
    public static int maxsize=1000*1000;

    /**
     * 相同网页爬取时间间隔(如果为-1，表示爬取时间间隔为无穷大)
     */
    public static long interval=-1;
    
    
    /**
     * 每个网页解析时，保存链接的数量上限(如果为null，则链接数量无上限)
     */
    public static Integer topN=null;
    
    /**
     * 爬取时，写爬取信息的SegmentWriter的缓存，如果希望爬取信息在断电等异常中断时无
     * 丢失，将该属性值设为1，但会造成磁盘操作频繁。不可将该属性设为0
     */
    public static int segmentwriter_buffer_size=50;
}
