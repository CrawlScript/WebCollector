/*
 * Copyright (C) 2014 hu
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

package cn.edu.hfut.dmic.webcollector.util;

/**
 * 全局配置
 * @author hu
 */
public class Config {
    public static final String old_info_path="crawldb/old/info.avro";
    public static final String current_info_path="crawldb/current/info.avro";
    public static final String new_info_path="crawldb/new/info.avro";
    public static final String lock_path="crawldb/lock";
    
    public static long requestMaxInterval=1000*60;

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
