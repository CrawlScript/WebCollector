/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.util;

/**
 *
 * @author hu
 */
public class Config {
    public static final String old_info_path="crawldb/old/info.avro";
    public static final String current_info_path="crawldb/current/info.avro";
    public static final String segment_prepath="segment";
    public static int maxsize=1000*1000;
    public static long interval=1;//000*60*3;
    public static final String lock_path="crawldb/lock";
    public static Integer topN=null;
}
