/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.util;

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author hu
 */
public class CrawlerConfiguration {
    
    public static Configuration create(){
         Configuration conf=new Configuration();
         conf.addResource("crawler-default.xml");
         conf.addResource("hadoop/core-site.xml");
         conf.addResource("hadoop/hdfs-site.xml");
         conf.addResource("hadoop/mapred-site.xml");
         

         //conf.set("mapred.jar", "/home/hu/mygit/WebCollector2/WebCollectorCluster/target/WebCollectorCluster-2.0.jar");
         return conf;
    }
    
    
    
}
