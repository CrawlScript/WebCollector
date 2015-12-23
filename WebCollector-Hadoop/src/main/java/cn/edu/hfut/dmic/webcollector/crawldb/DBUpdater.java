/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.crawldb;

import cn.edu.hfut.dmic.webcollector.util.CrawlerConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author hu
 */
public class DBUpdater {
    public static void updateDB(Path crawlPath,String segmentName,Configuration conf) throws Exception{
        Path segmentPath=new Path(crawlPath,"segments/"+segmentName);
        Path fetchPath=new Path(segmentPath,"fetch");
        Path parsePath=new Path(segmentPath,"parse");
        Path[] mergePaths=new Path[]{fetchPath,parsePath};
        Merge.merge(crawlPath, mergePaths,conf,"dbupdater");
        Merge.install(crawlPath,conf);
    }
    
}
