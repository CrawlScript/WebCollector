/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.crawldb;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.util.CrawlerConfiguration;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author hu
 */
public class Injector {

    public static final Logger LOG = LoggerFactory.getLogger(Injector.class);

    public static void inject(Path crawlPath, CrawlDatums datums,Configuration conf) throws IOException, InterruptedException, ClassNotFoundException, Exception {
        Path crawldbPath= new Path(crawlPath, "crawldb");
        FileSystem fs = FileSystem.get(conf);
        Path tempdb = new Path(crawldbPath, "temp");
        if (fs.exists(tempdb)) {
            fs.delete(tempdb);
        }

        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, new Path(tempdb, "info"), Text.class, CrawlDatum.class);

        for (CrawlDatum datum : datums) {

            String key = datum.getKey();
            writer.append(new Text(key), datum);
            LOG.info("inject:" + key);
        }
        writer.close();

        Path[] mergePaths=new Path[]{tempdb};

        Merge.merge(crawlPath, mergePaths,conf,"inject");
        Merge.install(crawlPath,conf);

        if (fs.exists(tempdb)) {
            fs.delete(tempdb);
        }
        

    }

}
