/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector.fetcher;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author hu
 */
public class Fetcher {

    public static void fetch(Path crawlPath, String segmentName,Configuration conf) throws Exception {
        Path segmentPath = new Path(crawlPath, "segments/" + segmentName);
        Path generatePath = new Path(segmentPath, "generate");

        
        Job job = new Job(conf);
        job.setJobName("fetch "+crawlPath.toString());
        job.setJarByClass(Fetcher.class);

        job.setReducerClass(FetcherReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(FetcherOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CrawlDatum.class);

        FileInputFormat.addInputPath(job, generatePath);
        FileOutputFormat.setOutputPath(job, segmentPath);

        job.waitForCompletion(true);
    }
}
